
package com.kong.eos.driver.stage

import java.io.Serializable

import com.kong.eos.driver.step.CubeMaker
import com.kong.eos.sdk.pipeline.aggregation.cube.{Dimension, DimensionType}
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.serving.core.models.policy.PhaseEnum
import com.kong.eos.serving.core.models.policy.cube.{CubeModel, OperatorModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.kong.eos.driver.schema.SchemaHelper
import com.kong.eos.driver.schema.SchemaHelper.DefaultTimeStampTypeString
import com.kong.eos.driver.step
import com.kong.eos.driver.step.{Cube, CubeMaker}
import com.kong.eos.driver.writer.{CubeWriterHelper, WriterOptions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

trait CubeStage extends BaseStage with TriggerStage {
  this: ErrorPersistor =>

  def cubesStreamStage(refUtils: ReflectionUtils,
                       initSchema: StructType,
                       inputData: DStream[Row],
                       outputs: Seq[Output]): Unit = {
    val cubes = cubeStage(refUtils, initSchema)
    val errorMessage = s"Something gone wrong executing the cubes stream for: ${policy.input.get.name}."
    val okMessage = s"Cubes executed correctly."
    generalTransformation(PhaseEnum.CubeStream, okMessage, errorMessage) {
      val dataCube = CubeMaker(cubes).setUp(inputData)
      dataCube.foreach { case (cubeName, aggregatedData) =>
        val cubeWriter = cubes.find(cube => cube.name == cubeName)
          .getOrElse(throw new Exception("Is mandatory one cube in the cube writer"))
        CubeWriterHelper.writeCube(cubeWriter, outputs, aggregatedData)
      }
    }
  }

  private[driver] def cubeStage(refUtils: ReflectionUtils, initSchema: StructType): Seq[Cube] =
    policy.cubes.map(cube => createCube(cube, refUtils, initSchema: StructType))

  private[driver] def createCube(cubeModel: CubeModel,
                                 refUtils: ReflectionUtils,
                                 initSchema: StructType): Cube = {
    val okMessage = s"Cube: ${cubeModel.name} created correctly."
    val errorMessage = s"Something gone wrong creating the cube: ${cubeModel.name}. Please re-check the policy."
    generalTransformation(PhaseEnum.Cube, okMessage, errorMessage) {
      val name = cubeModel.name
      val dimensions = cubeModel.dimensions.map(dimensionDto => {
        val fieldType = initSchema.find(stField => stField.name == dimensionDto.field).map(_.dataType)
        val defaultType = fieldType.flatMap(field => SchemaHelper.mapSparkTypes.get(field))
        Dimension(dimensionDto.name,
          dimensionDto.field,
          dimensionDto.precision,
          instantiateDimensionType(dimensionDto.`type`, dimensionDto.configuration, refUtils, defaultType))
      })
      val operators = getOperators(cubeModel.operators, refUtils, initSchema)
      val expiringDataConfig = SchemaHelper.getExpiringData(cubeModel)
      val triggers = triggerStage(cubeModel.triggers)
      val schema = SchemaHelper.getCubeSchema(cubeModel, operators, dimensions)
      val dateType = SchemaHelper.getTimeTypeFromString(cubeModel.writer.dateType.getOrElse(DefaultTimeStampTypeString))
      step.Cube(
        name,
        dimensions,
        operators,
        initSchema,
        schema,
        dateType,
        expiringDataConfig,
        triggers,
        WriterOptions(
          cubeModel.writer.outputs,
          cubeModel.writer.saveMode,
          cubeModel.writer.tableName,
          getAutoCalculatedFields(cubeModel.writer.autoCalculatedFields),
          cubeModel.writer.partitionBy,
          cubeModel.writer.primaryKey
        )
      )
    }
  }

  private[driver] def getOperators(operatorsModel: Seq[OperatorModel],
                                   refUtils: ReflectionUtils,
                                   initSchema: StructType): Seq[Operator] =
    operatorsModel.map(operator => createOperator(operator, refUtils, initSchema))

  private[driver] def createOperator(model: OperatorModel,
                                     refUtils: ReflectionUtils,
                                     initSchema: StructType): Operator = {
    val okMessage = s"Operator: ${model.`type`} created correctly."
    val errorMessage = s"Something gone wrong creating the operator: ${model.`type`}. Please re-check the policy."
    generalTransformation(PhaseEnum.Operator, okMessage, errorMessage) {
      refUtils.tryToInstantiate[Operator](model.`type` + Operator.ClassSuffix, (c) =>
        c.getDeclaredConstructor(
          classOf[String],
          classOf[StructType],
          classOf[Map[String, Serializable]])
          .newInstance(model.name, initSchema, model.configuration).asInstanceOf[Operator])
    }
  }

  private[driver] def instantiateDimensionType(dimensionType: String,
                                               configuration: Option[Map[String, String]],
                                               refUtils: ReflectionUtils,
                                               defaultType: Option[TypeOp]): DimensionType =
    refUtils.tryToInstantiate[DimensionType](dimensionType + Dimension.FieldClassSuffix, (c) => {
      (configuration, defaultType) match {
        case (Some(conf), Some(defType)) =>
          c.getDeclaredConstructor(classOf[Map[String, Serializable]], classOf[TypeOp])
            .newInstance(conf, defType).asInstanceOf[DimensionType]
        case (Some(conf), None) =>
          c.getDeclaredConstructor(classOf[Map[String, Serializable]]).newInstance(conf).asInstanceOf[DimensionType]
        case (None, Some(defType)) =>
          c.getDeclaredConstructor(classOf[TypeOp]).newInstance(defType).asInstanceOf[DimensionType]
        case (None, None) =>
          c.getDeclaredConstructor().newInstance().asInstanceOf[DimensionType]
      }
    })
}
