
package com.kong.eos.serving.core.utils

import com.kong.eos.serving.core.constants.AppConstant
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CheckpointTest extends BaseUtilsTest with CheckpointUtils {

  val utils = spy(this)

  "PolicyUtils.deleteCheckpointPath" should {
    "delete path from HDFS when using not local mode" in {
      doReturn(false)
        .when(utils)
        .isExecutionType(getPolicyModel(), AppConstant.ConfigLocal)

      utils.deleteCheckpointPath(getPolicyModel())

      verify(utils, times(1)).deleteFromHDFS(getPolicyModel())
    }

    "delete path from local when using local mode" in {
      doReturn(true)
        .when(utils)
        .isExecutionType(getPolicyModel(), AppConstant.ConfigLocal)

      doReturn(false)
        .when(utils)
        .isHadoopEnvironmentDefined

      utils.deleteCheckpointPath(getPolicyModel())

      verify(utils, times(1)).deleteFromLocal(getPolicyModel())
    }
  }
}
