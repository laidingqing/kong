
package com.kong.eos.plugin.input.fileSystem

import java.io._

import com.kong.eos.plugin.TemporalSparkContext
import org.scalatest._

import scala.io.Source

class FileSystemInputTest extends TemporalSparkContext with Matchers {

  val directory = getClass().getResource("/origin.txt")
  val lines = Source.fromURL(directory).getLines().toList
  val parentFile = new File(directory.getPath).getParent


  val properties = Map(("directory", "file://" + parentFile))
  val input = new FileSystemInput(properties)

  "Events counted" should " the same as files created" in {
    val dstream= input.initStream(ssc, "MEMORY_ONLY")
    val totalEvents = ssc.sparkContext.accumulator(0L)

    dstream.print()
    dstream.foreachRDD(rdd => {
      val count = rdd.count()
      println(s"EVENTS COUNT : \t $count")
      totalEvents.add(count)
    })

    ssc.start()

    Thread.sleep(3000)
    val file = new File(parentFile + "/output.txt")
    val out = new PrintWriter(file)
    lines.foreach(l => out.write(l))
    out.close()
    val numFile = if (file.exists()) 1 else 0
    ssc.awaitTerminationOrTimeout(10000)

    assert(totalEvents.value === numFile.toLong)
    file.delete()
  }
}