package com.zhang.api.sink

import com.zhang.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 10:52
 *         Description:
 * */
object SinkFileTest extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val inputPath = "D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt"

  val stream = env.readTextFile(inputPath)

  val dataStream = stream.map(data => {
    val attr = data.split(",")
    SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
  })

  dataStream.writeAsCsv("D:\\workspace\\flinkTutorial\\src\\main\\resources\\out.txt")

  dataStream.addSink(
    StreamingFileSink.forRowFormat(
      new Path("D:\\workspace\\flinkTutorial\\src\\main\\resources\\out1.txt"),
      new SimpleStringEncoder[SensorReading]()
    ).build()
  )

  env.execute()

}
