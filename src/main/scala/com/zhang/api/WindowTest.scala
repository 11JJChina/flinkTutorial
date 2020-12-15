package com.zhang.api

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 20:38
 *         Description:
 * */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt"

    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    })

    val resultStream = dataStream.map(data => (data.id,data.temperature)).keyBy(_._1).timeWindow(Time.seconds(10))

  }

}
