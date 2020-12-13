package com.zhang.api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 10:36
 *         Description:
 * */
object TransformTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt"

    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    })

    val aggStream = dataStream.keyBy(0).minBy(2)

    dataStream.keyBy(0).reduce((curState, newData) => {
      SensorReading(curState.id, newData.time, curState.temperature)
    })

    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")

    val warningStream = highStream.map(data => {
      (data.id, data.temperature)
    })

    val connectedStream = warningStream.connect(lowStream)

    connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    env.execute()


  }

}
