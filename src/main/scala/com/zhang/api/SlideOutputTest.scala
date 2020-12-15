package com.zhang.api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/15
 *         Time: 21:11
 *         Description:
 **/
object SlideOutputTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    })

    val highTempStream = dataStream.process(new SplitTempProcessor(30.0))
    highTempStream.print("high")
    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low"))
    lowTempStream.print("low")


  }
}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature > threshold) {
      collector.collect(i)
    } else {
      context.output(new OutputTag[(String, Long, Double)]("low"), i)
    }

  }
}