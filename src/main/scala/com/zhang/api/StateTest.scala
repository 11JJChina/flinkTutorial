package com.zhang.api

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/15
 *         Time: 19:45
 *         Description:
 **/
object StateTest {
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

    dataStream.keyBy(_.id)
      //      .flatMap(new TempChangAlert(10.0))
      .flatMapWithState[(String, Double, Double), Double] {
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) => {
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0)
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          else
            (List.empty, Some(data.temperature))
        }
      }

  }

}

class TempChangAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastState", classOf[Double]))

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val diff = (in.temperature - lastTemp).abs
    if (diff > threshold)
      collector.collect((in.id, lastTemp, in.temperature))
    lastTempState.update(in.temperature)
  }
}
