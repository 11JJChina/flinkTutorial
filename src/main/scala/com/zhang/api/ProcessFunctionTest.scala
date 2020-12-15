package com.zhang.api

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/14
 *         Time: 20:20
 *         Description:
 **/
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.time * 1000
    })

    val processStream = dataStream.keyBy(_.id).process(new TempIncrAlert)

    processStream.print()

    env.execute()
  }

  class TempIncrAlert extends KeyedProcessFunction[String, SensorReading, String] {

    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
      val preTemp = lastTemp.value

      lastTemp.update(i.temperature)

      val curTimerTs = currentTimer.value

      if (i.temperature > preTemp && curTimerTs == 0) {
        val timeTs = context.timerService().currentProcessingTime() + 10000L
        context.timerService().registerProcessingTimeTimer(timeTs)
        currentTimer.update(curTimerTs)
      } else if (preTemp > i.temperature || preTemp == 0.0) {
        context.timerService().deleteProcessingTimeTimer(curTimerTs)
        currentTimer.clear()
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(ctx.getCurrentKey + "温度连续上升")
      currentTimer.clear()
    }
  }

}
