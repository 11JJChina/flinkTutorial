package com.zhang.montecarko

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/14
 *         Time: 19:56
 *         Description:
 * */
object MonteCarlo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val myMapFunc = new RichMapFunction[(Long, MonteCarloPoint), (Long, Double)] {

    var countAndPi: ValueState[(Long, Long)] = _

    override def map(in: (Long, MonteCarloPoint)): (Long, Double) = {
      val tmpCurrentSum = countAndPi.value

      val currentSum = if (tmpCurrentSum != null) {
        tmpCurrentSum
      } else {
        (0L, 0L)
      }

      val allCount = currentSum._1 + 1
      val piCount = currentSum._2 + in._2.pi

      val newState = (allCount, piCount)

      countAndPi.update(newState)

      (allCount, 4.0 * piCount / allCount)


    }

    override def open(parameters: Configuration): Unit = {
      countAndPi = getRuntimeContext.getState(
        new ValueStateDescriptor[(Long, Long)]("MonteCarloPi", createTypeInformation[(Long, Long)])
      )
    }
  }

  val dataStream = env.addSource(new MonteCarloSource)

  val resultStream: DataStream[(Long, Double)] = dataStream.map((1L, _)).keyBy(0).map(myMapFunc)

  resultStream.print()

  env.execute()

}

case class MonteCarloPoint(x: Double, y: Double) {
  def pi: Int = if (x * x + y * y <= 1) 1 else 0
}

class MonteCarloSource extends RichSourceFunction[MonteCarloPoint] {
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[MonteCarloPoint]): Unit = {
    val rand = new Random()
    while (running) {
      sourceContext.collect(MonteCarloPoint(rand.nextDouble(), rand.nextDouble()))
    }

  }

  override def cancel(): Unit = running = false
}
