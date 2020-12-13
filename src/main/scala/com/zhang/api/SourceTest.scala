package com.zhang.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/11
 *         Time: 0:26
 *         Description:
 **/

case class SensorReading(id: String, time: Long, temperature: Double)

object SourceTest {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.server", "localhost:9092")
    properties.setProperty("group.id", "test-source")

    val stream01 = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), properties))

    stream01.print()

    env.addSource(new MySensorSource)


  }

}

class MySensorSource extends SourceFunction[SensorReading] {

  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()

    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

    while (running) {
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )

      val curTime = System.currentTimeMillis()

      curTemp.foreach(data =>
        sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )

      Thread.sleep(500)

    }

  }

  override def cancel(): Unit = running = false
}
