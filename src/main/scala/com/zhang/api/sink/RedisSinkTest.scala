package com.zhang.api.sink

import com.zhang.api.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 11:09
 *         Description:
 * */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt"

    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

    env.execute()

  }

}

class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  override def getValueFromData(t: SensorReading): String = t.id
}
