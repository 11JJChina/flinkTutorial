package com.zhang.api.sink

import java.util

import com.zhang.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 11:57
 *         Description:
 **/
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt"

    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble)
    })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.time.toString)

        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        requestIndexer.add(indexRequest)

        dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFunc).build())

        env.execute()
      }
    }


  }

}
