package com.zhang.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/10
 *         Time: 8:23
 *         Description:
 * */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ParameterTool.fromArgs(args)

    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")

    env.setParallelism(8)

    val inputDS: DataStream[String] = env.socketTextStream(host, port)

    val result = inputDS.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1))
      .keyBy(0)
      .sum(1)

    result.print()


    env.execute()

  }

}
