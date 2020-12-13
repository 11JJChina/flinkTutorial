package com.zhang.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/10
 *         Time: 8:14
 *         Description:
 * */
object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.readTextFile("D:\\workspace\\flinkTutorial\\src\\main\\resources\\hello.txt")

    val result = data.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    result.print()

  }

}
