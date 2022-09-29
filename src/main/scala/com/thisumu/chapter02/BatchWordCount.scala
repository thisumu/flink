package com.thisumu.chapter02

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2.读取文本文件数据
    val lineDataSet = env.readTextFile("data/input/words.txt")

    // 3.对数据集进行转换处理
    val wordAndOne = lineDataSet.flatMap(_.split(" ")).map(word => (word, 1))

    // 4.按照单词进行分组
    val wordAndOneGroup = wordAndOne.groupBy(0)

    // 5.对分组后的数据进行sum聚合统计
    val result = wordAndOneGroup.sum(1)

    // 6.打印输出结果
    result.print()
  }

}
