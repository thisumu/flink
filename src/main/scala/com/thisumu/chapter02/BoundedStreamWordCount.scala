package com.thisumu.chapter02

import org.apache.flink.streaming.api.scala._

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取文本文件数据
    val lineDataStream = env.readTextFile("data/input/words.txt")

    // 3.对数据集进行处理转换
    val wordAndOne = lineDataStream.flatMap((_.split(" "))).map(word => (word, 1))

    // 4.按照单词进行分组
    val wordAndOneGroup = wordAndOne.keyBy(_._1)

    // 5.对分组的数据进行sum聚合统计
    val result = wordAndOneGroup.sum(1)

    // 6.打印输出结果
    result.print()

    // 7.执行任务
    env.execute()

  }

}
