package com.thisumu.chapter02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取socket文本流数据
    /**
     * 在node101服务器中创建一个发送socket文本流的服务
     * 命令: nc -lk 7777
     *    l为监听7777端口，k为保持监听状态
     * 启起服务后再运行StreamWordCount程序，不然监听不到会报错
     * 运行好程序后到服务器发送文本，程序就会开始进行统计
     * 若服务器没有nc命令，则使用 yum install -y nc 命令进行安装
     */
//    val lineDataStream = env.socketTextStream("node101", 7777)
    // 从执行参数中获取主机名和端口，在执行参数中添加: --host node101 --port 7777
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    val lineDataStream = env.socketTextStream(host, port)
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
