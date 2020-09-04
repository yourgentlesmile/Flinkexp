package group.xc.flink.stream.demo1

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * 通过flink的程序，从socket里面接受数据，然后将数据进行单词计数统计
 */
object StreamSocket {
    def main(args: Array[String]): Unit = {
        //flink的程序入口类
        val environment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketValue : DataStream[String] = environment.socketTextStream("10.0.0.249", 8887)

        val value = socketValue.flatMap(x => x.split(" ")).map(x => (x, 1)).keyBy(0).sum(1).print()
        environment.execute("Test")
    }
}
