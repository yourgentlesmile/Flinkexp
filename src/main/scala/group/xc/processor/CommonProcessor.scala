package group.xc.processor

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}

import scala.collection.JavaConverters._

class CommonProcessor extends ProcessFunction[(String, Result), Put] {

  override def processElement(value: (String, Result), ctx: ProcessFunction[(String, Result), Put]#Context, out: Collector[Put]): Unit = {
    //value为上游source传过来的参数,_1为rowkey _2为rowkey对应的行结果
    //注意：不要在此打印日志或者有创建连接的操作，因为每一条数据过来都会调用此函数
    //示例代码：加密rowkey之后，将其他列数据原样写入，并传入下游sink函数
    val put: Put = new Put(Bytes.toBytes(value._1))
    for (cell: Cell <- value._2.listCells().asScala) {
      put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
    }
    out.collect(put)
  }
}
