package group.xc.sink

import com.alibaba.hbase.client.AliHBaseUEConnection
import group.xc.constant.Constants
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

class HBaseWriter(executeTable: String) extends RichSinkFunction[Put] {
  var conn: Connection = _
  val scan: Scan = null
  var mutator: BufferedMutator = _
  var count = 0

  /**
    * 建立HBase连接
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    config.set("hbase.client.connection.impl", classOf[AliHBaseUEConnection].getName)
    config.set("hbase.client.endpoint", Constants.CLIENT_ENDPOINT);
    config.set("hbase.client.username", Constants.USERNAME)
    config.set("hbase.client.password", Constants.PASSWD)
    conn = ConnectionFactory.createConnection(config)
    val tableName: TableName = TableName.valueOf(executeTable)
    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    //设置缓存5m，当达到5m时数据会自动刷到hbase
    params.writeBufferSize(5 * 1024 * 1024) //设置缓存的大小
    mutator = conn.getBufferedMutator(params)
  }

  override def invoke(value: Put, context: SinkFunction.Context[_]): Unit = {
    mutator.mutate(value)
    //每满5000条刷新一下数据
    if (count >= 5000){
      mutator.flush()
      count = 0
    }
    count = count + 1
  }

  override def close(): Unit = {
    close0()
  }

  def close0(): Unit = {
    try {
      if (mutator != null) {
        mutator.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e:Exception => println(e.getMessage)
    }
  }

  //任务初始化时需要执行的代码可以写在此函数里面
  def initProcedure(): Unit = {
    if (executeTable == null) throw new IllegalArgumentException("executeTable can't be null")
  }
}
