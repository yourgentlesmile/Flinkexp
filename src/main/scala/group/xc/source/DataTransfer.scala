package group.xc.source

import com.alibaba.hbase.client.AliHBaseUEConnection
import group.xc.constant.Constants
import group.xc.processor.CommonProcessor
import group.xc.sink.HBaseWriter
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable

object DataTransfer {

  def main(args: Array[String]): Unit = {
    val tableName = args.apply(0)
    val sourceParallel = args.apply(1).toInt
    val sinkParallel = args.apply(2).toInt
    storeAllStartEndKey(tableName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new HBaseReader(tableName, "",false)).setParallelism(sourceParallel)
      .process(new CommonProcessor).setParallelism(sinkParallel)
      .startNewChain()
      .addSink(new HBaseWriter("tablename")).setParallelism(sinkParallel)

    //启用checkpoint 每2.5秒执行一次checkpoint
    env.enableCheckpointing(15000).setStateBackend(new FsStateBackend("hdfs://cdh1:8020/flink"))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    //启用外部checkpoint,当任务取消时不清除checkpoint文件
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    val executionResult = env.execute(tableName)
  }

  /**
    * 储存当前执行表的所有region Start end key,协调表储存在source端集群
    *
    * @param table 需要读取的表名
    */
  def storeAllStartEndKey(table: String): Unit = {
    val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    conf.set("hbase.client.connection.impl", classOf[AliHBaseUEConnection].getName)
    conf.set("hbase.client.endpoint", Constants.CLIENT_ENDPOINT);
    conf.set("hbase.client.username", Constants.USERNAME)
    conf.set("hbase.client.password", Constants.PASSWD)
    val tableNameEntity = TableName.valueOf(table)
    val _conn = ConnectionFactory.createConnection(conf)
    val locator = _conn.getRegionLocator(tableNameEntity)
    val allStartKey = locator.getAllRegionLocations
    //获取region存储表对象
    val _storageTable = _conn.getTable(TableName.valueOf(Constants.COORDINATOR_TABLENAME))
    var i = 1;
    try {
      for (region <- allStartKey) {
        val put = new Put(Bytes.toBytes(table + i))
        val start = if (region.getRegionInfo.getStartKey.size == 0) Constants.ZERO else region.getRegionInfo.getStartKey
        val end = if (region.getRegionInfo.getEndKey.size == 0) Constants.ZERO else region.getRegionInfo.getEndKey
        println(s"set region info start = ${Bytes.toString(start)} , end = ${Bytes.toString(end)}")
        put.addColumn(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_STARTKEY, start)
        put.addColumn(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_ENDKEY, end)
        _storageTable.put(put)
        i += 1
      }
    } finally {
      _storageTable.close()
      _conn.close()
    }
  }

  private def getQualifiers(toEncryptCols: String) = {
    val tuples = mutable.Set[(Array[Byte], Array[Byte])]()
    val strs = toEncryptCols.split(",")
    strs.foreach(x => {
      val s = x.split(":")
      tuples.add((Bytes.toBytes(s(0)), Bytes.toBytes(s(1))))
    })
    tuples.foreach(x => println((s"(${Bytes.toString(x._1)} , ${Bytes.toString(x._2)})")))
    tuples
  }
}
