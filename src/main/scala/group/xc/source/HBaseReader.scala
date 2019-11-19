package group.xc.source

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.hbase.client.AliHBaseUEConnection
import group.xc.constant.Constants
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * source端,从HBase通过scan的方式获取全部数据
 */
class HBaseReader(executeTable: String,col: String, key: Boolean)
    extends RichParallelSourceFunction[(String,Result)] with CheckpointedFunction{
  @transient
  private var conn: Connection = _
  private var table: Table = _
  private var readColumn: String = col
  private var needRowkey: Boolean = key
  private val numRecords = new LongCounter()
  @volatile
  private var checkpointedState: ListState[List[Array[Byte]]] = _
  private var currentRowkey: Array[Byte] = _
  private var endRowkey: Array[Byte] = _
  private var isClosed = false
  /**
   * checkpoint状态恢复,任务首次创建的时候也会执行一次
   *
   * @param functionInitializationContext
   */
  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {
    val listStateDescriptor = new ListStateDescriptor("checkPointedList",
      TypeInformation.of(new TypeHint[List[Array[Byte]]]() {}))
    checkpointedState = functionInitializationContext.getOperatorStateStore.getListState(listStateDescriptor)
    //是否是从checkpoint中恢复
    if (functionInitializationContext.isRestored) {

      val list = new ArrayBuffer[List[Array[Byte]]]()
      for (state <- checkpointedState.get()) {
        list.add(state)
      }
      if (list.size != 1) {
        throw new IllegalArgumentException("invalid state. not equal 1 ")
      }
      currentRowkey = list.get(0).get(0)
      endRowkey = list.get(0).get(1)
      readColumn = if(list.get(0).get(2).sameElements(Constants.ZERO)) null else Bytes.toString(list.get(0).get(2))
      needRowkey = Bytes.toBoolean(list.get(0).get(3))
      println(s"[${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} " +
        s"Runtime-$executeTable] subtask ${getRuntimeContext.getIndexOfThisSubtask} recover from checkpoint " +
        s" startkey = ${Bytes.toString(currentRowkey)} endkey = ${Bytes.toString(endRowkey)} " +
        s"readColumn = $readColumn needRowkey = $needRowkey")
      //先处理先前的连接，如果存在
      close0()
    } else {
      //首次启动任务，需要从hbase获取到需要执行的startKey与endkey
      getReadRange(true)
    }
  }

  override def open(parameters: Configuration): Unit = {
    initProcedure()
    generateConnection()
    getRuntimeContext.addAccumulator("numRecords",this.numRecords)
  }

  override def run(sourceContext: SourceFunction.SourceContext[(String, Result)]): Unit = {
    try {
      do {
        if (isClosed) return
        if (table != null) table.close()
        table = conn.getTable(TableName.valueOf(executeTable))
        val recordStart = numRecords.getLocalValue
        val scan = new Scan()
        scan.setCacheBlocks(false)
        scan.withStartRow(currentRowkey)
        scan.withStopRow(endRowkey)
        if (!needRowkey && readColumn != null) {
          for (column <- readColumn.split(",")) {
            scan.addColumn(Bytes.toBytes(column.split(":").apply(0)), Bytes.toBytes(column.split(":").apply(1)))
          }
        }
        println(s"[${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} " +
          s"Runtime-$executeTable] subtask ${getRuntimeContext.getIndexOfThisSubtask} start run " +
          s" startkey = ${Bytes.toString(scan.getStartRow)} endkey = ${Bytes.toString(scan.getStopRow)} " +
          s"needRowkey = $needRowkey readColumn = $readColumn numRecord start = $recordStart ")
        val rs: ResultScanner = table.getScanner(scan)
        for (result <- rs) {
          currentRowkey = result.getRow
          val rowkey = Bytes.toString(result.getRow)
          sourceContext.collect((rowkey, result))
          numRecords.add(1)
        }
        val recordStop = numRecords.getLocalValue
        println(s"[${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} " +
          s"Runtime-$executeTable] subtask ${getRuntimeContext.getIndexOfThisSubtask} finished " +
          s"startkey = ${Bytes.toString(currentRowkey)} endkey = ${Bytes.toString(endRowkey)} " +
          s"numRecord stop = $recordStop consumed = ${recordStop - recordStart}")
      } while (getReadRange(false))
    } catch {
      case e: Exception => {
        println("-=-=-=-=- source trigger a exception -=-=-=-=")
        e.printStackTrace()
      }
    }
    sourceContext.close()
  }

  /**
   * checkpoint 状态储存
   *
   * @param functionSnapshotContext
   */
  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()

    val cache = currentRowkey ::
      endRowkey ::
      (if(readColumn == null) Constants.ZERO else Bytes.toBytes(readColumn)) ::
      Bytes.toBytes(needRowkey) :: Nil
    checkpointedState.add(cache)
  }

  /**
   * 创建连接
   */
  def generateConnection(): Unit = {
    val configuration: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    configuration.set("hbase.client.connection.impl", classOf[AliHBaseUEConnection].getName)
    configuration.set("hbase.client.endpoint", Constants.CLIENT_ENDPOINT)
    configuration.set("hbase.client.username", Constants.USERNAME)
    configuration.set("hbase.client.password", Constants.PASSWD)
    val tableName = TableName.valueOf(executeTable)
    conn = ConnectionFactory.createConnection(configuration)
  }

  //任务初始化时需要执行的代码可以写在此函数里面
  def initProcedure(): Unit = {
    if (executeTable == null) throw new IllegalArgumentException("executeTable can't be null")
  }

  /**
   * 从hbase获得需要执行的region
   * @param isInit 是否是在第一次初始化时调用
   * @return
   */
  def getReadRange(isInit: Boolean): Boolean = {
    val retryPolicy = new ExponentialBackoffRetry(1000,3)
    val client = CuratorFrameworkFactory.newClient("cdh1:2181,cdh2:2181,cdh3:2181",retryPolicy)
    client.start()
    val mutex = new InterProcessMutex(client, "/regionCoordinator/lock")
    var coordinateTable: Table = null
    try {
      //1、获取zookeeper锁之后才能访问hbase region协调表
      mutex.acquire()
      val configuration: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
      configuration.set("hbase.client.connection.impl", classOf[AliHBaseUEConnection].getName)
      configuration.set("hbase.client.endpoint", Constants.CLIENT_ENDPOINT)
      configuration.set("hbase.client.username", Constants.USERNAME)
      configuration.set("hbase.client.password", Constants.PASSWD)
      val tableName = TableName.valueOf(Constants.COORDINATOR_TABLENAME)
      val _conn = ConnectionFactory.createConnection(configuration)
      coordinateTable = _conn.getTable(tableName)

      val scan = new Scan()
      scan.setCacheBlocks(false)
      scan.setLimit(1)
      var result: Result = null
      for (t <- coordinateTable.getScanner(scan).iterator()) {
        result = t
      }
      if (result == null) {
        println(s"All region has been consumed.subtask ${getRuntimeContext.getIndexOfThisSubtask} finish")
        isClosed = true
        return false
      }
      //2、获取一个region信息，取startKey与EndKey的时候需要判断是否为null，这要特殊处理，在写入的时候遇到null会写zero字串
      val beginKey = Bytes.toString(result.getValue(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_STARTKEY))
      val stopKey = Bytes.toString(result.getValue(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_ENDKEY))
      currentRowkey = if (beginKey.equals(Bytes.toString(Constants.ZERO))) null
      else result.getValue(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_STARTKEY)
      endRowkey = if (stopKey.equals(Bytes.toString(Constants.ZERO))) null
      else result.getValue(Constants.COORDINATOR_FAMILY, Constants.COORDINATOR_ENDKEY)
      println(s"[${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} TaskChange-$executeTable] subtask ${getRuntimeContext.getIndexOfThisSubtask} receive a mission " +
        s"needrowkey = $needRowkey , startkey = ${Bytes.toString(currentRowkey)} , endkey = ${Bytes.toString(endRowkey)} " +
        s" original begin = $beginKey , stop = $stopKey")
      //3、获取到了对应的region之后删除掉该region，避免其他task读取到
      coordinateTable.delete(new Delete(result.getRow))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        isClosed = true
        return false
      }
    } finally {
      //4、释放zookeeper锁，避免阻塞其他subtask
      mutex.release()
      coordinateTable.close()
      client.close()
    }
    true
  }

  override def cancel(): Unit = {
    close0()
  }

  override def close(): Unit = {
    close0()
  }

  def close0(): Unit = {
    try {
      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e:Exception => println(e.getMessage)
    }
  }
}