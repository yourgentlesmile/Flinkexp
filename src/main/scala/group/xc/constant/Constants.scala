package group.xc.constant

import org.apache.hadoop.hbase.util.Bytes

object Constants {
  val ZERO: Array[Byte] = Bytes.toBytes("zero")

  val COORDINATOR_FAMILY: Array[Byte] = Bytes.toBytes("f")

  val COORDINATOR_STARTKEY: Array[Byte] = Bytes.toBytes("s")

  val COORDINATOR_ENDKEY: Array[Byte] = Bytes.toBytes("e")

  val COORDINATOR_TABLENAME: String = "regionCoordinator"

  val CLIENT_ENDPOINT: String = "fill it"

  val USERNAME: String = "username"

  val PASSWD: String = "password"
}
