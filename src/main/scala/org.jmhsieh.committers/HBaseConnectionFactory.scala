package org.jmhsieh.committers

import java.util

import collection.JavaConverters._
import collection.mutable._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.util._
import org.apache.hadoop.conf._
import org.apache.hadoop.hbase.util.Bytes

import scala.runtime.Nothing$

/**
  * Created by skinibizapps on 11/25/16.
  */


class HBaseConnectionFactory(val conf: Configuration, val tableName: TableName) {

  val DEFAULT_NAME: TableName = TableName.valueOf("committers")
  val DATA_CF: Array[Byte] = Bytes.toBytes("d")
  val JIRA_CF: Array[Byte] = Bytes.toBytes("j")
  val HAIR_KEY: Array[Byte] = Bytes.toBytes("hair")
  val BEARD_KEY: Array[Byte] = Bytes.toBytes("beard")
  val c: Connection = ConnectionFactory.createConnection(conf)
  setupTable(conf, tableName)

  def setupTable(configuration: Configuration, name: TableName) : Unit = {
    val a: Admin = c.getAdmin
    if (!a.tableExists(name)) {
      val htd = new HTableDescriptor(name)
      htd.addFamily(new HColumnDescriptor(DATA_CF))
      htd.addFamily(new HColumnDescriptor(JIRA_CF))
      a.createTable(htd)
    }
  }

  def getTable(name: TableName): Table = {
    c.getTable(name)
  }

  def writeCommitter(c: Committer, table: Table ): Unit = {

    if (c == null) {
      return
    }
    table.put(fromCommitter(c))
  }

  def writeCommitters(cs: util.Collection[Committer], table: Table): Unit = {
    val l = cs.asScala.toList.map(committer => fromCommitter(committer)).asJava
    table.put(l)
  }

  def fromCommitter(c: Committer) : Put = {
    val p: Put = {new Put(Bytes.toBytes(c.name))}
    if (c.beard != null)
      p.addColumn(DATA_CF, BEARD_KEY, Bytes.toBytes(c.beard))
    if (c.hair != null)
      p.addColumn(DATA_CF, HAIR_KEY, Bytes.toBytes(c.hair))
    c.jiras.map( entry => p.addColumn(JIRA_CF, Bytes.toBytes(entry._1), Bytes.toBytes(entry._2)))
    p
  }

  // read operations

  def readCommitter(name: String, table: Table): Committer = {
    val g: Get = new Get(Bytes.toBytes(name))
    val r: Result = table.get(g)
    fromResult(r)
  }

  def fromResult(result: Result): Committer =  {

    val r: Result = if(result == null &&  result.getRow == null) return null else result
    val name = Bytes.toString(r.getRow)
    val hair =  Bytes.toString(r.getValue(DATA_CF, HAIR_KEY))
    val beard = Bytes.toString(r.getValue(DATA_CF, BEARD_KEY))

    val jiras = r.getFamilyMap(JIRA_CF).asScala.map( elem => Bytes.toString(elem._1) -> Bytes.toString(elem._2))

    Committer(name, hair, beard, jiras.toMap)
  }


  def scanner(startName: String, endName: String, table: Table): Iterator[Committer]  = {
    var newEndName = endName
    var newStartName = startName

    if (startName != null && endName != null && startName.compareTo(endName) > 0) {
      // swap if not sorted properly.
      val tmp = newEndName
      newEndName = newStartName
      newStartName = tmp
    }
    val startNameBytes = if(newStartName == null) HConstants.EMPTY_START_ROW else Bytes.toBytes(newStartName)
    val endNameBytes = if(newEndName == null) HConstants.EMPTY_END_ROW else Bytes.toBytes(newEndName)
    val s: Scan = new Scan( startNameBytes, endNameBytes)

    val rsIterator = table.getScanner(s).iterator().asScala.toList

    val listOfCommitters = rsIterator.map(result => fromResult(result))
    listOfCommitters.iterator
  }
}
