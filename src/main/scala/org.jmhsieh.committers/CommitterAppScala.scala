package org.jmhsieh.committers

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.mortbay.jetty.Server
import org.slf4j.{Logger, LoggerFactory}

case class Committer(val name: String, val hair: String, val beard: String, val jiras: Map[String, String])


object CommitterAppScala extends App {
  val log = LoggerFactory.getLogger("CommitterAppScala")
  val hbaseConnectionFactory = new HBaseConnectionFactory(HBaseConfiguration.create(), TableName.valueOf("committers"))
  log.info("Hello World from Scala")
  val server = new Server(32322)
  server.addHandler(new DefaultRequestHandler(hbaseConnectionFactory))
  server.start()
  server.join()
}