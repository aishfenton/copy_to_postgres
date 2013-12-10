package com.opentable.copy_to_postgres.strategy

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.opentable.copy_to_postgres.source.Record

trait ImportStrategy {
  val dbUrl: String
  val dbConnectionProps: Map[Symbol,String]
  val table: String
  val aroundCommands: (Option[String],Option[String]) 

  Class.forName("org.postgresql.Driver");
  val connStr = s"jdbc:postgresql://$dbUrl?user=${dbConnectionProps('db_user)}&password=${dbConnectionProps('db_pass)}"

  var conn:BaseConnection = null; 
  
  def init:Unit = {
    conn = DriverManager.getConnection(connStr).asInstanceOf[BaseConnection]
    conn.setAutoCommit(false)
  }
  
  protected def performImport(records: Iterator[Record], destColumns: Seq[Symbol]): Unit;
  
  def complete:Unit = {
    conn.commit
    conn.setAutoCommit(true) 
    conn.close
  }
  
  def importRecords(records: Iterator[Record], destColumns: Seq[Symbol]) = {
    init
    try {
      execPreCmd

      println(s"Importing ${destColumns.mkString(",")} into ${this.table}")
      performImport(records, destColumns)

      execPostCmd
    } finally {
      complete
    }
  }

  protected def execPreCmd = {
    println(s"Exec pre command: ${aroundCommands._1}")
    aroundCommands._1.foreach { cmd => conn.createStatement.execute(cmd) }
  }
  
  protected def execPostCmd = {
    println(s"Exec post command: ${aroundCommands._2}")
    aroundCommands._2.foreach { cmd => conn.createStatement.execute(cmd) }
  }

}
