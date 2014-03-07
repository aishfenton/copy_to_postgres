package copy_to_postgres.strategy

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import copy_to_postgres.source.Record

trait ImportStrategy {
  val dbUrl: String
  val dbUser: String
  val dbPass: String
  val table: String
  val preCmd: Option[String]
  val postCmd: Option[String]

  Class.forName("org.postgresql.Driver");
  val connStr = s"jdbc:postgresql://$dbUrl?user${dbUser}&password=${dbPass}"

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
    preCmd map { cmd => 
      println(s"Exec pre command: ${cmd}")
      conn.createStatement.execute(cmd) 
    }
  }
  
  protected def execPostCmd = {
    postCmd map { cmd => 
      println(s"Exec pre command: ${cmd}")
      conn.createStatement.execute(cmd) 
    }
  }

}
