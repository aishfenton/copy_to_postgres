package com.opentable.copy_to_postgres.strategy

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.opentable.copy_to_postgres.source.Record
import com.opentable.copy_to_postgres.util.IteratorInputStream

class CopyStrategy(override val dbUrl: String, override val dbConnectionProps: Map[Symbol,String], override val table: String, override val aroundCommands: (Option[String],Option[String])) extends ImportStrategy {

  val Delimiter = "\037"

  private def copyCmd(destColumns: Seq[Symbol]) = s"""
    COPY ${this.table} (${destColumns.map(_.name).mkString(",")}) FROM STDIN WITH DELIMITER '${Delimiter}';
  """
  
  override protected def performImport(records: Iterator[Record], destColumns: Seq[Symbol]): Unit = {
    val copyManager = new CopyManager(conn);
    val inputStream = new IteratorInputStream(records.map { buildRecordString })

    copyManager.copyIn(copyCmd(destColumns), inputStream);
  }

  private def buildRecordString(record: Record) = {
    record.values.map( mkString ).mkString(Delimiter)
  }

  private def mkString(value: Any): String = value match {
    case v:String => v.toString.replaceAll("\n", "\\r").replaceAll("\r", "\\r")
    case v:Array[_] => mkString(v.toSeq)
    case v:Seq[_] => "{" + v.map(mkString).mkString(",") + "}"
    case null => ""
    case v@_ => v.toString    
  }
  
}
