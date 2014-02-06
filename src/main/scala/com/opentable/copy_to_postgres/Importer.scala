package com.opentable.copy_to_postgres

import java.io.File
import com.opentable.copy_to_postgres.mapping.Mapping
import com.opentable.copy_to_postgres.source.Record
import com.opentable.copy_to_postgres.strategy.CopyStrategy

class Importer(dbUrl: String, dbConnectionProps: Map[Symbol,String], table: String, aroundCommands: (Option[String],Option[String]),
  miscOptions: Map[Symbol,String]) {
  import SourceType._
 
  val strategy = new CopyStrategy(dbUrl, dbConnectionProps, table, aroundCommands)
  
  // If directory, then make into list of files
  private def expandFile(file: File) = {
    println(file)
    if (file.isDirectory)
      file.listFiles.filter(_.isFile).toList
    else
      List(file)
  }

  def performImport(file: File, fileType: SourceType, mapping: Mapping) = {

    val files = expandFile(file)

    val records = fileType match {
      case AVRO => new source.Avro(files, mapping)
      case CSV => new source.CSV(files, mapping, miscOptions.getOrElse('csv_sep, ",").charAt(0))
      //case JSON => new source.JSON
    }

    strategy.importRecords(records, mapping.map(_._2))
  }

}
