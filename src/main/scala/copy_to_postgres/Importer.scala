package copy_to_postgres

import java.io.{ File, FileInputStream, BufferedInputStream }
import copy_to_postgres.mapping.Mapping
import copy_to_postgres.source.Record
import copy_to_postgres.strategy.CopyStrategy

class Importer(config: Config) {
  import SourceType._
 
  val strategy = new CopyStrategy(config.dbUrl, config.dbUser, config.dbPass, config.table, config.preCmd, config.postCmd)

  def performImport = {
    
    val inputStreams = getInputStreams(config.files).toIterator

    val records = config.sourceType match {
      case AVRO => new source.Avro(inputStreams, config.mapping)
      case CSV => new source.CSV(inputStreams, config.mapping, config.csvSep)
      //case JSON => new source.JSON(inputStream, config.mapping)
    }

    strategy.importRecords(records, config.mapping.map(_._2))
  }

  private def getInputStreams(files: Seq[File]) = {
    if (files.isEmpty)
      files.map( f => new FileInputStream(f) )
    else
      List(new BufferedInputStream(System.in))
  }

}
