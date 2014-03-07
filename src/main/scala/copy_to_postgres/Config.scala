package copy_to_postgres

import java.io.File
import copy_to_postgres.mapping._

object SourceType extends Enumeration {
  type SourceType = Value
  val JSON,AVRO,CSV = Value

  implicit val sourceTypeRead: scopt.Read[SourceType.Value] = scopt.Read.reads(SourceType withName _)
}

case class Config(
  dbUrl: String = "", dbUser: String = System.getProperty("user.name"), dbPass: String = "", 
  table: String = "", mapping: Mapping = List[(MapInput,Symbol)](), 
  preCmd: Option[String] = None, postCmd: Option[String] = None,
  csvSep: Char = ',', debug: Boolean = false,
  sourceType: SourceType.Value = SourceType.CSV, files: Seq[File] = Seq()
)

