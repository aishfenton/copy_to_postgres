package copy_to_postgres.cli

import java.io.File

import copy_to_postgres.mapping._
import copy_to_postgres._
import copy_to_postgres.Config

object CLI {

  val versionStr: String = this.getClass.getPackage.getImplementationVersion

  private val parser = new scopt.OptionParser[Config]("copy_to_postgres") {
    head("copy_to_postgres", versionStr)

    opt[String]('m', "mapping") required() action { (x, c) =>
      c.copy(mapping = MappingParser.parse(x)) } text("A string expressing the mapping between source columns/fields and target columns.")
    
    opt[String]('t', "table") required() action { (x, c) =>
      c.copy(table = x) } text("The postgres table to copy into.")
 
    opt[String]('d', "db-url") required() action { (x, c) =>
      c.copy(table = x) } text("The URI of the postgres instance to connect to. For example: 'localhost:5432/mydb'.")

    opt[String]('u', "db-user") action { (x, c) =>
      c.copy(dbUser = x) } text("The username of used to connect to the database. Defaults to current shell user's name.")
   
    opt[String]('p', "db-pass") action { (x, c) =>
      c.copy(dbPass = x) } text("The password of used to connect to the database. Defaults to \"\".")

    opt[SourceType.Value]("type") action { (x, c) =>
      c.copy(sourceType = x) } text("The type of the input. Has to be one of CSV, JSON or AVRO. If using a file input source, then we'll try and inferer the file type from the file extension, otherwise it needs to be specified.")
      
    opt[String]("pre") action { (x, c) =>
      c.copy(preCmd = Option(x)) } text("A SQL command that is run prior to the copy, but still within the same transaction. Useful for temporary removing indexes, creating tables, etc.")
      
    opt[String]("post") action { (x, c) =>
      c.copy(postCmd = Option(x)) } text("A SQL command that is run after the copy but still within the same transaction. Useful for adding indexes, etc.")

    opt[String]("csv-sep") action { (x, c) =>
      c.copy(csvSep = x.head) } text("For CSV files this specifies the separator char used. Defaults to ','.")
      
    opt[Unit]("debug") hidden() action { (_, c) =>
      c.copy(debug = true) }
     
    help("help") text("prints this usage text")

    version("version")
    
    arg[File]("<file>...") unbounded() action { (x, c) =>
      c.copy(files = c.files :+ x) } text("input files")
 
    note("""
Example:

>copy_to_postgres -t recs --db-url localhost/test -m ' \
    source_field -> dest_field, \
    "string literal" -> dest_description, \
    2.23 -> dest_field_2, \ 
    1002 -> dest_field_3, \ 
  myfile.avro
"""
)
  
  }

  def parse(args: Array[String]) = {
    parser.parse(args, Config()) match {
      case Some(c) => c
      case None => exit(1) 
    }
  }

}


object App {

  def missingArg(message: String)  = throw new Exception(message)

  def main(args: Array[String]): Unit = {
    val config = CLI.parse(args) 
    println(config) 

    val importer = new Importer(config)
    importer.performImport
  }

}
