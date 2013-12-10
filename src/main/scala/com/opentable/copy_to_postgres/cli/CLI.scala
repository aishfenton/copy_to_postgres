package com.opentable.copy_to_postgres.cli

import java.io.File

import com.opentable.copy_to_postgres._

object CLI {
  val usage = """
Usage: 
  copy_to_postgres -m mapping -t table --db-url url [--type JSON|AVRO|CSV] [--db-user user] [--db-pass pass] [--version] input-file

Example:

>copy_to_postgres -t recs --type AVRO --db-url localhost/test -m ' \
    source_field -> dest_field, \
    "string literal" -> dest_description, \
    2.23 -> dest_field_2, \ 
    1002 -> dest_field_3, \ 
  myfile.avro
"""

  type OptionMap = Map[Symbol,String]

  private def getVersion = {
    getClass.getPackage.getImplementationVersion
  }

  //def inferType(filename) = { 'avro }

  def parseArgs(args: Array[String]): OptionMap = {
    if (args.length == 0) println(usage)

    def nextOption(map: OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "-m" :: value :: tail =>
          nextOption(map ++ Map('mapping -> value), tail)
        case "-t" :: value :: tail =>
          nextOption(map ++ Map('table -> value), tail)
        case "--db-url" :: value :: tail => 
          nextOption(map ++ Map('db_url -> value), tail)
        case "--db-user" :: value :: tail => 
          nextOption(map ++ Map('db_user -> value), tail)
        case "--db-pass" :: value :: tail => 
          nextOption(map ++ Map('db_pass -> value), tail)
        case "--pre-cmd" :: value :: tail =>
          nextOption(map ++ Map('pre_cmd -> value), tail)
        case "--post-cmd" :: value :: tail =>
          nextOption(map ++ Map('post_cmd -> value), tail)
        case "--type" :: value :: tail =>
          nextOption(map ++ Map('type -> value), tail)
        case "--version" :: _ => {
          println("version: " + getVersion.toString)
          exit(0)
        }
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), Nil)
        case option :: tail => {
          println("Unknown option " + option) 
          exit(1)
        }
      }
    }

    val options = nextOption(Map(), args.toList)
    options
  }

}

object App {

  def missingArg(message: String) = throw new Exception(message)

  def main(args: Array[String]) = {
    val options = CLI.parseArgs(args) 
    println(options) 

    val file = new File(options.getOrElse('infile, missingArg("Missing input file argument. This must be a file or directory")))
    val fileType = SourceType.withName(options('type))
    val mapping = (new MappingParser).parse(options('mapping))
    val table = options('table)
    val dbURL = options.getOrElse('db_url, missingArg("Parameter db_url is required. e.g. --db-url localhost:5432/mydb"))
    val dbConnectionProps = Map('db_user -> options.getOrElse('db_user,System.getProperty("user.name")), 'db_pass -> options.getOrElse('db_pass,""))

    val aroundCommands = (options.get('pre_cmd), options.get('post_cmd))

    val importer = new Importer(dbURL, dbConnectionProps, table, aroundCommands)
    importer.performImport(file, fileType, mapping)
  }
}
