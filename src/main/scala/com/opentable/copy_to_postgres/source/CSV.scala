package com.opentable.copy_to_postgres.source

import com.bizo.mighty.csv.CSVDictReader
import com.bizo.mighty.csv.CSVReaderSettings
import com.opentable.copy_to_postgres.mapping.Selector
import com.opentable.copy_to_postgres.mapping.Mapping
import java.io.File

class CSV(files: List[File], mapping: Mapping, sep:Char = ',') extends Source[Map[String,String]] {

  private val settings = CSVReaderSettings.Standard.copy(separator = sep)
  
  private var fileIterator: Iterator[File] = files.toIterator
  private var fileReader: Iterator[Map[String,String]] = nextFileReader


  override def hasNext: Boolean = {
    while(!fileReader.hasNext) {
      if (fileIterator.hasNext)
        fileReader = nextFileReader
      else
        return false
    }
    
    return true
  }

  override def next = {
    val record = fileReader.next
    println(record)
    applyMapping(record, mapping)
  }

  private def nextFileReader = {
    val file = fileIterator.next
    println(s"Reading ${file.getName}")
    println(settings)
    CSVDictReader(file.getPath)(settings)
  }

  override protected def select(record: Map[String,String], selector: Selector):Any = record(selector.path.head.name)

}


