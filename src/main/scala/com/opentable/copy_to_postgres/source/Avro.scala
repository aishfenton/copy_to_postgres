package com.opentable.copy_to_postgres.source

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File

import com.opentable.copy_to_postgres.mapping.Selector
import com.opentable.copy_to_postgres.mapping.Mapping

class Avro(files: List[File], mapping: Mapping) extends Source[GenericRecord] {

  var fileIterator: Iterator[File] = files.toIterator
  var fileReader:DataFileReader[GenericRecord] = nextFileReader;
  
  // We resuse the Avro record in the iterator for speed
  var avroRecord: GenericRecord = null;

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
    avroRecord = fileReader.next(avroRecord)
    applyMapping(avroRecord,mapping)
  }

  private def nextFileReader = {
    val file = fileIterator.next
    println(s"Reading ${file.getName}")
    new DataFileReader[GenericRecord](file, new GenericDatumReader[GenericRecord])    
  }

  override protected def select(record: GenericRecord, selector: Selector):Any = {
    val v = selector.path.foldLeft[Any](record) { (result,key) =>
      result.asInstanceOf[GenericRecord].get(key.name)
    }
    toScalaType(v)
  }
  
  private def toScalaType(obj: Any) = obj match {
    case v:GenericData.Array[_] => v.toArray; 
    case v@_ => v
  }

}

