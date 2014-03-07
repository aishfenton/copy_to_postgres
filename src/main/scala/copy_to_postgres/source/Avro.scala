package copy_to_postgres.source

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.{ File, InputStream }

import copy_to_postgres.mapping.Selector
import copy_to_postgres.mapping.Mapping

class Avro(inputStreams: Iterator[InputStream], mapping: Mapping) extends Source[GenericRecord] {

  var fileStream:DataFileStream[GenericRecord] = nextFileStream;
  
  // We resuse the Avro record in the iterator for speed
  var avroRecord: GenericRecord = null;

  override def hasNext: Boolean = {
    while(!fileStream.hasNext) {
      if (inputStreams.hasNext)
        fileStream = nextFileStream
      else
        return false
    }
    
    return true 
  }

  override def next = {
    avroRecord = fileStream.next(avroRecord)
    applyMapping(avroRecord,mapping)
  }

  private def nextFileStream = {
    val is = inputStreams.next
    new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord])    
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

