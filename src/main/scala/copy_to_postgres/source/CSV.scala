package copy_to_postgres.source

import com.bizo.mighty.csv.CSVDictReader
import com.bizo.mighty.csv.CSVReaderSettings
import copy_to_postgres.mapping.Selector
import copy_to_postgres.mapping.Mapping
import java.io.{ File, InputStream }

class CSV(inputStreams: Iterator[InputStream], mapping: Mapping, sep:Char = ',') extends Source[Map[String,String]] {

  private val settings = CSVReaderSettings.Standard.copy(separator = sep)
  private var inputStream: Iterator[Map[String,String]] = nextFileStream

  override def hasNext: Boolean = {
    while(!inputStream.hasNext) {
      if (inputStreams.hasNext)
        inputStream = nextFileStream
      else
        return false
    }
    
    return true
  }

  override def next = {
    val record = inputStream.next
    applyMapping(record, mapping)
  }

  private def nextFileStream = {
    val inputStream = inputStreams.next
    CSVDictReader(inputStream)(settings)
  }

  override protected def select(record: Map[String,String], selector: Selector):Any = record(selector.path.head.name)

}


