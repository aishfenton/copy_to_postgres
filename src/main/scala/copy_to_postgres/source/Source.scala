package copy_to_postgres.source

import copy_to_postgres.mapping._
import scala.collection.mutable.ListBuffer

trait Source[T] extends Iterator[Record] {

  protected def select(record: T, selector: Selector): Any; 

  protected def applyMapping(record: T, mapping: Mapping): Record = {
    val attributes = new ListBuffer[(Symbol,Any)]()
    
    mapping.foreach { t:(MapInput,Symbol) => 
      val (mapInput, name) = t 
      
      val value:Any = mapInput match {
        case l:Literal[_] => l
        case s:Selector => { select(record, s) }
      }
      
      attributes += Tuple2(name, value)
    }
    new Record(attributes.toList)
  }
  
}
