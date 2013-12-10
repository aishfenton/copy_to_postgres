package com.opentable.copy_to_postgres

object SourceType extends Enumeration {
  type SourceType = Value
  val JSON,AVRO,CSV = Value
}
