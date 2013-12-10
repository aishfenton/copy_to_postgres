package com.opentable.copy_to_postgres.source

case class Record(attributes: List[(Symbol,Any)]) {
  
  def values = { this.attributes.map(_._2) }

}
