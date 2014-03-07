package copy_to_postgres.mapping

sealed trait MapInput

case class Selector(path: List[Symbol]) extends MapInput

sealed trait Literal[T] extends MapInput {
  val value:T
  override def toString = value.toString
}

case class StringLiteral(value:String) extends Literal[String]
case class DoubleLiteral(value:Double) extends Literal[Double]
case class LongLiteral(value:Long) extends Literal[Long]
