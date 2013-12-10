package com.opentable.copy_to_postgres

import scala.util.parsing.combinator._

import com.opentable.copy_to_postgres.mapping._

// Little parser for understanding our custom mapping languages.
// A mapping look something like:
//
//  attr_1 -> col_1, attr_2.attr_3 -> col_2, "abc" -> col_4
// 
// Where each mapping tuple is separated by commas, and each tuple consists
// of an attribute selector and col pair. Attribute selectors support
// extracting from nested data structures via a ".". Also string, double and 
// integer literals can be inserted into columns.
class MappingParser extends RegexParsers {

  val Sep = ","
  val Arrow = "->"
  val Dot = "."
  
  def file: Parser[Mapping] = repsep(mapping, Sep)

  def mapping: Parser[(MapInput,Symbol)] = ((doubleLiteral|longLiteral|stringLiteral|selector) <~ Arrow) ~ ident ^^ {
    case a~b => (a,b)
  }
 
  def selector: Parser[Selector] = (rep1sep(ident,Dot) ^^ { ls:List[Symbol] => Selector(ls) })
 
  def ident: Parser[Symbol] = """\w+""".r ^^ { s => Symbol(s) }

  def stringLiteral: Parser[StringLiteral] = "\"" ~> """\w+""".r <~ "\"" ^^ { s => StringLiteral(s) }
  
  def longLiteral: Parser[LongLiteral] = """-?\d+""".r ^^ { s => LongLiteral(s.toLong) }
  
  def doubleLiteral: Parser[DoubleLiteral] = """-?(\d+\.\d+|\.\d+)""".r ^^ { s => DoubleLiteral(s.toDouble) }
  
  def parse(s: String) = parseAll(file, s) match {
    case Success(res, _) => res
    case e => throw new Exception(e.toString)
  }

}

