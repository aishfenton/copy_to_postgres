package copy_to_postgres.mapping

import scala.util.parsing.combinator._

import copy_to_postgres.mapping._

// Little parser for understanding our custom mapping languages.
// A mapping look something like:
//
//  attr_1 -> col_1, attr_2.attr_3 -> col_2, "abc" -> col_4
// 
// Where each mapping tuple is separated by commas, and each tuple consists
// of an attribute selector and col pair. Attribute selectors support
// extracting from nested data structures via a ".". Also string, double and 
// integer literals can be inserted into columns.
object MappingParser extends RegexParsers {

  lazy val Sep = ","
  lazy val Arrow = "->"
  lazy val Dot = "."
  
  lazy val file: Parser[Mapping] = repsep(mapping, Sep)

  lazy val mapping: Parser[(MapInput,Symbol)] = ((doubleLiteral|longLiteral|stringLiteral|selector) <~ Arrow) ~ ident ^^ {
    case a~b => (a,b)
  }
 
  lazy val selector: Parser[Selector] = (rep1sep(ident,Dot) ^^ { ls:List[Symbol] => Selector(ls) })
 
  lazy val ident: Parser[Symbol] = """\w[\w_\-\d]*""".r ^^ { s => Symbol(s) }

  lazy val stringLiteral: Parser[StringLiteral] = "\"" ~> """[^\"]*""".r <~ "\"" ^^ { s => StringLiteral(s) }
  
  lazy val longLiteral: Parser[LongLiteral] = """-?\d+""".r ^^ { s => LongLiteral(s.toLong) }
  
  lazy val doubleLiteral: Parser[DoubleLiteral] = """-?(\d+\.\d+|\.\d+)""".r ^^ { s => DoubleLiteral(s.toDouble) }
  
  def parse(s: String) = parseAll(file, s) match {
    case Success(res, _) => res
    case e => throw new Exception(e.toString)
  }

}

