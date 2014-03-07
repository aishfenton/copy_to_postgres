package copy_to_postgres.mapping

import org.specs2.mutable._
import org.specs2.matcher.ParserMatchers
import copy_to_postgres.mapping.MappingParser.{mapping,stringLiteral,longLiteral,doubleLiteral,selector,file,ident}

class MappingParserSpec extends Specification with ParserMatchers {

  val parsers = MappingParser 
  
  "Ident Parser" should {

    "Handle single chars" in { 
      ident must succeedOn("a").withResult(equalTo( 'a ))
    }

    "Handle special chars in ident" in { 
      ident must succeedOn("a_b-1").withResult(equalTo( Symbol("a_b-1") ))
    }

  }

  "Literal Parser" should {

    "Handle strings" in { 
      stringLiteral must succeedOn("\"abc\"").withResult(equalTo( StringLiteral("abc") ))
      stringLiteral must failOn("abc")
    }

    "Handle longs" in { 
      longLiteral must succeedOn("1").withResult(equalTo( LongLiteral(1) ))
      longLiteral must succeedOn("-1").withResult(equalTo( LongLiteral(-1) ))
      longLiteral must failOn("1.0")
    }

    "Handle doubles" in { 
      doubleLiteral must succeedOn("0.1").withResult(equalTo( DoubleLiteral(0.1) ))
      doubleLiteral must succeedOn(".1").withResult(equalTo( DoubleLiteral(0.1) ))
      doubleLiteral must succeedOn("-0.1").withResult(equalTo( DoubleLiteral(-0.1) ))
      doubleLiteral must succeedOn("-.1").withResult(equalTo( DoubleLiteral(-0.1) ))
      doubleLiteral must failOn("1")
      doubleLiteral must failOn("-1")
    }
 
  }

  "Selector Parser" should {

    "Handle 1 ident" in { 
      selector must succeedOn("abc").withResult(equalTo( Selector(List('abc)) ))
    }

    "Handle many idents" in { 
      selector must succeedOn("a.b.c").withResult(equalTo( Selector(List('a,'b,'c)) ))
    }
    
  }
  
  "Mapping Parser" should {

    "Handle selector to a column" in { 
      mapping must succeedOn("a -> b").withResult(equalTo( (Selector(List('a)),'b) ))
      mapping must succeedOn("a.b.c -> b").withResult(equalTo( (Selector(List('a,'b,'c)),'b) ))
    }

    "Handle literals to a column" in { 
      mapping must succeedOn("\"mystring\"->b").withResult(equalTo( (StringLiteral("mystring"),'b) ))
      mapping must succeedOn("-102 -> b").withResult(equalTo( (LongLiteral(-102),'b) ))
      mapping must succeedOn("-1.234 -> b").withResult(equalTo( (DoubleLiteral(-1.234),'b) ))
    }
  
  }

  "File Parser" should {

    "Handle multiple mappings with different whitespace" in { 
      file must succeedOn("a -> b, \"c\"->d_1, 0.4 ->e").withResult(equalTo(List(
        (Selector(List('a)),'b),
        (StringLiteral("c"),'d_1),
        (DoubleLiteral(0.4),'e)
      )))
    }
 
  }
  
}

