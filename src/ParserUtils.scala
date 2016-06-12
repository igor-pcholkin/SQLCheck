

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.combinator.JavaTokenParsers

trait ParserUtils { self : JavaTokenParsers =>
  class MyRichString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r
    def ic = ignoreCase
  }

  implicit def pimpString(str: String): MyRichString = new MyRichString(str)
  
  lazy val eol = sys.props("line.separator")
  
  // arbitrarily quoted string literal: allows single quotes
  def aqStringLiteral: Parser[String] =
    //("(\"|\')"+"""([^'"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"(\"|\')").r
    ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"\"").r |
    ("\'"+"""([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"\'").r
    
  lazy val aqStringValue = aqStringLiteral ^^ { case v => v.substring(1, v.length - 1) }
}