import org.specs2._

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

class ImporterSpec extends Specification { def is = s2"""

 ${ Step(createDatabase("test")) }

 Tests end-to-end that everything works 

 The 'standard mapping' string should
   contain 11 characters                                         $e1
   start with 'Hello'                                            $e2
   end with 'world'                                              $e3
                                                                 """

  def e1 = "Hello world" must have size(11)
  def e2 = "Hello world" must startWith("Hello")
  def e3 = "Hello world" must endWith("world")


  def createDatabase(name: String) = {
    Class.forName("org.postgresql.Driver");
    val connStr = s"jdbc:postgresql://$dbUrl?user${dbUser}&password=${dbPass}"

    val conn = DriverManager.getConnection(connStr).asInstanceOf[BaseConnection]
    conn.createStatement.execute(cmd)
  }
  
}


