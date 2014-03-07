import org.specs2._

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

object Database {
  Class.forName("org.postgresql.Driver");
  
  private val connStr = s"jdbc:postgresql://localhost?user=test"
  
  private val CreateDbCmd = """
    CREATE DATABASE ctp_test
  """

  private val CreateSchemaCmd = """
    CREATE TABLE test
    (
      int_col integer,
      real_col real,
      double_col double precision,
      varchar_col varchar(255),
      bool_col boolean,
      timestamp_col timestamp with time zone
    )
  """

  private val CleanCmd = """
    TRUNCATE test
  """
  
  lazy val conn = {
    DriverManager.getConnection(connStr).asInstanceOf[BaseConnection]
  }
  
  lazy val createDb = { 
    println("Creating test database")
    //conn.createStatement.execute(CreateDbCmd)
    conn.createStatement.execute(CreateSchemaCmd)
  }

  def cleanDb = {
    conn.createStatement.execute(CreateSchemaCmd)
  }
  
}

// use the createDB lazy val to create the database once for every specification inheriting from
// the DatabaseSpec trait
trait DatabaseSpec extends Specification {
  lazy val Db = Database

  def conn = Db.conn
 
  override def map(fs: => Fragments) = Step(Db.createDb) ^ Step(startDb) ^ fs ^ Step(cleanDb)
}

