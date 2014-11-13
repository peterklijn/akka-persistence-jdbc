package akka.persistence.jdbc.journal

import akka.persistence.jdbc.extension.ScalikeExtension
import scalikejdbc.DBSession

trait GenericJdbcSyncWriteJournal extends JdbcSyncWriteJournal with GenericStatements {
  override implicit val session: DBSession = ScalikeExtension(system).session

  createTableIfNotExists()
}

class PostgresqlSyncWriteJournal extends GenericJdbcSyncWriteJournal with PostgresqlStatements

class MysqlSyncWriteJournal extends GenericJdbcSyncWriteJournal with MySqlStatements

class H2SyncWriteJournal extends GenericJdbcSyncWriteJournal with H2Statements

class OracleSyncWriteJournal extends GenericJdbcSyncWriteJournal with OracleStatements

class MSSqlServerSyncWriteJournal extends GenericJdbcSyncWriteJournal with MSSqlServerStatements

class InformixSyncWriteJournal extends GenericJdbcSyncWriteJournal with InformixStatements