package akka.persistence.jdbc.journal

import akka.persistence.PersistentRepr
import akka.persistence.jdbc.common.PluginConfig
import akka.persistence.jdbc.util.{ByteString, Base64, EncodeDecode}
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}

trait JdbcStatements {
  def selectMessage(persistenceId: String, sequenceNr: Long): Option[PersistentRepr]

  def insertMessage(persistenceId: String, sequenceNr: Long, marker: String = "A", message: PersistentRepr)

  def updateMessage(persistenceId: String, sequenceNr: Long, marker: String, message: PersistentRepr)

  def deleteMessageSingle(persistenceId: String, sequenceNr: Long)

  def deleteMessageRange(persistenceId: String, toSequenceNr: Long)

  def selectMaxSequenceNr(persistenceId: String): Future[Long]

  def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr]
}

trait GenericStatements extends JdbcStatements with EncodeDecode {
  implicit val executionContext: ExecutionContext
  implicit val session: DBSession
  val cfg: PluginConfig

  implicit val base64 = cfg.base64
  val schema = cfg.journalSchemaName
  val table = cfg.journalTableName

  def createTableIfNotExists()

  def selectMessage(persistenceId: String, sequenceNr: Long): Option[PersistentRepr] =
    SQL(s"SELECT message FROM $schema$table WHERE persistence_id = ? AND sequence_number = ?").bind(persistenceId, sequenceNr)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .single()
      .apply()

  def insertMessage(persistenceId: String, sequenceNr: Long, marker: String = "A", message: PersistentRepr) {
    val msgToWrite = encodeString(Journal.toBytes(message))
    SQL(s"INSERT INTO $schema$table (persistence_id, sequence_number, marker, message, created) VALUES (?,?,?,?, current_timestamp)")
      .bind(persistenceId, sequenceNr, marker, msgToWrite).update().apply
  }

  def updateMessage(persistenceId: String, sequenceNr: Long, marker: String, message: PersistentRepr) {
    val msgToWrite = encodeString(Journal.toBytes(message))
    SQL(s"UPDATE $schema$table SET message = ?, marker = ? WHERE persistence_id = ? and sequence_number = ?")
      .bind(msgToWrite, marker, persistenceId, sequenceNr).update().apply
  }

  def deleteMessageSingle(persistenceId: String, sequenceNr: Long) {
    SQL(s"DELETE FROM $schema$table WHERE sequence_number = ? and persistence_id = ?")
      .bind(sequenceNr, persistenceId).update().apply
  }

  def deleteMessageRange(persistenceId: String, toSequenceNr: Long) {
    SQL(s"DELETE FROM $schema$table WHERE sequence_number <= ? and persistence_id = ?")
      .bind(toSequenceNr, persistenceId).update().apply
  }

  def selectMaxSequenceNr(persistenceId: String): Future[Long] = Future[Long] {
    SQL(s"SELECT MAX(sequence_number) FROM $schema$table WHERE persistence_id = ?")
      .bind(persistenceId)
      .map(_.longOpt(1))
      .single()
      .apply()
      .flatMap(identity)
      .getOrElse(0)
  }

  def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr] = {
    SQL(s"SELECT message FROM $schema$table WHERE persistence_id = ? and (sequence_number >= ? and sequence_number <= ?) ORDER BY sequence_number LIMIT ?")
      .bind(persistenceId, fromSequenceNr, toSequenceNr, max)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .list()
      .apply
  }
}

trait PostgresqlStatements extends GenericStatements {

  def createTableIfNotExists() {
    SQL(s"CREATE TABLE IF NOT EXISTS $schema$table (" +
      s"persistence_id VARCHAR(255) NOT NULL," +
      s"sequence_number BIGINT NOT NULL," +
      s"marker VARCHAR(255) NOT NULL," +
      s"message TEXT NOT NULL," +
      s"created TIMESTAMP NOT NULL," +
      s"PRIMARY KEY(persistence_id, sequence_number)" +
      s")").execute().apply
  }
}

trait MySqlStatements extends GenericStatements{

  def createTableIfNotExists() {
    SQL(s"CREATE TABLE IF NOT EXISTS $schema$table ( " +
      s"persistence_id VARCHAR(255) NOT NULL, " +
      s"sequence_number BIGINT NOT NULL, " +
      s"marker VARCHAR(255) NOT NULL, " +
      s"message TEXT NOT NULL, " +
      s"created TIMESTAMP NOT NULL, " +
      s"PRIMARY KEY(persistence_id, sequence_number) " +
      s")").execute().apply
  }
}

trait H2Statements extends GenericStatements {

  def createTableIfNotExists() {
    SQL(s"CREATE TABLE IF NOT EXISTS $schema$table ( " +
      s"persistence_id VARCHAR(255) NOT NULL, " +
      s"sequence_number BIGINT NOT NULL, " +
      s"marker VARCHAR(255) NOT NULL, " +
      s"message TEXT NOT NULL, " +
      s"created TIMESTAMP NOT NULL, " +
      s"PRIMARY KEY(persistence_id, sequence_number) " +
      s")").execute().apply
  }

  override def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr] = {
    val maxRecords = if (max == java.lang.Long.MAX_VALUE) java.lang.Integer.MAX_VALUE.toLong else max
    SQL(s"SELECT message FROM $schema$table WHERE persistence_id = ? and (sequence_number >= ? and sequence_number <= ?) ORDER BY sequence_number limit ?")
      .bind(persistenceId, fromSequenceNr, toSequenceNr, maxRecords)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .list()
      .apply
  }
}

trait OracleStatements extends GenericStatements {

  def createTableIfNotExists() {
    SQL(s"CREATE TABLE IF NOT EXISTS $schema$table ( " +
      s"persistence_id VARCHAR(255) NOT NULL, " +
      s"sequence_number NUMERIC NOT NULL, " +
      s"marker VARCHAR(255) NOT NULL, " +
      s"message CLOB NOT NULL, " +
      s"created TIMESTAMP NOT NULL, " +
      s"PRIMARY KEY(persistence_id, sequence_number) " +
      s")").execute().apply
  }

  override def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr] = {
    SQL(s"SELECT message FROM $schema$table WHERE persistence_id = ? AND (sequence_number >= ? AND sequence_number <= ?) AND ROWNUM <= ? ORDER BY sequence_number")
      .bind(persistenceId, fromSequenceNr, toSequenceNr, max)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .list()
      .apply
  }
}

trait MSSqlServerStatements extends GenericStatements {

  def createTableIfNotExists() {
    // TODO: Implement me :(
  }

  override def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr] = {
    SQL(s"SELECT TOP ? message FROM $schema$table WHERE persistence_id = ? AND (sequence_number >= ${} AND sequence_number <= ?) ORDER BY sequence_number")
      .bind(max, persistenceId, fromSequenceNr, toSequenceNr)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .list()
      .apply
  }
}

trait DB2Statements extends GenericStatements {
  // FIXME: Add support for DB2 databases?
  override def selectMessagesFor(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[PersistentRepr] = {
    SQL(s"SELECT message FROM $schema$table WHERE persistence_id = ? AND (sequence_number >= ? AND sequence_number <= ?) ORDER BY sequence_number FETCH FIRST ? ROWS ONLY")
      .bind(persistenceId, fromSequenceNr, toSequenceNr, max)
      .map(rs => Journal.fromBytes(decodeBinary(rs.string(1))))
      .list()
      .apply
  }
}

trait InformixStatements extends GenericStatements {

  def createTableIfNotExists() {
    SQL(s"CREATE TABLE IF NOT EXISTS $schema$table ( " +
      s"persistence_id VARCHAR(255) NOT NULL, " +
      s"sequence_number NUMERIC NOT NULL, " +
      s"marker VARCHAR(255) NOT NULL, " +
      s"message CLOB NOT NULL, " +
      s"created DATETIME YEAR TO FRACTION(5) NOT NULL, " +
      s"PRIMARY KEY(persistence_id, sequence_number) " +
      s")").execute().apply
  }
}