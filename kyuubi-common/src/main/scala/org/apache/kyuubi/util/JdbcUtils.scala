/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.util

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Locale
import javax.sql.DataSource

import scala.util.control.NonFatal

import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging

object JdbcUtils extends Logging {

  // Bounded, backing-off retry for read-only-transaction failures during a managed-database
  // failover. Two failure shapes are covered:
  //   - the pool holds connections pinned to a demoted node while the endpoint already points at a
  //     writable primary. Evicting is enough and the very next attempt succeeds, so most of the
  //     budget is never spent.
  //   - the server itself is still read-only mid-failover. Only waiting helps, so back off.
  // 5 attempts with 1s/2s/4s/4s of backoff, i.e. ~11s of added latency in the worst case, which
  // keeps a submission well inside a typical client timeout. Widen these if a failover is observed
  // to hold the database read-only for longer than that.
  private final val READ_ONLY_TXN_MAX_ATTEMPTS = 5
  private final val READ_ONLY_TXN_RETRY_BASE_WAIT_MS = 1000L
  private final val READ_ONLY_TXN_RETRY_MAX_WAIT_MS = 4000L

  def close(c: AutoCloseable): Unit = {
    if (c != null) {
      try {
        c.close()
      } catch {
        case NonFatal(t) => warn(s"Error on closing", t)
      }
    }
  }

  def withCloseable[R, C <: AutoCloseable](c: C)(block: C => R): R = {
    try {
      block(c)
    } finally {
      close(c)
    }
  }

  /**
   * Run `block` against a pooled connection, transparently recovering from a managed-database
   * failover. When the primary is swapped out (e.g. a weekly maintenance/failover on managed
   * PostgreSQL), pooled connections stay open but point at a node demoted to a read-only standby,
   * so every write fails with SQLSTATE 25006. We evict the pool -- so the next acquisition
   * reconnects through the endpoint and lands on the new primary -- then retry the block on a
   * fresh connection with bounded backoff. Retrying is safe specifically for this error:
   * PostgreSQL rejects the statement before it takes effect, so a 25006 failure guarantees nothing
   * was written and the block can be replayed without risking a partial or duplicated write. No
   * other failure mode gains retry semantics here.
   */
  def withConnection[R](block: Connection => R)(implicit ds: DataSource): R = {
    var attempt = 1
    while (true) {
      try {
        return withCloseable(ds.getConnection)(block)
      } catch {
        case t: Throwable if isReadOnlyTxnErr(t) =>
          softEvictConnections(ds)
          if (attempt >= READ_ONLY_TXN_MAX_ATTEMPTS) {
            error(s"The JDBC store is still read-only after $attempt attempts; giving up. The" +
              s" database failover may be taking longer than ${READ_ONLY_TXN_RETRY_MAX_WAIT_MS}ms" +
              s" to complete.")
            throw t
          }
          val waitMs = readOnlyTxnRetryWait(attempt)
          warn(s"Attempt $attempt of $READ_ONLY_TXN_MAX_ATTEMPTS failed against a read-only JDBC" +
            s" store; retrying on a fresh connection in ${waitMs}ms")
          Thread.sleep(waitMs)
          attempt += 1
      }
    }
    // unreachable: the loop either returns a result or rethrows
    throw new IllegalStateException("withConnection retry loop exited unexpectedly")
  }

  private def readOnlyTxnRetryWait(attempt: Int): Long =
    math.min(READ_ONLY_TXN_RETRY_BASE_WAIT_MS << (attempt - 1), READ_ONLY_TXN_RETRY_MAX_WAIT_MS)

  def execute(
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      implicit ds: DataSource): Boolean = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      pStmt.execute()
    }
  }

  def executeUpdate(
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      implicit ds: DataSource): Int = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      pStmt.executeUpdate()
    }
  }

  def executeQuery[R](
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      processResultSet: ResultSet => R)(
      implicit ds: DataSource): R = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      withCloseable(pStmt.executeQuery()) { rs =>
        processResultSet(rs)
      }
    }
  }

  def executeQueryWithRowMapper[R](
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      rowMapper: ResultSet => R)(
      implicit ds: DataSource): Seq[R] = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      withCloseable(pStmt.executeQuery()) { rs =>
        val builder = Seq.newBuilder[R]
        while (rs.next()) builder += rowMapper(rs)
        builder.result
      }
    }
  }

  def mapResultSet[R](rs: ResultSet)(rowMapper: ResultSet => R): Seq[R] = {
    val builder = Seq.newBuilder[R]
    while (rs.next()) builder += rowMapper(rs)
    builder.result
  }

  def redactPassword(password: Option[String]): String = {
    password match {
      case Some(s) if StringUtils.isNotBlank(s) => s"${"*" * s.length}(length:${s.length})"
      case _ => "(empty)"
    }
  }

  def isDuplicatedKeyDBErr(cause: Throwable): Boolean = {
    val duplicatedKeyKeywords = Seq(
      "Duplicate entry", // MySQL
      "duplicate key value violates unique constraint", // PostgreSQL
      "A UNIQUE constraint failed" // SQLite
    )
    duplicatedKeyKeywords.exists(cause.getMessage.contains)
  }

  /**
   * Whether the error indicates a write was attempted against a read-only database endpoint. This
   * happens when a load balancer fails traffic over to a standby/replica while the primary is down
   * (e.g. a maintenance/patching window). PostgreSQL raises SQLState 25006 (read_only_sql_
   * transaction); other engines surface it in the message. Such errors are transient and
   * recoverable once the primary is back, so callers should retry rather than fail hard.
   */
  def isReadOnlyTxnErr(cause: Throwable): Boolean = causeChain(cause).exists {
    case s: SQLException if "25006" == s.getSQLState => true
    case t =>
      Option(t.getMessage).map(_.toLowerCase(Locale.ROOT)).exists { m =>
        m.contains("read-only transaction") || m.contains("read only transaction")
      }
  }

  // Walk the exception's cause chain, bounded to guard against self-referential causes.
  private def causeChain(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(16)

  // Discard every connection currently in the Hikari pool so the next borrow reconnects. Best
  // effort: never throws, and is a no-op for non-Hikari datasources.
  private def softEvictConnections(ds: DataSource): Unit = ds match {
    case hikari: HikariDataSource =>
      try {
        warn("Detected a read-only-transaction error from the JDBC store; soft-evicting all " +
          "pooled connections so they reconnect to a writable primary.")
        Option(hikari.getHikariPoolMXBean).foreach(_.softEvictConnections())
      } catch {
        case NonFatal(t) => warn("Failed to soft-evict pooled connections", t)
      }
    case _ =>
  }
}
