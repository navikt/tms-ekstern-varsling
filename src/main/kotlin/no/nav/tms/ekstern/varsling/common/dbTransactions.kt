package no.nav.tms.ekstern.varsling.common

import kotliquery.Query
import kotliquery.TransactionalSession
import kotliquery.sessionOf
import no.nav.tms.common.postgres.BatchUpdateException
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.common.postgres.QueryException
import no.nav.tms.common.postgres.UniqueConstraintException
import org.postgresql.util.PSQLState
import java.sql.SQLException

fun <T> PostgresDatabase.transaction(actions: TransactionalSession.() -> T): T {
    val session = sessionOf(dataSource)

    val result: T = session.transaction {
        it.actions()
    }

    session.connection.close()

    return result
}

fun TransactionalSession.updateInTx(queryBuilder: () -> Query): Int {
    return try {
        queryBuilder()
            .asUpdate
            .let(::run)
    } catch (e: Exception) {
        if (e is SQLException && e.sqlState == PSQLState.UNIQUE_VIOLATION.state) {
            throw UniqueConstraintException(e)
        } else {
            throw QueryException("Error during 'update' query action", e)
        }
    }
}

fun TransactionalSession.batchUpdateInTx(statement: String, params: List<Map<String, Any?>>) {
    try {
        batchPreparedNamedStatement(statement, params)
    } catch (e: Exception) {
        throw BatchUpdateException(e)
    }
}