package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.Query
import kotliquery.TransactionalSession
import kotliquery.action.NullableResultQueryAction
import kotliquery.sessionOf
import no.nav.tms.common.postgres.PostgresDatabase

fun <T> PostgresDatabase.transaction(actions: TransactionalSession.() -> T): T {
    val session = sessionOf(dataSource)

    val result: T = session.transaction {
        it.actions()
    }

    session.connection.close()

    return result
}

fun TransactionalSession.updateInTx(queryBuilder: () -> Query) = run(queryBuilder.invoke().asUpdate)

fun <T> TransactionalSession.singleOrNullInTx(queryBuilder: () -> NullableResultQueryAction<T>) = run(queryBuilder.invoke())

fun <T> TransactionalSession.singleInTx(queryBuilder: () -> NullableResultQueryAction<T>) = singleOrNullInTx(queryBuilder)
    ?: throw IllegalStateException("Found no rows matching query")

