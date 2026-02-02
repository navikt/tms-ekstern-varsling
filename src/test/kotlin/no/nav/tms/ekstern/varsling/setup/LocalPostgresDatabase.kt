package no.nav.tms.ekstern.varsling.setup

import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import no.nav.tms.common.postgres.Postgres
import no.nav.tms.common.postgres.PostgresDatabase
import org.flywaydb.core.Flyway
import org.testcontainers.postgresql.PostgreSQLContainer

object LocalPostgresDatabase {

    private val container = PostgreSQLContainer("postgres:15")
        .apply { start() }

    private val database: PostgresDatabase by lazy {
        Postgres.connectToContainer(container).also {
            migrate(it.dataSource, expectedMigrations = 6)
        }
    }

    fun getInstance(): PostgresDatabase {
        return database
    }

    fun resetInstance() {
        database.update { queryOf("delete from ekstern_varsling") }
    }

    private fun migrate(dataSource: HikariDataSource, expectedMigrations: Int) {
        Flyway.configure()
            .connectRetries(3)
            .dataSource(dataSource)
            .load()
            .migrate()
            .let { assert(it.migrationsExecuted == expectedMigrations) }
    }
}
