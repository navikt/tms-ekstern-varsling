package no.nav.tms.ekstern.varsling.setup

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import kotliquery.queryOf
import org.flywaydb.core.Flyway
import org.flywaydb.core.internal.info.MigrationInfoDumper
import org.testcontainers.containers.PostgreSQLContainer

class LocalPostgresDatabase private constructor() : Database {

    private val memDataSource: HikariDataSource
    private val container = PostgreSQLContainer("postgres:15")

    companion object {
        private val instance by lazy {
            LocalPostgresDatabase().also {
                it.migrate(expectedMigrations = 5)
            }
        }

        fun cleanDb(): LocalPostgresDatabase {
            instance.update { queryOf("delete from ekstern_varsling") }
            return instance
        }
    }

    init {
        container.start()
        memDataSource = createDataSource()
    }

    override val dataSource: HikariDataSource
        get() = memDataSource

    private fun createDataSource(): HikariDataSource {
        return HikariDataSource().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
            isAutoCommit = true
            validate()
        }
    }

    private fun migrate(expectedMigrations: Int) {
        Flyway.configure()
            .connectRetries(3)
            .validateMigrationNaming(true)
            .dataSource(dataSource)
            .load()
            .migrate()
            .let { assert(it.migrationsExecuted == expectedMigrations) }

    }
}
