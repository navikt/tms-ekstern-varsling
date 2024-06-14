package no.nav.tms.ekstern.varsling.setup

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.tms.common.util.config.StringEnvVar

class PostgresDatabase : Database {


    private val envDataSource: HikariDataSource

    init {
        envDataSource = createConnectionForLocalDbWithDbUser()
    }

    override val dataSource: HikariDataSource
        get() = envDataSource

    private fun createConnectionForLocalDbWithDbUser(): HikariDataSource {
        return hikariFromLocalDb()
    }

    companion object {

        private val dbUser: String = StringEnvVar.getEnvVar("DB_USERNAME")
        private val dbPassword: String = StringEnvVar.getEnvVar("DB_PASSWORD")
        private val dbUrl: String = getDbUrl(
            StringEnvVar.getEnvVar("DB_HOST"),
            StringEnvVar.getEnvVar("DB_PORT"),
            StringEnvVar.getEnvVar("DB_DATABASE")
        )

        fun getDbUrl(host: String, port: String, name: String): String {
            return if (host.endsWith(":$port")) {
                "jdbc:postgresql://${host}/$name"
            } else {
                "jdbc:postgresql://${host}:${port}/${name}"
            }
        }

        fun hikariFromLocalDb(): HikariDataSource {
            val config = hikariCommonConfig()
            config.validate()
            return HikariDataSource(config)
        }

        private fun hikariCommonConfig(): HikariConfig {
            val config = HikariConfig().apply {
                driverClassName = "org.postgresql.Driver"
                jdbcUrl = dbUrl
                minimumIdle = 1
                maxLifetime = 1800000
                maximumPoolSize = 5
                connectionTimeout = 4000
                validationTimeout = 1000
                idleTimeout = 30000
                isAutoCommit = true
                transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                username = dbUser
                password = dbPassword
            }
            return config
        }
    }
}
