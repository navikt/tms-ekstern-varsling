package no.nav.tms.ekstern.varsling.setup

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration

object Flyway {

    fun runFlywayMigrations() {
        val flyway = configure().load()
        flyway.migrate()
    }

    private fun configure(): FluentConfiguration {
        val configBuilder = Flyway.configure()
            .validateMigrationNaming(true)
            .connectRetries(5)
        val dataSource = PostgresDatabase.hikariFromLocalDb()
        configBuilder.dataSource(dataSource)

        return configBuilder
    }

}
