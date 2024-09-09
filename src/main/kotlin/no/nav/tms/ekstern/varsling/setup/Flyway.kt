package no.nav.tms.ekstern.varsling.setup

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration

object Flyway {

    val log = KotlinLogging.logger {}

    fun runFlywayMigrations() {
        val flyway = configure().load()
        flyway.migrate()

        log.info { "Flyway complete " }
    }

    private fun configure(): FluentConfiguration {
        val configBuilder = Flyway.configure()
            .validateMigrationNaming(true)
            .connectRetries(5)
        val dataSource = PostgresDatabase.hikariFromLocalDb()
        configBuilder.dataSource(dataSource)

        log.info { "Prepared flyway" }

        return configBuilder
    }

}
