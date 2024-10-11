package no.nav.tms.ekstern.varsling.setup

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration

object Flyway {

    private val log = KotlinLogging.logger {}

    fun runFlywayMigrations() {
        log.info { "Starter flyway-migrering" }
        val flyway = configure().load()
        flyway.migrate()
        log.info { "Flyway migrering ferdig" }
    }

    private fun configure(): FluentConfiguration {
        val configBuilder = Flyway.configure()
            .connectRetries(5)
        val dataSource = PostgresDatabase.hikariFromLocalDb()
        configBuilder.dataSource(dataSource)

        return configBuilder
    }

}
