package no.nav.tms.ekstern.varsling.setup

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.internal.info.MigrationInfoDumper

object Flyway {

    private val log = KotlinLogging.logger {}

    fun runFlywayMigrations() {
        log.info { "Starter flyway-migrering" }
        configure().load()
            .also {
                MigrationInfoDumper.dumpToAsciiTable(it.info().all())
            }.migrate()
        log.info { "Flyway migrering ferdig" }
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
