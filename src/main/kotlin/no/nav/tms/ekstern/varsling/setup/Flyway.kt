package no.nav.tms.ekstern.varsling.setup

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.internal.info.MigrationInfoDumper

object Flyway {

    private val log = KotlinLogging.logger {}

    fun runFlywayMigrations() = try {

        log.info { "Starter flyway-migrering" }

        Flyway.configure()
            .validateMigrationNaming(true)
            .connectRetries(5)
            .dataSource(PostgresDatabase.hikariFromLocalDb())
            .load()
            .migrate()

        log.info { "Flyway migrering ferdig" }
    } catch (e: Exception) {
        log.warn(e) { "Feil ved flyway-migrering" }
    }
}
