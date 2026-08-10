package no.nav.tms.ekstern.varsling.arkiv

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.kafka.application.AppHealth
import java.time.Duration

class PeriodicArchiver(
    private val arkivRepository: ArkivRepository,
    private val ageThresholdDaysOpprettet: Long,
    private val ageThresholdDaysFerdigstilt: Long,
    private val leaderElection: PodLeaderElection,
    private val batchSize: Int = 10_000,
    interval: Duration = Duration.ofSeconds(10)
): PeriodicJob(interval) {

    private val log = KotlinLogging.logger {}

    override val job = initializeJob {
        if (leaderElection.isLeader()) {
            archiveOldVarsler()
        }
    }

    private fun archiveOldVarsler() {
        val opprettetThreshold = nowAtUtc().minusDays(ageThresholdDaysOpprettet)
        val ferdigstiltThreshold = nowAtUtc().minusDays(ageThresholdDaysFerdigstilt)

        try {
            arkivRepository.archiveEntriesByThresholds(
                opprettetThreshold = opprettetThreshold,
                ferdigstiltThreshold = ferdigstiltThreshold,
                limit = batchSize
            ).forEach {
                EKSTERN_VARSEL_ARKIVERT.labelValues(it.begrunnelse.name.lowercase()).inc()
            }

        } catch (e: Exception) {
            log.error(e) { "Fikk feil mot databasen ved arkivering av beskjed. Forsøker igjen senere." }
        }
    }

    fun isHealthy() = if (job.isActive) {
        AppHealth.Healthy
    } else {
        AppHealth.Unhealthy
    }

    companion object {
        private val EKSTERN_VARSEL_ARKIVERT: Counter = Counter.builder()
            .name("tms_ekstern_varsling_v2_ekstern_varsling_arkivert")
            .help("Ekstern varsling status oppdatert")
            .labelNames("begrunnelse")
            .register()
    }
}
