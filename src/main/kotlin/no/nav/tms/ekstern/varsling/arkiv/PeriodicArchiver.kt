package no.nav.tms.ekstern.varsling.arkiv

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import java.time.Duration

class PeriodicArchiver(
    private val varselArchivingRepository: ArkivRepository,
    private val opprettetThresholdDays: Int,
    private val ferdigstiltThresholdDays: Int,
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
        val opprettetThreshold = nowAtUtc().minusDays(opprettetThresholdDays.toLong())
        val ferdigstiltThreshold = nowAtUtc().minusDays(ferdigstiltThresholdDays.toLong())

        try {
            varselArchivingRepository.archiveEntriesByThresholds(
                opprettetThreshold = opprettetThreshold,
                ferdigstiltThreshold = ferdigstiltThreshold,
                limit = batchSize
            ).forEach { _ -> EKSTERN_VARSEL_ARKIVERT.inc() }

        } catch (e: Exception) {
            log.error(e) { "Fikk feil mot databasen ved arkivering av beskjed. Forsøker igjen senere." }
        }
    }

    companion object {
        private val EKSTERN_VARSEL_ARKIVERT: Counter = Counter.builder()
            .name("tms_ekstern_varsling_v2_ekstern_varsling_arkivert")
            .help("Ekstern varsling status oppdatert")
            .register()
    }
}
