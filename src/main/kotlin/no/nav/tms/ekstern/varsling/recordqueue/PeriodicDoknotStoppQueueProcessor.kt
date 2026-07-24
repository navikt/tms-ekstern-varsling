package no.nav.tms.ekstern.varsling.recordqueue

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStopp
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.logging.TeamLogs
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.kafka.application.AppHealth
import no.nav.tms.kafka.producer.ProducerSendUtils.batched
import no.nav.tms.kafka.producer.RetriableSendException
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

class PeriodicDoknotStoppQueueProcessor(
    private val repository: DoknotStopQueueRepository,
    private val recordProducer: Producer<String, DoknotifikasjonStopp>,
    private val leaderElection: PodLeaderElection,
    private val doknotStoppTopic: String,
    private val batchSize: Int = 1000,
    private val syncTimeoutSeconds: Long = 15,
    internal: Duration = Duration.ofSeconds(5)
): PeriodicJob(internal) {

    private val log = KotlinLogging.logger { }
    private val teamLog = TeamLogs.logger { }

    override val job = initializeJob {
        if (leaderElection.isLeader()) {
            processQueue()
        }
    }

    private fun processQueue() {
        reportQueueSize()

        val nextInQueue = repository.peekNextDoknotStop(batchSize)
        if (nextInQueue.isEmpty()) {
            return
        }

        log.info { "Behandler neste ${nextInQueue.size} elementer i doknotstop record-queue" }

        try {
            recordProducer.batched(syncTimeoutSeconds) {
                nextInQueue.forEach { dto ->
                    val record = DoknotifikasjonStopp.newBuilder()
                        .setBestillingsId(dto.sendingsId)
                        .setBestillerId(TmsEksternVarsling.appnavn)
                        .build()
                        .let {
                            ProducerRecord(doknotStoppTopic, dto.sendingsId, it)
                        }

                    sendInBatch(record) {
                        repository.dequeueDoknotStopp(dto.id)
                        reportEntryProcessed()
                    }
                }
            }
        } catch (e: RetriableSendException) {
            log.warn { "Midlertidig feil ved sending av doknot-stop records fra outbox til kafka. Prøver på nytt senere" }
            teamLog.warn(e) { "Midlertidig feil ved sending av doknot-stop records fra outbox til kafka. Prøver på nytt senere" }
        } catch (e: Exception) {
            log.error { "Feil ved sending av doknot-stop records fra outbox til kafka. Avslutter prosessering." }
            teamLog.error(e) { "Feil ved sending av doknot-stop records fra outbox til kafka. Avslutter prosessering." }
        }
    }

    fun isHealthy() = if (job.isActive) {
        AppHealth.Healthy
    } else {
        AppHealth.Unhealthy
    }

    fun flushAndClose() {
        try {
            recordProducer.flush()
            recordProducer.close()
            log.info { "Produsent for doknot-stop records er flushet og lukket." }
        } catch (e: Exception) {
            log.warn { "Klarte ikke å flushe og lukke produsent. Det kan være eventer som ikke ble produsert." }
        }
    }

    private fun reportQueueSize() {
        DOKNOT_STOPP_QUEUE_SIZE.set(repository.doknotStopQueueSize().toDouble())
    }

    private fun reportEntryProcessed() {
        DOKNOT_STOPP_QUEUE_PROCESSED.inc()
    }

    companion object {
        private const val DOKNOT_STOPP_QUEUE_SIZE_NAME = "tms_ekstern_varsling_v2_doknot_stopp_queue_size"
        private const val DOKNOT_STOPP_QUEUE_PROCESSED_NAME = "tms_ekstern_varsling_v2_doknot_stopp_queue_processed"

        private val DOKNOT_STOPP_QUEUE_PROCESSED: Counter = Counter.builder()
            .name(DOKNOT_STOPP_QUEUE_PROCESSED_NAME)
            .help("Antall utgående doknotstopp-elementer prosessert fra kø")
            .register()

        private val DOKNOT_STOPP_QUEUE_SIZE: Gauge = Gauge.builder()
            .name(DOKNOT_STOPP_QUEUE_SIZE_NAME)
            .help("Totalt antall utgående doknotstopp-elementer i kø")
            .register()
    }
}
