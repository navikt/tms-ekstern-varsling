package no.nav.tms.ekstern.varsling.recordqueue

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import io.prometheus.metrics.core.metrics.Gauge
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.logging.TeamLogs
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.kafka.application.AppHealth
import no.nav.tms.kafka.producer.ProducerSendUtils.batched
import no.nav.tms.kafka.producer.RetriableSendException
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

class PeriodicStatusOppdatertQueueProcessor(
    private val repository: StatusOppdatertQueueRepository,
    private val recordProducer: Producer<String, String>,
    private val leaderElection: PodLeaderElection,
    private val varseltopic: String,
    private val batchSize: Int = 1000,
    private val syncTimeoutSeconds: Long = 15,
    internal: Duration = Duration.ofSeconds(1)
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

        val nextInQueue = repository.peekStatusOppdatert(batchSize)
        if (nextInQueue.isEmpty()) {
            return
        }

        log.info { "Behandler neste ${nextInQueue.size} elementer i status-oppdatert queue" }

        try {
            recordProducer.batched(syncTimeoutSeconds) {
                nextInQueue.forEach { dto ->
                    val record = ProducerRecord(varseltopic, dto.varselId, dto.statusinnhold)

                    sendInBatch(record) {
                        repository.dequeueStatusOppdatert(dto.id)
                        reportEntryProcessed(dto.statusnavn)
                    }
                }
            }
        } catch (e: RetriableSendException) {
            log.warn { "Midlertidig feil ved sending av statusoppdatert-records fra outbox til kafka. Prøver på nytt senere" }
            teamLog.warn(e) { "Midlertidig feil ved sending av statusoppdatert-records fra outbox til kafka. Prøver på nytt senere" }
        } catch (e: Exception) {
            log.error { "Feil ved sending av statusoppdatert-records fra outbox til kafka. Avslutter prosessering." }
            teamLog.error(e) { "Feil ved sending av statusoppdatert-records fra outbox til kafka. Avslutter prosessering." }
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
            log.info { "Produsent for kafka-eventer er flushet og lukket." }
        } catch (e: Exception) {
            log.warn { "Klarte ikke å flushe og lukke produsent. Det kan være eventer som ikke ble produsert." }
        }
    }


    private fun reportQueueSize() {
        STATUS_OPPDATERT_QUEUE_SIZE.set(repository.statusOppdatertQueueSize().toDouble())
    }

    private fun reportEntryProcessed(status: String) {
        STATUS_OPPDATERT_QUEUE_PROCESSED.labelValues(status.lowercase()).inc()
    }

    companion object {
        private const val STATUS_OPPDATERT_QUEUE_SIZE_NAME = "tms_ekstern_varsling_v2_status_oppdatert_queue_size"
        private const val STATUS_OPPDATERT_QUEUE_PROCESSED_NAME = "tms_ekstern_varsling_v2_status_oppdatert_queue_processed"

        private val STATUS_OPPDATERT_QUEUE_PROCESSED: Counter = Counter.builder()
            .name(STATUS_OPPDATERT_QUEUE_PROCESSED_NAME)
            .help("Antall utgående status-oppdatert elementer prosessert fra kø")
            .labelNames("status")
            .register()

        private val STATUS_OPPDATERT_QUEUE_SIZE: Gauge = Gauge.builder()
            .name(STATUS_OPPDATERT_QUEUE_SIZE_NAME)
            .help("Antall utgående status-oppdatert elementer i kø")
            .register()
    }
}



