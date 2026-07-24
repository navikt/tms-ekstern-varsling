package no.nav.tms.ekstern.varsling.status

import com.fasterxml.jackson.annotation.JsonProperty
import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus
import no.nav.tms.ekstern.varsling.bestilling.Produsent
import no.nav.tms.ekstern.varsling.bestilling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.defaultObjectMapper
import no.nav.tms.ekstern.varsling.recordqueue.StatusOppdatertQueueRepository

class EksternVarslingOppdatertProducer(
    private val queueRepository: StatusOppdatertQueueRepository,
) {
    private val log = KotlinLogging.logger { }
    private val objectMapper = defaultObjectMapper()

    fun eksternStatusOppdatert(oppdatering: EksternStatusOppdatering) {

        val statusInnhold = objectMapper.writeValueAsString(oppdatering)

        queueRepository.enqueueStatusOppdatert(oppdatering.varselId, oppdatering.status.name, statusInnhold)

        EKSTERN_STATUS_OPPDATERT.labelValues(
            oppdatering.varseltype.name.lowercase(),
            oppdatering.status.name.lowercase(),
            oppdatering.kanal ?: "",
            oppdatering.renotifikasjon?.toString() ?: "",
            oppdatering.batch.toString(),
        ).inc()

        log.info { "eksternStatusOppdatert-record lagt i outbox" }
    }
}

data class EksternStatusOppdatering(
    val status: EksternStatus.Status,
    val varselId: String,
    val ident: String,
    val kanal: String?,
    val renotifikasjon: Boolean?,
    val batch: Boolean?,
    val varseltype: Varseltype,
    val produsent: Produsent,
    val melding: String?,
    val feilmelding: String?
) {
    @JsonProperty("@event_name") val eventName = "eksternVarslingStatusOppdatert"
    val tidspunkt = nowAtUtc()
}

private val EKSTERN_STATUS_OPPDATERT: Counter = Counter.builder()
    .name("tms_ekstern_varsling_v2_ekstern_varsling_status_oppdatert")
    .help("Ekstern varsling status oppdatert")
    .labelNames("varseltype", "status", "kanal", "renotifikasjon", "batch")
    .register()
