package no.nav.tms.ekstern.varsling.status

import com.fasterxml.jackson.annotation.JsonProperty
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus
import no.nav.tms.ekstern.varsling.bestilling.Produsent
import no.nav.tms.ekstern.varsling.bestilling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

class EksternVarslingOppdatertProducer(private val kafkaProducer: Producer<String, String>,
                                       private val topicName: String
) {
    private val log = KotlinLogging.logger { }
    private val objectMapper = defaultObjectMapper()

    fun eksternStatusOppdatert(oppdatering: EksternStatusOppdatering) {

        val producerRecord = ProducerRecord(topicName, oppdatering.varselId, objectMapper.writeValueAsString(oppdatering))

        kafkaProducer.send(producerRecord)

        log.info { "eksternStatusOppdatert-event produsert til kafka" }
    }

    fun flushAndClose() {
        try {
            kafkaProducer.flush()
            kafkaProducer.close()
            log.info { "Produsent for ekstern status oppdatert eventer er flushet og lukket." }
        } catch (e: Exception) {
            log.warn { "Klarte ikke å flushe og lukke produsent for ekstern status oppdatert. Det kan være eventer som ikke ble produsert." }
        }
    }
}

data class EksternStatusOppdatering(
    val status: EksternStatus.Status,
    val varselId: String,
    val ident: String,
    val kanal: String?,
    val renotifikasjon: Boolean?,
    val batch: Boolean,
    val varseltype: Varseltype,
    val produsent: Produsent,
    val feilmelding: String?
) {
    @JsonProperty("@event_name") val eventName = "eksternVarslingStatusOppdatert"
    val tidspunkt = nowAtUtc()

}
