package no.nav.tms.ekstern.varsling.bestilling

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStopp
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

class InaktivertVarselSubscriber (
    private val repository: EksternVarslingRepository,
    private val kafkaProducer: Producer<String, DoknotifikasjonStopp>,
    private val doknotStoppTopic: String
) : Subscriber() {

    private val log = KotlinLogging.logger {}

    override fun subscribe() = Subscription.forEvent("inaktivert")
        .withFields("varselId", "produsent")

    override suspend fun receive(jsonMessage: JsonMessage) = traceVarsel(id = jsonMessage["varselId"].asText(), mapOf("action" to "inaktiver")) {

        val varselId = jsonMessage["varselId"].asText()
        val eksternVarsling = repository.findSendingForVarsel(varselId, aktiv = true)

        if (eksternVarsling == null) {
            log.debug { "Fant ingen ekstern varsling tilknyttet varsel" }
            return@traceVarsel
        }

        log.info { "Markerer varsel som inaktivert" }

        val alleVarslerInaktivert = eksternVarsling.varsler
            .map { varsel ->
                if (varsel.varselId == varselId) {
                    varsel.copy(aktiv = false)
                } else {
                    varsel
                }
            }.let { varsler ->
                repository.updateVarsler(sendingsId = eksternVarsling.sendingsId, varsler)
                varsler.all { !it.aktiv }
            }

        if (eksternVarsling.status == Sendingsstatus.Sendt && eksternVarsling.bestilling?.revarsling != null && alleVarslerInaktivert) {

            log.info { "Sender doknotifikasjonstopp for sendingsId ${eksternVarsling.sendingsId}" }

            DoknotifikasjonStopp.newBuilder()
                .setBestillingsId(eksternVarsling.sendingsId)
                .setBestillerId(TmsEksternVarsling.appnavn)
                .build()
                .let { ProducerRecord(doknotStoppTopic, eksternVarsling.sendingsId, it) }
                .let { kafkaProducer.send(it) }
        }
    }
}
