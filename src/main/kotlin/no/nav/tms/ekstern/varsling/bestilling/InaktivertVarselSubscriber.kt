package no.nav.tms.ekstern.varsling.bestilling

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.ekstern.varsling.recordqueue.DoknotStopQueueRepository
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription

class InaktivertVarselSubscriber(
    private val repository: EksternVarslingRepository,
    private val recordQueueRepository: DoknotStopQueueRepository
) : Subscriber() {

    private val log = KotlinLogging.logger {}

    override fun subscribe() = Subscription.forEvent("inaktivert")
        .withFields("varselId", "produsent")

    override suspend fun receive(jsonMessage: JsonMessage) {

        val varselId = jsonMessage["varselId"].asText()
        val eksternVarsling = repository.findSendingForVarsel(varselId, aktiv = true)

        if (eksternVarsling == null) {
            log.debug { "Fant ingen ekstern varsling tilknyttet varsel" }
            return
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

            log.info { "Forbereder doknotifikasjonstopp for sendingsId ${eksternVarsling.sendingsId}" }

            recordQueueRepository.enqueueDoknotStopp(eksternVarsling.sendingsId)
        }
    }
}
