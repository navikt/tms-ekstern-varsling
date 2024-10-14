package no.nav.tms.ekstern.varsling.status

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.ekstern.varsling.bestilling.EksternVarslingRepository
import no.nav.tms.ekstern.varsling.bestilling.Varsel
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription

class BehandletAvLegacySubscriber(private val repository: EksternVarslingRepository) : Subscriber() {

    private val log = KotlinLogging.logger {}

    override fun subscribe() = Subscription.forEvent("eksternVarslingStatus")
        .withoutValue("bestillerAppnavn", TmsEksternVarsling.appnavn)
        .withAnyValue("status", "OVERSENDT", "FEILET")
        .withFields("eventId")

    override suspend fun receive(jsonMessage: JsonMessage) = traceVarsel(jsonMessage["eventId"].asText(), mapOf("action" to "duplikatBehandling")) {
        val varselId = jsonMessage["eventId"].asText()

        val eksternVarsling = repository.findSendingForVarsel(varselId)
        if (eksternVarsling != null) {

            log.info { "Markerer varsel som allerede behandlet av tms-ekstern-varselbestiller" }

            eksternVarsling.varsler.map { varsel: Varsel ->
                if (varsel.varselId == varselId) {
                    varsel.copy(behandletAvLegacy = true)
                } else {
                    varsel
                }
            }.let { varsler ->
                repository.updateVarsler(sendingsId = eksternVarsling.sendingsId, varsler)
            }
        }
    }
}
