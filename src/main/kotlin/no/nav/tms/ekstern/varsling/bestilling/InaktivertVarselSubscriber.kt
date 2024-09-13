package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.databind.JsonNode
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.ZonedDateTime
import java.util.*

class InaktivertVarselSubscriber (private val repository: EksternVarselRepository) : Subscriber() {

    override fun subscribe() = Subscription.forEvent("inaktivert")
        .withFields("varselId", "produsent")

    override suspend fun receive(jsonMessage: JsonMessage) {
        val varselId = jsonMessage["varselId"].asText()
        val eksternVarsling = repository.findSendingForVarsel(varselId)
        if (eksternVarsling != null) {
            val updatedVarsler = eksternVarsling.varsler
                .map { varsel: Varsel ->
                    if (varsel.varselId == varselId) {
                        varsel.copy(aktiv = false)
                    } else {
                        varsel
                    }
                }
            repository.updateVarsler(sendingsId = eksternVarsling.sendingsId, updatedVarsler)
        }
    }
}