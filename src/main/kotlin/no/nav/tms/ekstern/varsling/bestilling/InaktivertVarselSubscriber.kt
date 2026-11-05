package no.nav.tms.ekstern.varsling.bestilling

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.asZonedDateTime
import no.nav.tms.ekstern.varsling.recordqueue.DoknotStopQueueRepository
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.SkippableMessageException
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription

class InaktivertVarselSubscriber(
    private val repository: EksternVarslingBestillingRepository,
    private val recordQueueRepository: DoknotStopQueueRepository
) : Subscriber() {

    private val log = KotlinLogging.logger {}

    override fun subscribe() = Subscription.forEvent("inaktivert")
        .withFields("varselId", "produsent", "tidspunkt")

    override suspend fun receive(jsonMessage: JsonMessage) {

        val varselId = jsonMessage["varselId"].asText()
        val tidspunkt = jsonMessage["tidspunkt"].asZonedDateTime()

        val eksternVarsling = repository.findSendingForVarsel(varselId)
            ?: throw SendingNotFoundException()

        val varsel = eksternVarsling.varsler.first { it.varselId == varselId }

        if (!varsel.aktiv) {
            throw VarselInaktivtException()
        }

        log.info { "Markerer varsel som inaktivert" }

        repository.inaktiverVarsel(varselId, tidspunkt)

        if (eksternVarsling.status == Sendingsstatus.Sendt &&
            eksternVarsling.bestilling?.revarsling != null) {

            log.info { "Forbereder doknotifikasjonstopp for sendingsId ${eksternVarsling.sendingsId}" }

            recordQueueRepository.enqueueDoknotStopp(eksternVarsling.sendingsId)
        }
    }
}

private class SendingNotFoundException: SkippableMessageException("Fant ingen ekstern varsling tilknyttet inaktivert varsel")
private class VarselInaktivtException: SkippableMessageException("Varsel er allerede markert inaktivt.")
