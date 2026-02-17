package no.nav.tms.ekstern.varsling.status

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater.FailureReason.DuplicateStatus
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater.FailureReason.UnknownEksternVarsling
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater.FailureReason.HistorikkSaturated
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.MessageException
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.ZonedDateTime

class EksternVarslingStatusSubscriber(
    private val eksternStatusUpdater: EksternStatusUpdater
) : Subscriber() {

    private val log = KotlinLogging.logger { }

    override fun subscribe(): Subscription = Subscription
        .forEvent("eksternVarslingStatus")
        .withValue("bestillerAppnavn", TmsEksternVarsling.appnavn)
        .withFields(
            "eventId",
            "status",
            "melding",
            "tidspunkt",
            "tidspunktZ"
        )
        .withOptionalFields("distribusjonsId", "kanal")


    override suspend fun receive(jsonMessage: JsonMessage) = traceVarsel(jsonMessage["eventId"].asText(), mapOf("action" to "eksternStatus")) {

        val eksternVarslingStatus = DoknotifikasjonStatusEvent(
            eventId = jsonMessage["eventId"].asText(),
            bestillerAppnavn = jsonMessage["bestillerAppnavn"].asText(),
            status = jsonMessage["status"].asText(),
            melding = jsonMessage["melding"].asText(),
            distribusjonsId = jsonMessage.getOrNull("distribusjonsId")?.asLong(),
            kanal = jsonMessage.getOrNull("kanal")?.asText(),
            tidspunkt = getTidspunkt(jsonMessage)
        )

        try {
            eksternStatusUpdater.updateEksternVarslingStatus(eksternVarslingStatus)
            log.info { "Behandlet eksternVarslingStatus" }

        } catch (e: EksternStatusUpdater.StatusUpdateException) {
            when (e.failureReason) {
                UnknownEksternVarsling -> {
                    log.warn { "Ignorer status [${eksternVarslingStatus.status}] fordi det ikke tilhørte en kjent ekstern varsling." }
                    throw UnknownSendingsIdException()
                }
                DuplicateStatus -> {
                    log.warn { "Ignorer status [${eksternVarslingStatus.status}] fordi det var duplikat." }
                    throw DuplicateStatusException()
                }
                HistorikkSaturated -> {
                    log.warn { "Ignorer status [${eksternVarslingStatus.status}] fordi det ikke var viktig nok å behandle etter historikken allerede var full." }
                    throw HistorikkSaturatedException()
                }
            }
        }
    }

    private fun getTidspunkt(jsonMessage: JsonMessage): ZonedDateTime {
        return jsonMessage.getOrNull("tidspunktZ")?.let {
            ZonedDateTime.parse(it.asText())
        } ?: jsonMessage["tidspunkt"].asText().let {
            ZonedDateTime.parse("${it}Z")
        }
    }

    class UnknownSendingsIdException(): MessageException("Fant ikke fant ekstern varsling tilhørende status")
    class DuplicateStatusException(): MessageException("Statusoppdatering var duplikat")
    class HistorikkSaturatedException(): MessageException("Statusoppdatering ignorert fordi historikken var full")
}

data class DoknotifikasjonStatusEvent(
    val eventId: String,
    val bestillerAppnavn: String,
    val status: String,
    val melding: String,
    val distribusjonsId: Long?,
    val kanal: String?,
    val tidspunkt: ZonedDateTime
)

enum class DoknotifikasjonStatusEnum {
    FEILET, INFO, OVERSENDT, FERDIGSTILT
}
