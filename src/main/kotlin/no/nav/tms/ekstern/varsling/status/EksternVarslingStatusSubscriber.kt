package no.nav.tms.ekstern.varsling.status

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.common.observability.withTraceLogging
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.LocalDateTime
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

        eksternStatusUpdater.updateEksternVarslingStatus(eksternVarslingStatus)
        log.info { "Behandlet eksternVarslingStatus" }
    }

    private fun getTidspunkt(jsonMessage: JsonMessage): ZonedDateTime {
        return jsonMessage.getOrNull("tidspunktZ")?.let {
            ZonedDateTime.parse(it.asText())
        } ?: jsonMessage["tidspunkt"].asText().let {
            ZonedDateTime.parse("${it}Z")
        }
    }
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
