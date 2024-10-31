package no.nav.tms.ekstern.varsling.status

import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

fun eksternVarslingStatus(
    eventId: String,
    status: DoknotifikasjonStatusEnum = DoknotifikasjonStatusEnum.INFO,
    bestillerAppnavn: String  = TmsEksternVarsling.appnavn,
    melding: String = "Melding",
    distribusjonsId: Long? = null,
    kanal: String? = null,
    tidspunkt: LocalDateTime = LocalDateTime.now(ZoneId.of("Z")),
    tidspunktZ: ZonedDateTime? = nowAtUtc()
) = """
{
    "@event_name": "eksternVarslingStatus",
    "eventId": "$eventId",
    "bestillerAppnavn": "$bestillerAppnavn",
    "status": "$status",
    "melding": "$melding",
    ${distribusjonsId.optionalJson("distribusjonsId")}
    ${kanal.optionalJson("kanal")}
    "tidspunkt": "$tidspunkt",
    "tidspunktZ": "$tidspunktZ"
}
"""

private fun Any?.optionalJson(name: String, isEnd: Boolean = false): String {
    val suffix = if(isEnd) "" else ","

    return when (this) {
        null -> ""
        is String -> "\"$name\": \"$this\"$suffix"
        is Number -> "\"$name\": $this$suffix"
        else -> throw IllegalArgumentException("Not supported")
    }
}
