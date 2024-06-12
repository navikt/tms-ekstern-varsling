package no.nav.tms.ekstern.varsling.bestilling

import java.time.ZonedDateTime

enum class Kanal{
    EPOST, SMS
}
data class Varsel(
    val varselId: String,
    val varseltype: String,
    val prefererteKanaler: List<Kanal> = emptyList(),
    val smsVarslingstekst: String? = null,
    val epostVarslingstittel: String? = null,
    val epostVarslingstekst: String? = null,
)

data class EksternVarsling(
    val sendingsId: String,
    val ident: String,
    val erBatch: Boolean,
    val erUtsattVarsel: Boolean,
    val varsler: List<Varsel>,
    val utsending: ZonedDateTime?,
    val kanal: Kanal,
    val sendt: ZonedDateTime?,
    val opprettet: ZonedDateTime,
)