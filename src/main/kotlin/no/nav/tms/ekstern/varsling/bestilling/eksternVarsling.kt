package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.annotation.JsonValue
import java.time.ZonedDateTime

enum class Kanal {
    EPOST, SMS
}

enum class Varseltype(val alias: String) {
    Oppgave(alias = "oppgave"),
    Beskjed(alias = "beskjed"),
    Innboks(alias = "beskjed");

    @JsonValue
    fun lowercaseName() = name.lowercase()
}

enum class Sendingsstatus() {
    Sendt,
    Venter,
    Kanselert,
}

data class Produsent(
    val cluster: String,
    val namespace: String,
    val appnavn: String
)

data class Varsel(
    val varselId: String,
    val varseltype: Varseltype,
    val prefererteKanaler: List<Kanal> = emptyList(),
    val smsVarslingstekst: String? = null,
    val epostVarslingstittel: String? = null,
    val epostVarslingstekst: String? = null,
    val produsent: Produsent,
    val aktiv: Boolean,
)

data class EksternVarsling(
    val sendingsId: String,
    val ident: String,
    val erBatch: Boolean,
    val erUtsattVarsel: Boolean,
    val varsler: List<Varsel>,
    val utsending: ZonedDateTime?,
    val kanal: Kanal?,
    val ferdigstilt: ZonedDateTime?,
    val status: Sendingsstatus,
    val opprettet: ZonedDateTime,
)
