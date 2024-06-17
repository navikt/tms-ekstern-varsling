package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.annotation.JsonValue
import java.time.ZonedDateTime

enum class Kanal{
    EPOST, SMS
}

enum class Varseltype(val alias: String, val emailTitle: String, val smsText: String, emailTextFile: String) {
    Oppgave(
        alias = "oppgave",
        emailTitle = "Du har fått en oppgave fra NAV",
        smsText = "Hei! Du har fått en ny oppgave fra NAV. Logg inn på NAV for å se hva oppgaven gjelder. Vennlig hilsen NAV",
        emailTextFile = "epost_oppgave.txt",
    ),
    Beskjed(
        alias = "beskjed",
        emailTitle = "Beskjed fra NAV",
        smsText = "Hei! Du har fått en ny beskjed fra NAV. Logg inn på NAV for å se hva beskjeden gjelder. Vennlig hilsen NAV",
        emailTextFile = "epost_beskjed.txt",
    ),
    Innboks(
        alias = "beskjed",
        emailTitle = "Du har fått en melding fra NAV",
        smsText = "Hei! Du har fått en ny melding fra NAV. Logg inn på NAV for å lese meldingen. Vennlig hilsen NAV",
        emailTextFile = "epost_innboks.txt",
    );

    val emailText = this::class.java.getResource("/texts/$emailTextFile")!!.readText(Charsets.UTF_8)

    @JsonValue
    fun lowercaseName() = name.lowercase()
}

data class Produsent (
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
    val produsent: Produsent
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
