package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.annotation.JsonValue
import java.time.ZonedDateTime

enum class Kanal {
    EPOST, SMS, BETINGET_SMS
}

enum class Varseltype(val alias: String) {
    Oppgave(alias = "oppgave"),
    Beskjed(alias = "beskjed"),
    Innboks(alias = "beskjed");

    @JsonValue
    fun lowercaseName() = name.lowercase()
}

enum class Sendingsstatus {
    Sendt,
    Venter,
    Kansellert,
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
    val behandletAvLegacy: Boolean
)

data class EksternVarsling(
    val sendingsId: String,
    val ident: String,
    val erBatch: Boolean,
    val erUtsattVarsel: Boolean,
    val varsler: List<Varsel>,
    val utsending: ZonedDateTime?,
    val ferdigstilt: ZonedDateTime?,
    val status: Sendingsstatus,
    val eksternStatus: EksternStatus.Oversikt?,
    val bestilling: Bestilling?,
    val opprettet: ZonedDateTime,
)

data class Bestilling(
    val preferertKanal: Kanal,
    val tekster: Tekster?,
    val revarsling: Revarsling?
)

data class Revarsling(
    val intervall: Int,
    val antall: Int
)

object EksternStatus {
    data class Oversikt(
        val sendt: Boolean,
        val renotifikasjonSendt: Boolean,
        val kanal: String?,
        val historikk: List<HistorikkEntry>,
        val sistOppdatert: ZonedDateTime
    )

    data class HistorikkEntry(
        val melding: String,
        val status: Status,
        val distribusjonsId: Long?,
        val kanal: String?,
        val renotifikasjon: Boolean?,
        val tidspunkt: ZonedDateTime
    )

    enum class Status {
        Feilet, Info, Bestilt, Sendt, Ferdigstilt, Kansellert, Venter;

        @JsonValue
        fun toJson() = name.lowercase()
    }
}
