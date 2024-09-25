package no.nav.tms.ekstern.varsling.bestilling

import java.time.ZonedDateTime
import java.util.UUID

fun String?.nullableTextToJson(): String {
    return if (this == null) {
        "null"
    } else {
        "\"$this\""
    }
}

fun createVarsel(
    varselId: String = UUID.randomUUID().toString(),
    varseltype: Varseltype = Varseltype.Beskjed,
    prefererteKanaler: List<Kanal> = listOf(Kanal.SMS),
    smsVarslingstekst: String? = "Dummy tekst",
    epostVarslingstittel: String? = "Dummy epost tittel",
    epostVarslingstekst: String? = "Dummy epost tekst",
    produsent: Produsent = Produsent("test-cluster", "test-namespace", "test-app"),
    aktiv: Boolean = true
) = Varsel(
    varselId = varselId,
    varseltype = varseltype,
    prefererteKanaler = prefererteKanaler,
    smsVarslingstekst = smsVarslingstekst,
    epostVarslingstittel = epostVarslingstittel,
    epostVarslingstekst = epostVarslingstekst,
    produsent = produsent,
    aktiv = aktiv
)


fun createEksternVarslingEvent(
    id: String,
    ident: String,
    type: String = "oppgave",
    prefererteKanaler: List<Kanal> = listOf(Kanal.SMS),
    smsVarslingstekst: String? = null,
    epostVarslingstittel: String? = null,
    epostVarslingstekst: String? = null,
    opprettet: ZonedDateTime = ZonedDateTimeHelper.nowAtUtc(),
    erBatch: Boolean? = null,
    utsettSendingTil: ZonedDateTime? = null,
) = """
    {
        "type": "$type",
        "varselId": "$id",
        "ident": "$ident",
        "sensitivitet": "high",
        "innhold": {
        "tekst": "Dummy tekst",
        "link": "https://nav.no",
        "tekster": [
        {
            "spraakkode": "nb",
            "tekst": "Dummy tekst",
            "default": true
        }
        ]
    },
        "produsent": {
        "cluster": "dev-gcp",
        "namespace": "dummy",
        "appnavn": "dummy-app"
    },
        "eksternVarslingBestilling": {
        "prefererteKanaler": [${
    prefererteKanaler.map { it.name }.joinToString(prefix = "\"", postfix = "\"", separator = "\",\"")
}],
        "smsVarslingstekst": ${smsVarslingstekst.nullableTextToJson()},
        "epostVarslingstittel": ${epostVarslingstittel.nullableTextToJson()},
        "epostVarslingstekst": ${epostVarslingstekst.nullableTextToJson()},
        "kanBatches": $erBatch,
        "utsettSendingTil": ${utsettSendingTil?.toString().nullableTextToJson()}
    },
        "opprettet": "$opprettet",
        "aktivFremTil": "2024-01-01T11:11:11.111Z",
        "@event_name": "opprettet",
        "tidspunkt": "2024-01-01T11:11:11.111Z"
    }
    """

fun opprettetEventUtenEksternVarsling(
    id: String,
    ident: String,
    type: String = "oppgave",
    opprettet: ZonedDateTime = ZonedDateTimeHelper.nowAtUtc(),
) = """
    {
        "type": "$type",
        "varselId": "$id",
        "ident": "$ident",
        "sensitivitet": "high",
        "innhold": {
        "tekst": "Dummy tekst",
        "link": "https://nav.no",
        "tekster": [
        {
            "spraakkode": "nb",
            "tekst": "Dummy tekst",
            "default": true
        }
        ]
    },
        "produsent": {
        "cluster": "dev-gcp",
        "namespace": "dummy",
        "appnavn": "dummy-app"
    },
        "eksternVarslingBestilling": null,
        "opprettet": "$opprettet",
        "aktivFremTil": "2024-01-01T11:11:11.111Z",
        "@event_name": "opprettet",
        "tidspunkt": "2024-01-01T11:11:11.111Z"
    }
    """

fun createInaktiverEvent(id: String) = """
    {
        "@event_name": "inaktivert",
        "varselId": "$id",
        "produsent": {
            "cluster": "dev-gcp",
            "namespace": "dummy",
            "appnavn": "dummy-app"
        }
    }
    """

fun createEksternVarslingDBRow(
    sendingsId: String,
    ident: String,
    erBatch: Boolean = false,
    erUtsattVarsel: Boolean = false,
    varsler: List<Varsel> = listOf(
        Varsel(
            varselId = "11111",
            varseltype = Varseltype.Oppgave,
            prefererteKanaler = listOf(Kanal.SMS),
            smsVarslingstekst = null,
            epostVarslingstittel = null,
            epostVarslingstekst = null,
            produsent = Produsent("test-clsuter", "test-namespace", "test-app"),
            aktiv = true
        ), Varsel(
            varselId = "22222",
            varseltype = Varseltype.Oppgave,
            prefererteKanaler = listOf(Kanal.SMS),
            smsVarslingstekst = null,
            epostVarslingstittel = null,
            epostVarslingstekst = null,
            produsent = Produsent("test-clsuter", "test-namespace", "test-app"),
            aktiv = true
        )
    ),
    utsending: ZonedDateTime? = null,
    ferdigstilt: ZonedDateTime? = null,
    opprettet: ZonedDateTime = ZonedDateTimeHelper.nowAtUtc().minusSeconds(30),
    status: Sendingsstatus = Sendingsstatus.Venter
) = EksternVarsling(
    sendingsId = sendingsId,
    ident = ident,
    erBatch = erBatch,
    erUtsattVarsel = erUtsattVarsel,
    varsler = varsler,
    utsending = utsending,
    kanal = null,
    ferdigstilt = ferdigstilt,
    opprettet = opprettet,
    status = status
)
