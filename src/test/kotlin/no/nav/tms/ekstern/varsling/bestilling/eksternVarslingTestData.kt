package no.nav.tms.ekstern.varsling.bestilling

import java.time.ZonedDateTime

fun String?.nullableTextToJson(): String {
    return if (this == null) {
        "null"
    } else {
        "\"$this\""
    }
}

fun createEksternVarslingEvent(
    id: String,
    ident: String,
    type: String = "oppgave",
    prefererteKanaler: List<Kanal> = listOf(Kanal.SMS),
    smsVarslingstekst: String? = null,
    epostVarslingstittel: String? = null,
    epostVarslingstekst: String? = null,
    opprettet: ZonedDateTime = ZonedDateTime.now(),
    erBatch: Boolean? = null,
    erUtsattVarsel: Boolean? = null,
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
        "erBatch": $erBatch,
        "erUtsattVarsel": $erUtsattVarsel
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
    opprettet: ZonedDateTime = ZonedDateTime.now(),
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


fun createEksternVarslingDBRow(
    sendingsId: String,
    ident: String,
    erBatch: Boolean = false,
    erUtsattVarsel: Boolean = false,
    varsler: List<Varsel> = listOf(
        Varsel(
            varselId = "11111",
            varseltype = "oppgave",
            prefererteKanaler = listOf(Kanal.SMS),
            smsVarslingstekst = null,
            epostVarslingstittel = null,
            epostVarslingstekst = null
        ), Varsel(
            varselId = "22222",
            varseltype = "oppgave",
            prefererteKanaler = listOf(Kanal.SMS),
            smsVarslingstekst = null,
            epostVarslingstittel = null,
            epostVarslingstekst = null
        )
    ),
    utsending: ZonedDateTime? = null,
    kanal: Kanal = Kanal.SMS,
    sendt: ZonedDateTime? = null,
    opprettet: ZonedDateTime = ZonedDateTime.now().minusSeconds(30),
) = EksternVarsling(
    sendingsId = sendingsId,
    ident = ident,
    erBatch = erBatch,
    erUtsattVarsel = erUtsattVarsel,
    varsler = varsler,
    utsending = utsending,
    kanal = kanal,
    sendt = sendt,
    opprettet = opprettet
)
