package no.nav.tms.ekstern.varsling.bestilling

import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.ZonedDateTime
import java.util.*


class OpprettetVarselSubscriber(private val repository: EksternVarselRepository) : Subscriber() {
    override suspend fun receive(jsonMessage: JsonMessage) {
        val varsel = Varsel(
            varseltype = jsonMessage["type"].asText(),
            varselId = jsonMessage["varselId"].asText(),
            prefererteKanaler = jsonMessage["eksternVarslingBestilling"]["prefererteKanaler"].map { Kanal.valueOf(it.asText()) },
            smsVarslingstekst = jsonMessage["eksternVarslingBestilling"]["smsVarslingstekst"].asText(),
            epostVarslingstittel = jsonMessage["eksternVarslingBestilling"]["epostVarslingstittel"].asText(),
            epostVarslingstekst = jsonMessage["eksternVarslingBestilling"]["epostVarslingstekst"].asText(),
        )

        val eksternVarsling = EksternVarsling(
            sendingsId = UUID.randomUUID().toString(),
            ident = jsonMessage["ident"].asText(),
            erBatch = false,
            erUtsattVarsel = false,
            varsler = listOf(varsel),
            utsending = null,
            kanal = varsel.prefererteKanaler.find { it == Kanal.SMS } ?: Kanal.EPOST,
            sendt = null,
            opprettet = jsonMessage["opprettet"].asText().let { ZonedDateTime.parse(it) }
        )
        repository.insertEksternVarsling(eksternVarsling)
    }

    override fun subscribe() = Subscription.forEvent("opprettet")
        .withFields("type", "varselId", "ident", "eksternVarslingBestilling", "opprettet")
}