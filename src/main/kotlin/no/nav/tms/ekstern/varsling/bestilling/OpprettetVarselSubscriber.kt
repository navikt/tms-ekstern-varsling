package no.nav.tms.ekstern.varsling.bestilling

import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.ZonedDateTime
import java.util.*


class OpprettetVarselSubscriber(private val repository: EksternVarselRepository) : Subscriber() {

    override fun subscribe() = Subscription.forEvent("opprettet")
        .withFields("type", "varselId", "ident", "eksternVarslingBestilling", "opprettet", "produsent")

    override suspend fun receive(jsonMessage: JsonMessage) {
        val produsent = Produsent(
            cluster = jsonMessage["produsent"]["cluster"].asText(),
            namespace = jsonMessage["produsent"]["namespace"].asText(),
            appnavn = jsonMessage["produsent"]["appnavn"].asText()
        )

        val varsel = Varsel(
            varseltype = jsonMessage["type"].asText().let(::parseVarseltype),
            varselId = jsonMessage["varselId"].asText(),
            prefererteKanaler = jsonMessage["eksternVarslingBestilling"]["prefererteKanaler"].map { Kanal.valueOf(it.asText()) },
            smsVarslingstekst = jsonMessage["eksternVarslingBestilling"]["smsVarslingstekst"].asText(),
            epostVarslingstittel = jsonMessage["eksternVarslingBestilling"]["epostVarslingstittel"].asText(),
            epostVarslingstekst = jsonMessage["eksternVarslingBestilling"]["epostVarslingstekst"].asText(),
            produsent = produsent
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
            opprettet = jsonMessage["opprettet"].asText().let(ZonedDateTime::parse)
        )
        repository.insertEksternVarsling(eksternVarsling)
    }

    fun parseVarseltype(type: String): Varseltype {
        return Varseltype.entries.find { it.name.lowercase() == type.lowercase() }
            ?: throw IllegalArgumentException("Fant ikke varsel type med type: $type")
    }
}
