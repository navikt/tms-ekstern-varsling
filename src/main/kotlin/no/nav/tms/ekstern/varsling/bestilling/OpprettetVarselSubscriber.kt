package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.databind.JsonNode
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

        if (isDuplicate(varsel)) {
            return
        }

        findExistingBatch(jsonMessage)
            ?.let { addToExistingBatch(varsel, it) }
            ?: createNewEksternVarsling(varsel, jsonMessage)
    }

    fun isDuplicate(varsel: Varsel): Boolean {
        return repository.varselExists(varsel.varselId)
    }

    fun addToExistingBatch(varsel: Varsel, existingBatch: EksternVarsling) {
        repository.addVarselToExisting(
            sendingsId = existingBatch.sendingsId,
            varsel = varsel,
            kanal = varsel.prefererteKanaler.find { it == Kanal.SMS } ?: existingBatch.kanal)
    }

    fun createNewEksternVarsling(varsel: Varsel, jsonMessage: JsonMessage) {
        val erBatch = jsonMessage["eksternVarslingBestilling"]["erBatch"].asBooleanOrNull() ?: false
        val utsettSendingTil = jsonMessage["eksternVarslingBestilling"]["utsettSendingTil"].asTextOrNull()?.let { ZonedDateTime.parse(it) }

        val utsending = if (utsettSendingTil != null) {
            utsettSendingTil
        } else if (erBatch){
            ZonedDateTimeHelper.nowAtUtc().plusHours(1)
        } else {
            null
        }

        val eksternVarsling = EksternVarsling(
            sendingsId = UUID.randomUUID().toString(),
            ident = jsonMessage["ident"].asText(),
            erBatch = erBatch,
            erUtsattVarsel = utsettSendingTil != null,
            varsler = listOf(varsel),
            utsending = utsending,
            kanal = varsel.prefererteKanaler.find { it == Kanal.SMS } ?: Kanal.EPOST,
            sendt = null,
            opprettet = jsonMessage["opprettet"].asText().let(ZonedDateTime::parse)
        )

        repository.insertEksternVarsling(eksternVarsling)
    }

    fun findExistingBatch(jsonMessage: JsonMessage): EksternVarsling? {
        val erBatch = jsonMessage["eksternVarslingBestilling"]["erBatch"]?.asBoolean() ?: false

        return if (erBatch) {
            repository.findExistingBatch(jsonMessage["ident"].asText())
        } else {
            null
        }
    }

    fun parseVarseltype(type: String): Varseltype {
        return Varseltype.entries.find { it.name.lowercase() == type.lowercase() }
            ?: throw IllegalArgumentException("Fant ikke varsel type med type: $type")
    }

    fun JsonNode?.asTextOrNull(): String? {
        return if (this == null || isNull) {
            null
        } else {
            asText()
        }
    }

    fun JsonNode?.asBooleanOrNull(): Boolean? {
        return if (this == null || isNull) {
            null
        } else {
            asBoolean()
        }
    }
}
