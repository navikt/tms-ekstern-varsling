package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.databind.JsonNode
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.common.logging.TeamLogs
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.ekstern.varsling.status.EksternStatusOppdatering
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.kafka.application.JsonMessage
import no.nav.tms.kafka.application.MessageException
import no.nav.tms.kafka.application.Subscriber
import no.nav.tms.kafka.application.Subscription
import java.time.ZonedDateTime
import java.util.*


class OpprettetVarselSubscriber(
    private val repository: EksternVarslingRepository,
    private val statusProducer: EksternVarslingOppdatertProducer,
    private val enableBatch: Boolean
) : Subscriber() {

    private val log = KotlinLogging.logger {}
    private val teamLog = TeamLogs.logger {}

    override fun subscribe() = Subscription.forEvent("opprettet")
        .withFields("type", "varselId", "ident", "eksternVarslingBestilling", "opprettet", "produsent")

    override suspend fun receive(jsonMessage: JsonMessage) = traceVarsel(id = jsonMessage["varselId"].asText(), mapOf("action" to "opprett")) {
        val produsent = Produsent(
            cluster = jsonMessage["produsent"]["cluster"].asText(),
            namespace = jsonMessage["produsent"]["namespace"].asText(),
            appnavn = jsonMessage["produsent"]["appnavn"].asText()
        )

        val varsel = Varsel(
            varseltype = jsonMessage["type"].asText().let(::parseVarseltype),
            varselId = jsonMessage["varselId"].asText(),
            prefererteKanaler = jsonMessage["eksternVarslingBestilling"]["prefererteKanaler"].map {
                Kanal.valueOf(it.asText()) },
            smsVarslingstekst = jsonMessage["eksternVarslingBestilling"]["smsVarslingstekst"].asTextOrNull(),
            epostVarslingstittel = jsonMessage["eksternVarslingBestilling"]["epostVarslingstittel"].asTextOrNull(),
            epostVarslingstekst = jsonMessage["eksternVarslingBestilling"]["epostVarslingstekst"].asTextOrNull(),
            produsent = produsent,
            aktiv = true,
            behandletAvLegacy = false
        ).also {
            validate(it)
        }

        if (isDuplicate(varsel)) {
            log.info { "Ignorerer duplikat varsel" }
            throw DuplicateVarselException()
        }

        findExistingBatch(jsonMessage)
            ?.let { addToExistingBatch(varsel, it) }
            ?: createNewEksternVarsling(varsel, jsonMessage)

        EksternStatusOppdatering(
            status = EksternStatus.Status.Venter,
            varselId = varsel.varselId,
            ident = jsonMessage["ident"].asText(),
            varseltype = varsel.varseltype,
            produsent = varsel.produsent,
            kanal = null,
            renotifikasjon = null,
            batch = null,
            melding = null,
            feilmelding = null
        ).let {
            statusProducer.eksternStatusOppdatert(it)
        }
    }

    private fun validate(varsel: Varsel) {
        try {
            VarseltekstValidation.validate(varsel)
        } catch (e: VarseltekstValidationException) {
            log.warn { "Feil ved validering av varseltekser" }
            teamLog.warn { "Feil ved validering av varseltekster for opprettet varsel [$varsel]: ${e.explanation.joinToString()}" }

            throw InvalidVarseltekstException()
        }
    }

    private fun isDuplicate(varsel: Varsel): Boolean {
        return repository.varselExists(varsel.varselId)
    }

    private fun addToExistingBatch(varsel: Varsel, existingBatch: EksternVarsling) {
        log.info { "Legger til varsel i eksisterende varsling" }
        repository.addVarselToExisting(
            sendingsId = existingBatch.sendingsId,
            varsel = varsel
        )
    }

    private fun createNewEksternVarsling(varsel: Varsel, jsonMessage: JsonMessage) {
        val kanBatches = if (enableBatch) {
            jsonMessage["eksternVarslingBestilling"]["kanBatches"].asBooleanOrNull() ?: false
        } else {
            false
        }

        val utsettSendingTil = jsonMessage["eksternVarslingBestilling"]["utsettSendingTil"].asTextOrNull()?.let { ZonedDateTime.parse(it) }

        val (erBatch, utsending) = if (utsettSendingTil != null) {
            false to utsettSendingTil
        } else if (kanBatches){
            true to ZonedDateTimeHelper.nowAtUtc().plusHours(1)
        } else {
            false to null
        }

        val sendingsId = if (erBatch) {
            UUID.randomUUID().toString()
        } else {
            varsel.varselId
        }

        val eksternVarsling = EksternVarsling(
            sendingsId = sendingsId,
            ident = jsonMessage["ident"].asText(),
            erBatch = erBatch,
            erUtsattVarsel = utsettSendingTil != null,
            varsler = listOf(varsel),
            utsending = utsending,
            ferdigstilt = null,
            status = Sendingsstatus.Venter,
            eksternStatus = null,
            bestilling = null,
            opprettet = jsonMessage["opprettet"].asText().let(ZonedDateTime::parse)
        )

        log.info { "Oppretter ny varsling for varsel" }
        repository.insertEksternVarsling(eksternVarsling)
    }

    private fun findExistingBatch(jsonMessage: JsonMessage): EksternVarsling? {
        val kanBatches = if (enableBatch) {
            jsonMessage["eksternVarslingBestilling"]["kanBatches"].asBooleanOrNull() ?: false
        } else {
            false
        }

        val utsettSendingTil = jsonMessage["eksternVarslingBestilling"]["utsettSendingTil"].asTextOrNull()?.let { ZonedDateTime.parse(it) }

        return if (kanBatches && utsettSendingTil == null) {
            repository.findExistingBatch(jsonMessage["ident"].asText())
        } else {
            null
        }
    }

    private fun parseVarseltype(type: String): Varseltype {
        return Varseltype.entries.find { it.name.lowercase() == type.lowercase() }
            ?: throw IllegalArgumentException("Fant ikke varsel type med type: $type")
    }

    private fun JsonNode?.asTextOrNull(): String? {
        return if (this == null || isNull) {
            null
        } else {
            asText()
        }
    }

    private fun JsonNode?.asBooleanOrNull(): Boolean? {
        return if (this == null || isNull) {
            null
        } else {
            asBoolean()
        }
    }
}

class DuplicateVarselException: MessageException("Ekstern varsling er allerede registrert for varsel")
class InvalidVarseltekstException: MessageException("Innhold i varseltekster er ikke gyldig")
