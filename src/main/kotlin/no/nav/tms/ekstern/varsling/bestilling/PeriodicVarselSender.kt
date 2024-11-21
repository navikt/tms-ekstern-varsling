package no.nav.tms.ekstern.varsling.bestilling

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.client.Counter
import no.nav.doknotifikasjon.schemas.Doknotifikasjon
import no.nav.doknotifikasjon.schemas.PrefererteKanal
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.observability.traceVarsel
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.TmsEksternVarsling
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.status.EksternStatusOppdatering
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.kafka.application.AppHealth
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

class PeriodicVarselSender(
    private val repository: EksternVarslingRepository,
    private val kanalDecider: PreferertKanalDecider,
    private val kafkaProducer: Producer<String, Doknotifikasjon>,
    private val statusProducer: EksternVarslingOppdatertProducer,
    private val doknotTopic: String,
    private val leaderElection: PodLeaderElection,
    interval: Duration = Duration.ofSeconds(1),
) : PeriodicJob(interval) {

    private val log = KotlinLogging.logger {}
    private val securelog = KotlinLogging.logger("secureLog")

    override val job = initializeJob {
        if (leaderElection.isLeader()) {
            try {
                repository
                    .nextInVarselQueue()
                    .also { logInfo(it) }
                    .forEach(::processRequest)
            } catch (e: Exception) {
                log.error { "Feil ved prosessering av varsel-kø" }
                securelog.error(e) { "Feil ved prosessering av varsel-kø" }
            }
        }
    }

    fun isHealthy() = if (job.isActive) {
        AppHealth.Healthy
    } else {
        AppHealth.Unhealthy
    }

    private fun logInfo(varsler: List<EksternVarsling>) {
        if (varsler.isNotEmpty()) {
            log.info { "Behandler ${varsler.size} ventende sendinger." }
        }
    }

    private fun processRequest(eksternVarsling: EksternVarsling) {
        if (eksternVarsling.varsler.any { it.aktiv && !it.behandletAvLegacy }) {
            sendEksternVarsling(eksternVarsling)
        } else {
            logKansellering(eksternVarsling)
            repository.markAsCancelled(ferdigstilt = nowAtUtc(), eksternVarsling.sendingsId)
            sendKansellertStatus(eksternVarsling)
            EKSTERN_VARSLING_KANSELLERT.inc()
        }
    }

    private fun sendEksternVarsling(varsling: EksternVarsling) {

        val tekster = bestemTekster(varsling)

        val kanal = kanalDecider.bestemKanal(varsling)

        val revarsling = bestemRevarsling(varsling)

        val doknot = Doknotifikasjon.newBuilder()
            .setBestillingsId(varsling.sendingsId)
            .setBestillerId(TmsEksternVarsling.appnavn)
            .setFodselsnummer(varsling.ident)
            .setTittel(tekster.epostTittel)
            .setEpostTekst(tekster.epostTekst)
            .setSmsTekst(tekster.smsTekst)
            .setPrefererteKanaler(mapKanal(kanal))
            .setRenotifikasjoner(revarsling)
            .build()

        logSending(varsling, kanal)

        val bestilling = Bestilling(
            preferertKanal = kanal,
            revarsling = revarsling,
            tekster = tekster
        )

        kafkaProducer.send(ProducerRecord(doknotTopic, varsling.sendingsId, doknot))
        repository.markAsSent(
            sendingsId = varsling.sendingsId, ferdigstilt = nowAtUtc(), bestilling = bestilling
        )

        EKSTERN_VARSLING_SENDT.labels(
            varsling.erBatch.toString(),
            kanal.name,
            varsling.erUtsattVarsel.toString()
        ).inc()
    }

    private fun sendKansellertStatus(varsling: EksternVarsling) {
        varsling.varsler.map {
            EksternStatusOppdatering(
                status = EksternStatus.Status.Kansellert,
                varselId = it.varselId,
                ident = varsling.ident,
                kanal = null,
                renotifikasjon = null,
                batch = varsling.varsler.size > 1,
                varseltype = it.varseltype,
                produsent = it.produsent,
                melding = null,
                feilmelding = null
            ).let { status ->
                statusProducer.eksternStatusOppdatert(status)
            }
        }
    }

    private fun bestemRevarsling(varsling: EksternVarsling): Revarsling? {
        return if (varsling.varsler.size == 1) {
            val varsel = varsling.varsler.first()

            when (varsel.varseltype) {
                Varseltype.Innboks -> Revarsling(
                    antall = 1,
                    intervall = 4
                )
                Varseltype.Oppgave -> Revarsling(
                    antall = 1,
                    intervall = 7
                )
                Varseltype.Beskjed -> null
            }
        } else {
            null
        }
    }

    private fun Doknotifikasjon.Builder.setRenotifikasjoner(revarsling: Revarsling?) = also {
        if (revarsling != null) {
            antallRenotifikasjoner = revarsling.antall
            renotifikasjonIntervall = revarsling.intervall
        } else {
            antallRenotifikasjoner = 0
            renotifikasjonIntervall = 0
        }
    }

    private fun logSending(varsling: EksternVarsling, kanal: Kanal) = traceVarsel(id = varsling.sendingsId, mapOf("action" to "sendEksternVarsling")) {
        if (varsling.erBatch && varsling.varsler.size > 1) {
            log.info { "Sender ekstern varsling via kanal $kanal for batch med ${varsling.varsler.size} varsler." }
        } else if (varsling.erBatch) {
            log.info { "Sender ekstern varsling via kanal $kanal for batch med ett ${varsling.varsler.first().varseltype}-varsel." }
        } else if (varsling.erUtsattVarsel) {
            log.info { "Sender utsatt ekstern varsling via kanal $kanal for ett ${varsling.varsler.first().varseltype}-varsel." }
        } else {
            log.info { "Sender ekstern varsling via kanal $kanal for ett ${varsling.varsler.first().varseltype}-varsel." }
        }
    }

    private fun logKansellering(varsling: EksternVarsling) = traceVarsel(id = varsling.sendingsId, mapOf("action" to "kansellerEksternVarsling")) {
        if (varsling.varsler.none { it.aktiv }) {
            log.info { "Kansellerer varsling fordi alle (${varsling.varsler.size}) varsler ble markert inaktive." }
        } else if (varsling.varsler.all { it.behandletAvLegacy }) {
            log.info { "Kansellerer varsling fordi alle (${varsling.varsler.size}) varsler ble markert som behandlet av legacy." }
        } else {
            log.info { "Kansellerer sending av ekstern varsling." }
        }
    }
}

private fun mapKanal(kanal: Kanal) = when(kanal) {
    Kanal.EPOST -> listOf(PrefererteKanal.EPOST)
    Kanal.SMS -> listOf(PrefererteKanal.SMS)
    else -> throw IllegalArgumentException("Kun SMS og EPOST lar seg oversette direkte til doknot preferert kanal")
}

private val EKSTERN_VARSLING_SENDT: Counter = Counter.build()
    .name("ekstern_varsling_bestilt")
    .namespace("tms_ekstern_varsling_v2")
    .help("Ekstern varsling bestilt")
    .labelNames("er_batch","kanal", "er_utsatt")
    .register()

private val EKSTERN_VARSLING_KANSELLERT: Counter = Counter.build()
    .name("ekstern_varsling_kansellert")
    .namespace("tms_ekstern_varsling_v2")
    .help("Ekstern varsling kansellert")
    .register()
