package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.annotation.JsonProperty
import io.prometheus.client.Counter
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

class PeriodicVarselSender(
    private val repository: EksternVarselRepository,
    private val kafkaProducer: Producer<String, String>,
    private val kafkaTopic: String,
    private val leaderElection: PodLeaderElection,
    interval: Duration = Duration.ofSeconds(1)
) : PeriodicJob(interval) {

    override val job = initializeJob {
        if (leaderElection.isLeader()) {
            repository
                .nextInVarselQueue()
                .forEach(::processRequest)
        }
    }
    private val objectMapper = defaultObjectMapper()

    private fun processRequest(eksternVarsling: EksternVarsling) {
        if (eksternVarsling.varsler.any{it.aktiv}) {
            sendEksternVarsling(eksternVarsling)

        } else {
            repository.kansellerSending(ferdigstilt = ZonedDateTimeHelper.nowAtUtc(),eksternVarsling.sendingsId)
            EKSTERN_VARSLING_KANSELLERT.inc()
        }
    }

    private fun sendEksternVarsling(eksternVarsling: EksternVarsling) {

        val tekster = bestemTekster(eksternVarsling)

        val revarsling = bestemRevarsling(eksternVarsling)

        val kanal = bestemKanal(eksternVarsling)

        val sending = SendEksternVarsling(
            sendingsId = eksternVarsling.sendingsId,
            ident = eksternVarsling.ident,
            kanal = kanal,
            smsVarslingstekst = tekster.smsTekst,
            epostVarslingstittel = tekster.epostTittel,
            epostVarslingstekst = tekster.epostTekst,
            antallRevarslinger = revarsling.antallRevarslinger,
            revarslingsIntervall = revarsling.revarslingsIntervall,
            produsent = Produsent("todo-gcp", "min-side", "tms-ekstern-varsling"),
        ).let { objectMapper.writeValueAsString(it) }

        kafkaProducer.send(ProducerRecord(kafkaTopic, eksternVarsling.sendingsId, sending))
        repository.markAsSent(
            sendingsId = eksternVarsling.sendingsId, ferdigstilt = ZonedDateTimeHelper.nowAtUtc(), kanal = kanal
        )

        EKSTERN_VARSLING_SENDT.labels(
            eksternVarsling.erBatch.toString(),
            kanal.name,
            eksternVarsling.erUtsattVarsel.toString()
        ).inc()
    }

    private fun bestemKanal(eksternVarsling: EksternVarsling): Kanal {
        return eksternVarsling.varsler
            .filter { it.aktiv }
            .flatMap { it.prefererteKanaler }
            .distinct()
            .find { it == Kanal.SMS }
            ?: Kanal.EPOST
    }
}

private data class Revarsling(
    val antallRevarslinger: Int,
    val revarslingsIntervall: Int,
) {
    companion object {
        fun ingen() = Revarsling(0, 0)
    }
}

private fun bestemRevarsling(eksternVarsling: EksternVarsling): Revarsling {
    if (eksternVarsling.varsler.size > 1) {
        return Revarsling.ingen()
    }

    val varsel = eksternVarsling.varsler.first()

    return when (varsel.varseltype) {
        Varseltype.Beskjed -> Revarsling.ingen()
        Varseltype.Innboks -> Revarsling(antallRevarslinger = 1, revarslingsIntervall = 4)
        Varseltype.Oppgave -> Revarsling(antallRevarslinger = 1, revarslingsIntervall = 7)
    }
}

private data class SendEksternVarsling(
    val sendingsId: String,
    val ident: String,
    val kanal: Kanal,
    val smsVarslingstekst: String,
    val epostVarslingstittel: String,
    val epostVarslingstekst: String,
    val antallRevarslinger: Int,
    val revarslingsIntervall: Int,
    val produsent: Produsent
) {
    @JsonProperty("@event_name")
    val eventName: String = "sendEksternVarsling"
}

private val EKSTERN_VARSLING_SENDT: Counter = Counter.build()
    .name("ekstern_varsling_sendt")
    .namespace("tms_ekstern_varsling_v1")
    .help("Ekstern varsling sendt")
    .labelNames("er_batch","kanal", "er_utsatt")
    .register()

private val EKSTERN_VARSLING_KANSELLERT: Counter = Counter.build()
    .name("ekstern_varsling_kansellert")
    .namespace("tms_ekstern_varsling_v1")
    .help("Ekstern varsling kansellert")
    .register()
