package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.annotation.JsonAlias
import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.time.ZonedDateTime

class PeriodicVarselSender(
    private val repository: EksternVarselRepository,
    private val kafkaProducer: Producer<String, String>,
    private val kafkaTopic: String
) : PeriodicJob(Duration.ofMinutes(1)) {

    val isLeader = true
    override val job = initializeJob {
        if (isLeader) {
            repository
                .nextInVarselQueue()
                .forEach(::processRequest)
        }
    }
    private val objectMapper = defaultObjectMapper()

    private fun processRequest(eksternVarsling: EksternVarsling) {

        val tekster = bestemTekster(eksternVarsling)

        val revarsling = bestemRevarsling(eksternVarsling)

        val sending = SendEksternVarsling(
            sendingsId = eksternVarsling.sendingsId,
            ident = eksternVarsling.ident,
            kanal = eksternVarsling.kanal,
            smsVarslingstekst = tekster.smsTekst,
            epostVarslingstittel = tekster.epostTittel,
            epostVarslingstekst = tekster.epostTekst,
            antallRevarslinger = revarsling.antallRevarslinger,
            revarslingsIntervall = revarsling.revarslingsIntervall,
            produsent = Produsent("todo-gcp", "min-side", "tms-ekstern-varsling"),
        ).let { objectMapper.writeValueAsString(it) }

        kafkaProducer.send(ProducerRecord(kafkaTopic, eksternVarsling.sendingsId, sending))
        repository.markAsSent(
            sendingsId = eksternVarsling.sendingsId, sendt = ZonedDateTime.now()
        )
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
    @JsonAlias("@event_name")
    val eventName: String = "sendEksternVarsling"
}
