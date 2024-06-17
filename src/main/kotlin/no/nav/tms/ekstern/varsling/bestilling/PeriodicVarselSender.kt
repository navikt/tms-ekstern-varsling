package no.nav.tms.ekstern.varsling.bestilling

import no.nav.tms.common.util.scheduling.PeriodicJob
import no.nav.tms.varsel.action.EksternVarslingBestilling
import org.apache.kafka.clients.producer.Producer
import java.time.Duration

class PeriodicVarselSender(
    val repository: EksternVarselRepository,
    val kafkaProducer: Producer<String, String>,
) : PeriodicJob(Duration.ofMinutes(1)) {

    val isLeader = true
    override val job = initializeJob {
        if (isLeader) {
            repository
                .nextInVarselQueue()
                .forEach(::processRequest)
        }
    }

    private fun processRequest(eksternVarsling: EksternVarsling) = try {

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
            produsent = eksternVarsling.produsent,
        )

            val varselId = eksternVarsling.sendingsId
                kafkaProducer.send()
            EksternVarslingBestilling()

            repository.markAsSent(
                referenceId = eksternVarsling.referenceId,
                ident = eksternVarsling.ident,
                varselId = varselId
            )
        } catch (e: VarselValidationException) {
            repository.markAsFailed(
                referenceId = eksternVarsling.referenceId,
                ident = eksternVarsling.ident,
                feilkilde = Feilkilde(
                    melding = e.message ?: "Feil i validering av varsel",
                    forklaring = e.explanation
                )
            )
    } catch (e: Exception) {

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

    return when(varsel.varseltype) {
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
)
