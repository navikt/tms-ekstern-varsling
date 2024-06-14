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

    private fun processRequest(request: EksternVarsling) = try {
        /*
            val varselId = kafkaProducer.send()
            EksternVarslingBestilling()

            repository.markAsSent(
                referenceId = request.referenceId,
                ident = request.ident,
                varselId = varselId
            )
        } catch (e: VarselValidationException) {
            repository.markAsFailed(
                referenceId = request.referenceId,
                ident = request.ident,
                feilkilde = Feilkilde(
                    melding = e.message ?: "Feil i validering av varsel",
                    forklaring = e.explanation
                )
            )
        */
    } catch (e: Exception) {

    }
}
