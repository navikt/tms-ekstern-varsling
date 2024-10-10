package no.nav.tms.ekstern.varsling.status

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus.Status.*
import no.nav.tms.ekstern.varsling.bestilling.EksternVarsling
import no.nav.tms.ekstern.varsling.bestilling.EksternVarslingRepository
import no.nav.tms.ekstern.varsling.bestilling.Varsel
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import java.time.Duration
import java.time.temporal.ChronoUnit

class EksternStatusUpdater(
    private val repository: EksternVarslingRepository,
    private val eksternVarslingOppdatertProducer: EksternVarslingOppdatertProducer
) {
    private val log = KotlinLogging.logger {}

    fun updateEksternVarslingStatus(statusEvent: DoknotifikasjonStatusEvent) {
        val varsling = repository.getEksternVarsling(statusEvent.eventId)

        if (varsling == null || statusIsDuplicate(varsling, statusEvent)) {
            log.debug { "Ignorer status [${statusEvent.status}] pga duplikat eller manglende varsel." }
            return
        }

        val currentStatus = varsling.eksternStatus ?: initOversikt()

        updateExistingStatus(statusEvent, currentStatus, varsling)
    }

    private fun initOversikt() = EksternStatus.Oversikt(
        sendt = false,
        renotifikasjonSendt = false,
        kanal = null,
        historikk = emptyList(),
        sistOppdatert = nowAtUtc()
    )

    private fun updateExistingStatus(statusEvent: DoknotifikasjonStatusEvent, currentStatus: EksternStatus.Oversikt, varsling: EksternVarsling) {
        val newEntry = EksternStatus.HistorikkEntry(
            melding = statusEvent.melding,
            status = determineInternalStatus(statusEvent),
            distribusjonsId = statusEvent.distribusjonsId,
            kanal = statusEvent.kanal,
            renotifikasjon = determineIfRenotifikasjon(currentStatus, statusEvent),
            tidspunkt = statusEvent.tidspunkt
        )

        val updatedStatus = EksternStatus.Oversikt(
            sendt = currentStatus.sendt || newEntry.status == Sendt,
            renotifikasjonSendt = if (newEntry.renotifikasjon == true) true else currentStatus.renotifikasjonSendt,
            kanal = currentStatus.kanal ?: newEntry.kanal,
            historikk = currentStatus.historikk + newEntry,
            sistOppdatert = nowAtUtc()
        )

        log.info { "Oppdaterer status [${newEntry.status}] for ekstern varsling" }

        repository.updateEksternStatus(varsling.sendingsId, updatedStatus)
        varsling.varsler.forEach { varsel ->
            buildOppdatering(newEntry, varsel, batch = varsling.varsler.size > 1).let {
                eksternVarslingOppdatertProducer.eksternStatusOppdatert(it)
            }
        }
    }

    private fun determineIfRenotifikasjon(currentStatus: EksternStatus.Oversikt, statusEvent: DoknotifikasjonStatusEvent): Boolean? {
        return when {
            determineInternalStatus(statusEvent) != Sendt -> null
            isFirstAttempt(currentStatus) -> false
            intervalSinceFirstAttempt(currentStatus, statusEvent) > Duration.ofHours(23) -> true
            else -> false
        }
    }

    private fun isFirstAttempt(currentStatus: EksternStatus.Oversikt): Boolean {
        return currentStatus.historikk.none { it.status == Sendt || it.status == Feilet }
    }

    private fun statusIsDuplicate(varsling: EksternVarsling, statusEvent: DoknotifikasjonStatusEvent): Boolean {

        return if (varsling.eksternStatus == null) {
            false
        } else {
            varsling.eksternStatus.historikk
                .filter { it.status == determineInternalStatus(statusEvent) }
                .filter { it.distribusjonsId == statusEvent.distribusjonsId }
                .filter { it.kanal == statusEvent.kanal }
                .filter { it.tidspunkt.truncatedTo(ChronoUnit.MILLIS) == statusEvent.tidspunkt.truncatedTo(ChronoUnit.MILLIS) }
                .any()
        }
    }

    private fun intervalSinceFirstAttempt(currentStatus: EksternStatus.Oversikt, statusEvent: DoknotifikasjonStatusEvent): Duration {
        val previous = currentStatus.historikk
            .filter { it.status == Sendt || it.status == Feilet }
            .minOf { it.tidspunkt }

        return Duration.between(previous, statusEvent.tidspunkt)
    }

    private fun buildOppdatering(newEntry: EksternStatus.HistorikkEntry, varsel: Varsel, batch: Boolean) = EksternStatusOppdatering(
        status = newEntry.status,
        kanal = newEntry.kanal,
        varseltype = varsel.varseltype,
        varselId = varsel.varselId,
        renotifikasjon = newEntry.renotifikasjon,
        feilmelding = if (newEntry.status == Feilet) newEntry.melding else null,
        batch = batch,
        produsent = varsel.produsent
    )

    private fun determineInternalStatus(statusEvent: DoknotifikasjonStatusEvent): EksternStatus.Status {
        return when(statusEvent.status) {
            DoknotifikasjonStatusEnum.FERDIGSTILT.name -> if (statusEvent.kanal.isNullOrBlank()) EksternStatus.Status.Ferdigstilt else Sendt
            DoknotifikasjonStatusEnum.INFO.name -> Info
            DoknotifikasjonStatusEnum.FEILET.name -> Feilet
            DoknotifikasjonStatusEnum.OVERSENDT.name -> Bestilt
            else -> throw IllegalArgumentException("Kjente ikke igjen doknotifikasjon status ${statusEvent.status}.")
        }
    }
}
