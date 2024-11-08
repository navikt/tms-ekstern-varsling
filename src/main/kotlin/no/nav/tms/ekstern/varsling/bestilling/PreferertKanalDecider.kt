package no.nav.tms.ekstern.varsling.bestilling

import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId

class PreferertKanalDecider(
    private val smsUtsendingStart: LocalTime,
    private val smsUtsendingEnd: LocalTime,
    private val timezone: ZoneId
) {
    private val log = KotlinLogging.logger {}

    fun bestemKanal(eksternVarsling: EksternVarsling): Kanal {
        val kanaler = eksternVarsling.varsler
            .filter { it.aktiv }
            .flatMap { it.prefererteKanaler }
            .distinct()

        return if (eksternVarsling.erBatch) {
            when {
                kanaler.contains(Kanal.BETINGET_SMS) -> prioritertKanal()
                kanaler.contains(Kanal.SMS) -> Kanal.SMS
                else -> Kanal.EPOST
            }
        } else if (kanaler.size > 1) {
            val kanal = prioritertKanal()

            log.info { "Velger ${kanal.name} som prioritert kanal for varsel med ${kanaler.map { it.name }} preferert. " }

            kanal
        } else if (kanaler.size == 1) {
            when(val kanal = kanaler.first()) {
                Kanal.BETINGET_SMS -> prioritertKanal()
                else -> kanal
            }
        } else {
            Kanal.EPOST
        }
    }

    private fun prioritertKanal(): Kanal {
        val now = LocalTimeHelper.nowAt(timezone)

        val willSendSmsImmediately = now.isAfter(smsUtsendingStart) && now.isBefore(smsUtsendingEnd)

        return if (willSendSmsImmediately) {
            Kanal.SMS
        } else {
            Kanal.EPOST
        }
    }
}

