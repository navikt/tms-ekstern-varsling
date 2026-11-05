package no.nav.tms.ekstern.varsling.utsending

import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.*
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper
import no.nav.tms.ekstern.varsling.common.JsonAgg.Companion.jsonAgg
import java.time.ZonedDateTime

class EksternVarslingUtsendingRepository(val database: PostgresDatabase) {

    fun nextInVarselQueue(batchSize: Int): List<EksternVarsling> {
        return database.list {
            queryOf(
                """
                    select 
                        ev.*,
                        jsonb_agg(v.*) as varsler
                    from 
                        ekstern_varsling as ev
                        left join varsel as v on ev.sendingsId = v.sendingsId
                    where 
                        ferdigstilt is null and (utsending is null or utsending < :now)
                    group by ev.sendingsId
                    order by ev.opprettet
                    limit :antall
                """,
                mapOf(
                    "antall" to batchSize,
                    "now" to ZonedDateTimeHelper.nowAtUtc()
                )
            ).map {
                mapEksternVarsling(it)
            }
        }
    }

    // Teller elementer som er klare for sending umiddelbart
    fun varselQueueSize(): Int {
        return database.single {
            queryOf(
                """
                    select
                       count(*) as antall
                    from 
                        ekstern_varsling
                    where 
                        ferdigstilt is null and (utsending is null or utsending < :now)
                """,
                mapOf(
                    "now" to ZonedDateTimeHelper.nowAtUtc()
                )
            ).map {
                it.int("antall")
            }
        }
    }

    fun markAsSent(sendingsId: String, ferdigstilt: ZonedDateTime, bestilling: Bestilling) {
        database.update {
            queryOf(
                """
                update 
                    ekstern_varsling 
                set 
                    ferdigstilt = :ferdigstilt,
                    status = :status,
                    bestilling = :bestilling
                where 
                    sendingsId = :sendingsId
                """,
                mapOf(
                    "ferdigstilt" to ferdigstilt,
                    "sendingsId" to sendingsId,
                    "status" to Sendingsstatus.Sendt.name,
                    "bestilling" to bestilling.toJsonb()
                )
            )
        }
    }

    fun markAsCancelled(ferdigstilt: ZonedDateTime, sendingsId: String) {
        database.update {
            queryOf(
                "update ekstern_varsling set ferdigstilt = :ferdigstilt, status = :status where sendingsId = :sendingsId",
                mapOf(
                    "ferdigstilt" to ferdigstilt,
                    "sendingsId" to sendingsId,
                    "status" to Sendingsstatus.Kansellert.name)
            )
        }
    }

    private fun mapEksternVarsling(row: Row) = EksternVarsling(
        sendingsId = row.string("sendingsId"),
        ident = row.string("ident"),
        erBatch = row.boolean("erBatch"),
        erUtsattVarsel = row.boolean("erUtsattVarsel"),
        utsending = row.zonedDateTimeOrNull("utsending"),
        ferdigstilt = row.zonedDateTimeOrNull("ferdigstilt"),
        status = row.string("status").let(Sendingsstatus::valueOf),
        eksternStatus = row.jsonOrNull("eksternStatus"),
        bestilling = row.jsonOrNull("bestilling"),
        opprettet = row.zonedDateTime("opprettet"),
        varsler = row.jsonAgg("varsler")
            .map { varsel ->
                Varsel(
                    varselId = varsel.string("varselId"),
                    varseltype = varsel.string("varseltype").let(Varseltype::valueOf),
                    preferertKanal = varsel.stringOrNull("preferertKanal")?.let(Kanal::valueOf),
                    smsVarslingstekst = varsel.stringOrNull("smsVarslingstekst"),
                    epostVarslingstittel = varsel.stringOrNull("epostVarslingstittel"),
                    epostVarslingstekst = varsel.stringOrNull("epostVarslingstekst"),
                    produsent = varsel.json("produsent"),
                    aktiv = varsel.boolean("aktiv"),
                    opprettet = varsel.zonedDateTime("opprettet"),
                    inaktivert = varsel.zonedDateTimeOrNull("inaktivert")
                )
            }
    )
}
