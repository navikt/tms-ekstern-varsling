package no.nav.tms.ekstern.varsling.utsending

import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper

import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.Bestilling
import no.nav.tms.ekstern.varsling.EksternVarsling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.Varsel
import no.nav.tms.ekstern.varsling.Varseltype
import java.time.ZonedDateTime

class EksternVarslingUtsendingRepository(val database: PostgresDatabase) {

    fun nextInVarselQueue(batchSize: Int): List<EksternVarsling> {
        return database.list {
            queryOf(
                """
                select * from (
                    select 
                        ev.*,
                        ev.opprettet as ev_opprettet,
                        v.*,
                        v.opprettet as v_opprettet
                    from 
                        ekstern_varsling as ev
                        left join varsel as v on ev.sendingsId = v.sendingsId
                    where 
                        ferdigstilt is null and (utsending is null or utsending < :now)
                    order by ev_opprettet
                ) as subquery order by ev_opprettet limit :antall
                """,
                mapOf(
                    "antall" to batchSize,
                    "now" to ZonedDateTimeHelper.nowAtUtc()
                )
            ).map {
                mapEksternVarsling(it) to mapVarsel(it)
            }
        }.let {
            joinEksternVarslingWithVarsel(it)
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
        opprettet = row.zonedDateTime("ev_opprettet"),
        varsler = row.json("varsler")
    )

    private fun mapVarsel(row: Row): Varsel? {
        return if(row.stringOrNull("varselId") != null) {
            Varsel(
                varselId = row.string("varselId"),
                varseltype = row.string("varseltype").let(Varseltype::valueOf),
                preferertKanal = row.stringOrNull("preferertKanal")?.let(Kanal::valueOf),
                smsVarslingstekst = row.stringOrNull("smsVarslingstekst"),
                epostVarslingstittel = row.stringOrNull("epostVarslingstittel"),
                epostVarslingstekst = row.stringOrNull("epostVarslingstekst"),
                produsent = row.json("produsent"),
                aktiv = row.boolean("aktiv"),
                opprettet = row.zonedDateTime("v_opprettet"),
                inaktivert = row.zonedDateTimeOrNull("inaktivert"),
                legacyJsonb = false
            )
        } else {
            null
        }
    }

    private fun joinEksternVarslingWithVarsel(rows: List<Pair<EksternVarsling, Varsel?>>): List<EksternVarsling> {
        return rows
            .groupBy({(eksternVarsling, _) -> eksternVarsling}, { (_, varsel) -> varsel })
            .map { (eksternVarsling, varsler) ->
                eksternVarsling.copy(varsler = eksternVarsling.varsler + varsler.filterNotNull())
            }
    }
}
