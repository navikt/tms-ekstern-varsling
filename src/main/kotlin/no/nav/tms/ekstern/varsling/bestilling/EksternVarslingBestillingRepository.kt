package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.Row
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.*
import no.nav.tms.ekstern.varsling.common.JsonAgg.Companion.jsonAgg
import java.time.ZonedDateTime

class EksternVarslingBestillingRepository(val database: PostgresDatabase) {

    fun insertEksternVarsling(eksternVarsling: EksternVarsling) {
        database.transaction {
            insertEksternVarslingRow(eksternVarsling)
            eksternVarsling.varsler.forEach { varsel ->
                insertVarselRow(eksternVarsling.sendingsId, varsel)
            }
        }
    }

    private fun TransactionalSession.insertEksternVarslingRow(eksternVarsling: EksternVarsling) {
        updateInTx {
            queryOf(
                """
                    insert into ekstern_varsling(
                        sendingsId,
                        ident,
                        erBatch,
                        erUtsattVarsel,
                        utsending,
                        ferdigstilt,
                        status,
                        eksternStatus,
                        bestilling,
                        opprettet
                    ) values (
                        :sendingsId,
                        :ident,
                        :erBatch,
                        :erUtsattVarsel,
                        :utsending,
                        :ferdigstilt,
                        :status,
                        :eksternStatus,
                        :bestilling,
                        :opprettet
                    )
                """, mapOf(
                    "sendingsId" to eksternVarsling.sendingsId,
                    "ident" to eksternVarsling.ident,
                    "erBatch" to eksternVarsling.erBatch,
                    "erUtsattVarsel" to eksternVarsling.erUtsattVarsel,
                    "utsending" to eksternVarsling.utsending,
                    "ferdigstilt" to eksternVarsling.ferdigstilt,
                    "status" to eksternVarsling.status.name,
                    "eksternStatus" to eksternVarsling.eksternStatus.toJsonb(),
                    "bestilling" to eksternVarsling.bestilling.toJsonb(),
                    "opprettet" to eksternVarsling.opprettet
                )
            )
        }
    }

    private fun TransactionalSession.insertVarselRow(sendingsId: String, varsel: Varsel) {
        updateInTx {
            queryOf(
                """
                    insert into varsel(
                        varselId,
                        sendingsId,
                        varseltype,
                        preferertKanal,
                        smsVarslingstekst,
                        epostVarslingstittel,
                        epostVarslingstekst,
                        aktiv,
                        produsent,
                        opprettet,
                        inaktivert
                    ) values (
                        :varselId,
                        :sendingsId,
                        :varseltype,
                        :preferertKanal,
                        :smsVarslingstekst,
                        :epostVarslingstittel,
                        :epostVarslingstekst,
                        :aktiv,
                        :produsent,
                        :opprettet,
                        :inaktivert
                    )
                """, mapOf(
                    "varselId" to varsel.varselId,
                    "sendingsId" to sendingsId,
                    "varseltype" to varsel.varseltype.name,
                    "preferertKanal" to varsel.preferertKanal?.name,
                    "smsVarslingstekst" to varsel.smsVarslingstekst,
                    "epostVarslingstittel" to varsel.epostVarslingstittel,
                    "epostVarslingstekst" to varsel.epostVarslingstekst,
                    "aktiv" to varsel.aktiv,
                    "produsent" to varsel.produsent.toJsonb(),
                    "opprettet" to varsel.opprettet!!,
                    "inaktivert" to varsel.inaktivert
                )
            )
        }
    }

    fun findExistingBatch(ident: String, utsendingEtter: ZonedDateTime): String? = database.singleOrNull {
        queryOf(
            """
                select 
                    sendingsId
                from 
                    ekstern_varsling
                where
                    ident = :ident and
                    erBatch and
                    not erUtsattVarsel and
                    ferdigstilt is null and
                    utsending > :utsendingEtter
            """,
            mapOf(
                "ident" to ident,
                "utsendingEtter" to utsendingEtter
            )
        )
            .map {
                it.string("sendingsId")
            }
    }

    fun addVarsel(sendingsId: String, varsel: Varsel) {
        database.update {
            queryOf(
                """
                    insert into varsel(
                        varselId,
                        sendingsId,
                        varseltype,
                        preferertKanal,
                        smsVarslingstekst,
                        epostVarslingstittel,
                        epostVarslingstekst,
                        aktiv,
                        produsent,
                        opprettet,
                        inaktivert
                    ) values (
                        :varselId,
                        :sendingsId,
                        :varseltype,
                        :preferertKanal,
                        :smsVarslingstekst,
                        :epostVarslingstittel,
                        :epostVarslingstekst,
                        :aktiv,
                        :produsent,
                        :opprettet,
                        :inaktivert
                    )
                """, mapOf(
                    "varselId" to varsel.varselId,
                    "sendingsId" to sendingsId,
                    "varseltype" to varsel.varseltype.name,
                    "preferertKanal" to varsel.preferertKanal?.name,
                    "smsVarslingstekst" to varsel.smsVarslingstekst,
                    "epostVarslingstittel" to varsel.epostVarslingstittel,
                    "epostVarslingstekst" to varsel.epostVarslingstekst,
                    "aktiv" to varsel.aktiv,
                    "produsent" to varsel.produsent.toJsonb(),
                    "opprettet" to varsel.opprettet,
                    "inaktivert" to varsel.inaktivert
                )
            )
        }
    }

    fun findSendingForVarsel(varselId: String): EksternVarsling? {
        return database.singleOrNull {
            queryOf(
                """
                    with sending as (
                        select 
                            sendingsId 
                        from 
                            varsel
                        where 
                            varselId = :varselId
                    ) 
                select 
                    ev.*,
                    jsonb_agg(v.*) as varsler
                from 
                    sending 
                    join ekstern_varsling as ev on sending.sendingsId = ev.sendingsId
                    left join varsel as v on ev.sendingsId = v.sendingsId
                group by ev.sendingsId
                """,
                mapOf(
                    "varselId" to varselId
                )
            ).map {
                mapEksternVarsling(it)
            }
        }
    }

    fun varselExists(varselId: String): Boolean {
        return database.singleOrNull {
            queryOf(
                """
                    select 
                        ev.sendingsId
                    from 
                        ekstern_varsling as ev
                        left join varsel as v on ev.sendingsId = v.sendingsId
                    where 
                        v.varselId = :varselId
                """,
                mapOf(
                    "varselId" to varselId
                )
            ).map {
                true
            }
        } ?: false

    }

    fun inaktiverVarsel(varselId: String, inaktivert: ZonedDateTime) {
        database.update {
            queryOf(
                "update varsel set aktiv = false, inaktivert = :inaktivert where varselId = :varselId",
                mapOf("varselId" to varselId, "inaktivert" to inaktivert)
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
