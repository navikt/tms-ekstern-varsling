package no.nav.tms.ekstern.varsling.setup

import kotliquery.Row
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.*
import no.nav.tms.ekstern.varsling.bestilling.transaction
import no.nav.tms.ekstern.varsling.bestilling.updateInTx
import no.nav.tms.ekstern.varsling.common.JsonAgg.Companion.jsonAgg

class TestRepository(
    private val database: PostgresDatabase
) {
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
                    "opprettet" to eksternVarsling.opprettet,
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

    fun getEksternVarsling(sendingsId: String): EksternVarsling? {
        return database.singleOrNull {
            queryOf(
                """
                select 
                    ev.*,
                    jsonb_agg(v.*) as varsler
                from 
                    ekstern_varsling as ev
                    left join varsel as v on ev.sendingsId = v.sendingsId
                where
                    ev.sendingsId = :sendingsId
                group by ev.sendingsId
            """,
                mapOf("sendingsId" to sendingsId)
            ).map {
                mapEksternVarsling(it)
            }
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
