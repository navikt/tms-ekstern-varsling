package no.nav.tms.ekstern.varsling.setup

import kotliquery.Row
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.EksternVarsling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Kanal.valueOf
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.Varsel
import no.nav.tms.ekstern.varsling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.transaction
import no.nav.tms.ekstern.varsling.bestilling.updateInTx

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
                        varsler,
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
                        :varsler,
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
                    "varsler" to emptyList<Varsel>().toJsonb()
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

    fun insertEksternVarslingWithLegacyVarsel(eksternVarsling: EksternVarsling) {
        database.update {
            queryOf(
                """
                insert into ekstern_varsling(sendingsId, ident, erBatch, erUtsattVarsel, varsler, utsending, ferdigstilt, opprettet, status, bestilling)
                values (:sendingsId, :ident, :erBatch, :erUtsattVarsel, :varsler, :utsending, :ferdigstilt, :opprettet, :status, :bestilling)
            """,
                mapOf(
                    "sendingsId" to eksternVarsling.sendingsId,
                    "ident" to eksternVarsling.ident,
                    "erBatch" to eksternVarsling.erBatch,
                    "erUtsattVarsel" to eksternVarsling.erUtsattVarsel,
                    "varsler" to eksternVarsling.varsler.filter{ it.legacyJsonb }.toJsonb(),
                    "utsending" to eksternVarsling.utsending,
                    "ferdigstilt" to eksternVarsling.ferdigstilt,
                    "status" to eksternVarsling.status.name,
                    "bestilling" to eksternVarsling.bestilling?.toJsonb(),
                    "opprettet" to eksternVarsling.opprettet,
                )
            )
        }

        eksternVarsling.varsler.filterNot { it.legacyJsonb }.forEach { varsel ->
            database.update {
                queryOf(
                    """
                insert into varsel(varselId, sendingsId, varseltype, preferertKanal, smsVarslingstekst, epostVarslingstittel, epostVarslingstekst, aktiv, produsent, opprettet, inaktivert)
                values(:varselId, :sendingsId, :varseltype, :preferertKanal, :smsVarslingstekst, :epostVarslingstittel, :epostVarslingstekst, :aktiv, :produsent, :opprettet, :inaktivert)
            """,
                    mapOf(
                        "sendingsId" to eksternVarsling.sendingsId,
                        "varselId" to varsel.varselId,
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
    }

    fun getEksternVarsling(sendingsId: String): EksternVarsling? {
        return database.list {
            queryOf(
                """
                select 
                    ev.*,
                    ev.opprettet as ev_opprettet,
                    v.*,
                    v.opprettet as v_opprettet
                from 
                    ekstern_varsling as ev
                    left join varsel as v on ev.sendingsId = v.sendingsId
                where
                    ev.sendingsId = :sendingsId
            """,
                mapOf("sendingsId" to sendingsId)
            ).map {
                mapEksternVarsling(it) to mapVarsel(it)
            }
        }.let {
            joinEksternVarslingWithVarsel(it)
        }.firstOrNull()
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
