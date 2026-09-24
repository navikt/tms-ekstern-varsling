package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.Row
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.EksternStatus
import no.nav.tms.ekstern.varsling.EksternVarsling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.Varsel
import no.nav.tms.ekstern.varsling.Varseltype
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

    fun findSendingForVarsel(varselId: String, aktiv: Boolean? = null): EksternVarsling? {
        val sendingsIdForLegacyVarsel = database.singleOrNull {
            queryOf(
                """
                    select 
                        ev.sendingsId 
                    from 
                        ekstern_varsling as ev
                    where 
                        ev.varsler @> :varsel 
                """,
                mapOf(
                    "varsel" to varselId.toParam(aktiv),
                )
            ).map {
                it.string("sendingsId")
            }
        }

        if (sendingsIdForLegacyVarsel != null) {
            return getEksternVarsling(sendingsIdForLegacyVarsel)
        }

        return database.list {
            queryOf(
                """
                    with sending as (
                        select 
                            ev.sendingsId 
                        from 
                            ekstern_varsling as ev
                            left join varsel v on ev.sendingsId = v.sendingsId
                        where 
                            v.varselId = :varselId
                            and v.aktiv
                    ) 
                select 
                    ev.*,
                    ev.opprettet as ev_opprettet,
                    v.*,
                    v.opprettet as v_opprettet
                from 
                    sending 
                    join ekstern_varsling as ev on sending.sendingsId = ev.sendingsId
                    left join varsel as v on ev.sendingsId = v.sendingsId
                """,
                mapOf(
                    "varselId" to varselId
                )
            ).map {
                mapEksternVarsling(it) to mapVarsel(it)
            }
        }.let {
            joinEksternVarslingWithVarsel(it)
        }.firstOrNull()
    }


    fun varselExists(varselId: String): Boolean {
        val existsLegacy = database.singleOrNull {
            queryOf(
                """
                        select 
                            ev.sendingsId 
                        from 
                            ekstern_varsling as ev
                        where 
                            ev.varsler @> :varsel
                    """,
                mapOf(
                    "varsel" to varselId.toParam(),
                    "varselId" to varselId
                )
            ).map {
                true
            }
        } ?: false

        return if (existsLegacy) {
            true
        } else {
            database.singleOrNull {
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
    }

    private fun String.toParam(aktiv: Boolean? = null) = if (aktiv == null) {
        listOf(mapOf("varselId" to this)).toJsonb()
    } else {
        listOf(mapOf("varselId" to this, "aktiv" to aktiv)).toJsonb()
    }

    fun inaktiverVarsel(varselId: String, inaktivert: ZonedDateTime) {
        database.update {
            queryOf(
                "update varsel set aktiv = false, inaktivert = :inaktivert where varselId = :varselId",
                mapOf("varselId" to varselId, "inaktivert" to inaktivert)
            )
        }
    }

    fun updateLegacyVarsler(sendingsId: String, varsler: List<Varsel>){
        database.update {
            queryOf(
                "update ekstern_varsling set varsler = :varsler where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId, "varsler" to varsler.toJsonb())
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
