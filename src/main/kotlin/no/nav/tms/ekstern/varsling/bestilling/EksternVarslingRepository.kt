package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.*
import java.time.Duration
import java.time.ZonedDateTime

class EksternVarslingRepository(val database: Database) {

    private val objectMapper = defaultObjectMapper()

    fun insertEksternVarsling(dbVarsel: EksternVarsling) {
        database.update {
            queryOf(
                """
                    insert into ekstern_varsling(
                        sendingsId,
                        ident,
                        erBatch,
                        erUtsattVarsel,
                        varsler,
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
                        :varsler,
                        :utsending,
                        :ferdigstilt,
                        :status,
                        :eksternStatus,
                        :bestilling,
                        :opprettet
                    )
                """, mapOf(
                    "sendingsId" to dbVarsel.sendingsId,
                    "ident" to dbVarsel.ident,
                    "erBatch" to dbVarsel.erBatch,
                    "erUtsattVarsel" to dbVarsel.erUtsattVarsel,
                    "varsler" to dbVarsel.varsler.toJsonb(objectMapper),
                    "utsending" to dbVarsel.utsending,
                    "ferdigstilt" to dbVarsel.ferdigstilt,
                    "status" to dbVarsel.status.name,
                    "eksternStatus" to dbVarsel.eksternStatus.toJsonb(objectMapper),
                    "bestilling" to dbVarsel.bestilling.toJsonb(objectMapper),
                    "opprettet" to dbVarsel.opprettet
                )
            )
        }
    }

    fun getEksternVarsling(sendingsId: String): EksternVarsling? = database.singleOrNull {
        queryOf(
            """
                select 
                    sendingsId,
                    ident,
                    erBatch,
                    erUtsattVarsel,
                    varsler,
                    utsending,
                    ferdigstilt,
                    status,
                    eksternStatus,
                    bestilling,
                    opprettet
                from 
                    ekstern_varsling
                where
                    sendingsId = :sendingsId
            """,
            mapOf("sendingsId" to sendingsId)
        )
            .map(::mapEksternVarsling)
            .asSingle
    }

    fun findExistingBatch(ident: String): EksternVarsling? = database.singleOrNull {
        queryOf(
            """
                select 
                    sendingsId,
                    ident,
                    erBatch,
                    erUtsattVarsel,
                    varsler,
                    utsending,
                    ferdigstilt,
                    status,
                    eksternStatus,
                    bestilling,
                    opprettet
                from 
                    ekstern_varsling
                where
                    ident = :ident and
                    erBatch and
                    not erUtsattVarsel and
                    ferdigstilt is null                    
            """,
            mapOf("ident" to ident)
        )
            .map(::mapEksternVarsling)
            .asSingle
    }

    fun addVarselToExisting(sendingsId: String, varsel: Varsel) {
        database.update {
            queryOf(
                """
                    update ekstern_varsling set varsler = :varsel || varsler where sendingsId = :sendingsId 
                """, mapOf(
                    "sendingsId" to sendingsId,
                    "varsel" to listOf(varsel).toJsonb(objectMapper)
                )

            )
        }
    }

    fun findSendingForVarsel(varselId: String, aktiv: Boolean? = null): EksternVarsling?{
        return database.singleOrNull {
            queryOf(
                """select 
                    sendingsId,
                    ident,
                    erBatch,
                    erUtsattVarsel,
                    varsler,
                    utsending,
                    ferdigstilt,
                    status,
                    eksternStatus,
                    bestilling,
                    opprettet
                from 
                    ekstern_varsling 
                where 
                    varsler @> :varsel
                """,
                mapOf("varsel" to varselId.toParam(aktiv))
            )
                .map(::mapEksternVarsling)
                .asSingle
        }
    }

    fun varselExists(varselId: String): Boolean {
        return database.singleOrNull {
            queryOf(
                "select sendingsId from ekstern_varsling where varsler @> :varsel",
                mapOf("varsel" to varselId.toParam())
            ).map {
                true
            }.asSingle
        } ?: false
    }

    fun nextInVarselQueue(batchSize: Int = 100): List<EksternVarsling> {
        return database.list {
            queryOf(
                """
                select 
                    sendingsId,
                    ident,
                    erBatch,
                    erUtsattVarsel,
                    varsler,
                    utsending,
                    ferdigstilt,
                    status,
                    eksternStatus,
                    bestilling,
                    opprettet
                from 
                    ekstern_varsling
                where 
                    ferdigstilt is null and (utsending is null or utsending < :now)
                limit :antall
                """,
                mapOf(
                    "antall" to batchSize,
                    "now" to ZonedDateTimeHelper.nowAtUtc()
                )
            ).map(::mapEksternVarsling)
            .asList
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
                    "bestilling" to bestilling.toJsonb(objectMapper)
                )
            )
        }
    }

    fun markAsCancelled(ferdigstilt: ZonedDateTime, sendingsId: String) {
        database.update {
            queryOf(
                "update ekstern_varsling set ferdigstilt = :ferdigstilt, status = :status where sendingsId = :sendingsId",
                mapOf(                    "ferdigstilt" to ferdigstilt,
                    "sendingsId" to sendingsId,
                    "status" to Sendingsstatus.Kansellert.name)
            )
        }
    }

    private fun String.toParam(aktiv: Boolean? = null) = if (aktiv == null) {
        listOf(mapOf("varselId" to this)).toJsonb(objectMapper)
    } else {
        listOf(mapOf("varselId" to this, "aktiv" to aktiv)).toJsonb(objectMapper)
    }

    fun updateVarsler(sendingsId: String, varsler: List<Varsel>){
        database.update {
            queryOf(
                "update ekstern_varsling set varsler = :varsler where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId, "varsler" to varsler.toJsonb(objectMapper))
            )
        }
    }

    fun updateEksternStatus(sendingsId: String, eksternStatus: EksternStatus.Oversikt) {
        database.update {
            queryOf(
                "update ekstern_varsling set eksternStatus = :status where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId, "status" to eksternStatus.toJsonb())
            )
        }
    }

    private fun mapEksternVarsling(row: Row) = EksternVarsling(
        sendingsId = row.string("sendingsId"),
        ident = row.string("ident"),
        erBatch = row.boolean("erBatch"),
        erUtsattVarsel = row.boolean("erUtsattVarsel"),
        varsler = row.json<List<Varsel>>("varsler", objectMapper),
        utsending = row.zonedDateTimeOrNull("utsending"),
        ferdigstilt = row.zonedDateTimeOrNull("ferdigstilt"),
        status = row.string("status").let { Sendingsstatus.valueOf(it) },
        eksternStatus = row.jsonOrNull("eksternStatus", objectMapper),
        bestilling = row.jsonOrNull("bestilling", objectMapper),
        opprettet = row.zonedDateTime("opprettet")
    )
}
