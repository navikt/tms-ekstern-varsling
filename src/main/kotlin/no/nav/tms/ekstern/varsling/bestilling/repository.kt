package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.Database
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import no.nav.tms.ekstern.varsling.setup.json
import no.nav.tms.ekstern.varsling.setup.toJsonb
import java.time.ZonedDateTime

class EksternVarselRepository(val database: Database) {

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
                        kanal,
                        ferdigstilt,
                        status,
                        opprettet
                    ) values (
                        :sendingsId,
                        :ident,
                        :erBatch,
                        :erUtsattVarsel,
                        :varsler,
                        :utsending,
                        :kanal,
                        :ferdigstilt,
                        :status,
                        :opprettet
                    )
                """, mapOf(
                    "sendingsId" to dbVarsel.sendingsId,
                    "ident" to dbVarsel.ident,
                    "erBatch" to dbVarsel.erBatch,
                    "erUtsattVarsel" to dbVarsel.erUtsattVarsel,
                    "varsler" to dbVarsel.varsler.toJsonb(objectMapper),
                    "utsending" to dbVarsel.utsending,
                    "kanal" to dbVarsel.kanal.name,
                    "ferdigstilt" to dbVarsel.ferdigstilt,
                    "opprettet" to dbVarsel.opprettet,
                    "status" to dbVarsel.status.name
                )
            )
        }
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
                    kanal,
                    ferdigstilt,
                    opprettet,
                    status
                from 
                    ekstern_varsling
                where
                    ident = :ident and
                    erBatch and
                    not erUtsattVarsel and
                    ferdigstilt is null
                    
        """, mapOf("ident" to ident)
        ).map { it ->
            EksternVarsling(
                sendingsId = it.string("sendingsId"),
                ident = it.string("ident"),
                erBatch = it.boolean("erBatch"),
                erUtsattVarsel = it.boolean("erUtsattVarsel"),
                varsler = it.json<List<Varsel>>("varsler", objectMapper),
                utsending = it.zonedDateTimeOrNull("utsending"),
                kanal = Kanal.valueOf(it.string("kanal")),
                ferdigstilt = it.zonedDateTimeOrNull("ferdigstilt"),
                opprettet = it.zonedDateTime("opprettet"),
                status = it.string("status").let { Sendingsstatus.valueOf(it) }
            )
        }.asSingle
    }

    fun addVarselToExisting(sendingsId: String, varsel: Varsel, kanal: Kanal) {
        database.update {
            queryOf(
                """
                    update ekstern_varsling set kanal = :kanal, varsler = :varsel || varsler where sendingsId = :sendingsId 
                """, mapOf(
                    "sendingsId" to sendingsId,
                    "varsel" to listOf(varsel).toJsonb(objectMapper),
                    "kanal" to kanal.name,
                )
            )
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
                        kanal,
                        ferdigstilt,
                        status,
                        opprettet
                    from 
                        ekstern_varsling
                    where ferdigstilt is null and (utsending is null or utsending < :now)
                    limit :antall
                   """.trimIndent(), mapOf("antall" to batchSize, "now" to ZonedDateTimeHelper.nowAtUtc())
            ).map { row ->
                EksternVarsling(
                    sendingsId = row.string("sendingsId"),
                    ident = row.string("ident"),
                    erBatch = row.boolean("erBatch"),
                    erUtsattVarsel = row.boolean("erBatch"),
                    varsler = row.json<List<Varsel>>("varsler", objectMapper),
                    utsending = null,
                    kanal = Kanal.valueOf(row.string("kanal")),
                    ferdigstilt = row.zonedDateTimeOrNull("ferdigstilt"),
                    opprettet = row.zonedDateTime("opprettet"),
                    status = row.string("status").let { Sendingsstatus.valueOf(it) }
                )
            }.asList
        }
    }

    fun markAsSent(sendingsId: String, ferdigstilt: ZonedDateTime) {
        database.update {
            queryOf(
                "update ekstern_varsling set ferdigstilt = :ferdigstilt, status = :status where sendingsId = :sendingsId", mapOf(
                    "ferdigstilt" to ferdigstilt,
                    "sendingsId" to sendingsId,
                    "status" to Sendingsstatus.Sendt.name
                    )
            )
        }
    }

    private fun String.toParam() = listOf(mapOf("varselId" to this)).toJsonb(objectMapper)
}
