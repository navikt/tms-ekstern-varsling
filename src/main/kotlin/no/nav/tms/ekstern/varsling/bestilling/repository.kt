package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.serialization.json.Json
import kotliquery.Row
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
                    insert into eksterne_varsler(
                        sendingsId,
                        ident,
                        erBatch,
                        erUtsattVarsel,
                        varsler,
                        utsending,
                        kanal,
                        sendt,
                        opprettet
                    ) values (
                        :sendingsId,
                        :ident,
                        :erBatch,
                        :erUtsattVarsel,
                        :varsler,
                        :utsending,
                        :kanal,
                        :sendt,
                        :opprettet
                    )
                """,
                mapOf(
                    "sendingsId" to dbVarsel.sendingsId,
                    "ident" to dbVarsel.ident,
                    "erBatch" to dbVarsel.erBatch,
                    "erUtsattVarsel" to dbVarsel.erUtsattVarsel,
                    "varsler" to dbVarsel.varsler.toJsonb(objectMapper),
                    "utsending" to dbVarsel.utsending,
                    "kanal" to dbVarsel.kanal.name,
                    "sendt" to dbVarsel.sendt,
                    "opprettet" to dbVarsel.opprettet
                )
            )
        }
    }

    fun nextInVarselQueue(batchSize:Int = 20): List<EksternVarsling>{
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
                        sendt,
                        opprettet
                    from 
                        eksterne_varsler
                    where sendt is null and (utsending is null or utsending < :now) 
                    limit :antall
                   """.trimIndent(),
                mapOf("antall" to batchSize, "now" to ZonedDateTime.now())
            ).map {row ->
                EksternVarsling(
                    sendingsId = row.string("sendingsId"),
                    ident = row.string("ident"),
                    erBatch = row.boolean("erBatch"),
                    erUtsattVarsel = row.boolean("erBatch"),
                    varsler = row.json<List<Varsel>>("varsler", objectMapper),
                    utsending = null,
                    kanal = Kanal.valueOf(row.string("kanal")),
                    sendt = row.zonedDateTimeOrNull("sendt"),
                    opprettet = row.zonedDateTime("opprettet"),
                )
            }.asList
        }
    }

    fun markAsSent(sendingsId: String, sendt: ZonedDateTime) {
        database.update {
            queryOf(
                "update eksterne_varsler set sendt = :sendt where sendingsId = :sendingsId",
                mapOf(
                    "sendt" to sendt,
                    "sendingsId" to sendingsId,

                    )
            )
        }
    }

}