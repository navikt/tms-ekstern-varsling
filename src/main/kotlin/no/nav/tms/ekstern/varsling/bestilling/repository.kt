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
                """, mapOf(
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
                    sendt,
                    opprettet
                from 
                    eksterne_varsler
                where
                    ident = :ident and
                    erBatch and
                    sendt is null
                    
        """, mapOf("ident" to ident)
        ).map { it ->
            EksternVarsling(
                sendingsId = it.string("sendingsId"),
                ident = it.string("ident"),
                erBatch = it.boolean("erBatch"),
                erUtsattVarsel = it.boolean("erBatch"),
                varsler = it.json<List<Varsel>>("varsler", objectMapper),
                utsending = it.zonedDateTimeOrNull("utsending"),
                kanal = Kanal.valueOf(it.string("kanal")),
                sendt = it.zonedDateTimeOrNull("sendt"),
                opprettet = it.zonedDateTime("opprettet"),
            )
        }.asSingle
    }

    fun addVarselToExisting(sendingsId: String, varsel: Varsel, kanal: Kanal) {
        database.update {
            queryOf(
                """
                    update eksterne_varsler set kanal = :kanal, varsler = :varsel || varsler where sendingsId = :sendingsId 
                """, mapOf(
                    "sendingsId" to sendingsId,
                    "varsel" to listOf(varsel).toJsonb(objectMapper),
                    "kanal" to kanal.name,
                )
            )
        }
    }

    fun nextInVarselQueue(batchSize: Int = 20): List<EksternVarsling> {
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
                   """.trimIndent(), mapOf("antall" to batchSize, "now" to ZonedDateTime.now())
            ).map { row ->
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
                "update eksterne_varsler set sendt = :sendt where sendingsId = :sendingsId", mapOf(
                    "sendt" to sendt,
                    "sendingsId" to sendingsId,

                    )
            )
        }
    }

}