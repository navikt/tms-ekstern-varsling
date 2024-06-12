package no.nav.tms.ekstern.varsling.bestilling

import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.database.Database
import no.nav.tms.ekstern.varsling.setup.database.defaultObjectMapper
import no.nav.tms.ekstern.varsling.setup.database.toJsonb

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
}