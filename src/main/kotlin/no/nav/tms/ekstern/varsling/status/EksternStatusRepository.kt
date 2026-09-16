package no.nav.tms.ekstern.varsling.status

import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.EksternStatus
import no.nav.tms.ekstern.varsling.Produsent
import no.nav.tms.ekstern.varsling.Varseltype
import no.nav.tms.ekstern.varsling.common.enum

class EksternStatusRepository(val database: PostgresDatabase) {

    fun getEksternStatus(sendingsId: String): EksternStatusSummary? {
        return database.singleOrNull {
            queryOf(
                """
                select 
                    sendingsId,
                    ident,
                    eksternStatus
                from 
                    ekstern_varsling
                where
                    sendingsId = :sendingsId
            """,
                mapOf("sendingsId" to sendingsId)
            ).map {
                mapStatusSummary(it)
            }
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

    fun getAffectedVarsler(sendingsId: String): List<VarselSummary> {
        val legacy: List<VarselSummary> = database.single {
            queryOf(
                "select varsler from ekstern_varsling where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId)
            ).map {
                it.json("varsler")
            }
        }

        val varsler = database.list {
            queryOf(
                """
                    select
                        varselId,
                        varseltype,
                        produsent
                    from varsel
                        where sendingsId = :sendingsId
                """,
                mapOf("sendingsId" to sendingsId)
            ).map {
                VarselSummary(
                    varselId = it.string("varselId"),
                    varseltype = it.enum("varseltype"),
                    produsent = it.json("produsent")
                )
            }
        }

        return legacy + varsler
    }

    private fun mapStatusSummary(row: Row) = EksternStatusSummary(
        sendingsId = row.string("sendingsId"),
        ident = row.string("ident"),
        eksternStatus = row.jsonOrNull("eksternStatus")
    )

    data class EksternStatusSummary(
        val sendingsId: String,
        val ident: String,
        val eksternStatus: EksternStatus.Oversikt?
    )

    data class VarselSummary(
        val varselId: String,
        val varseltype: Varseltype,
        val produsent: Produsent
    )
}
