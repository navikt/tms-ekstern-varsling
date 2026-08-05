package no.nav.tms.ekstern.varsling.arkiv

import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.JsonbHelper.jsonOrNull
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.bestilling.Bestilling
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus
import no.nav.tms.ekstern.varsling.bestilling.Sendingsstatus
import no.nav.tms.ekstern.varsling.bestilling.Varsel
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper
import no.nav.tms.ekstern.varsling.common.enum
import java.time.ZonedDateTime
import kotlin.collections.map

class ArkivRepository(private val database: PostgresDatabase) {

    fun archiveEntriesByThresholds(
        opprettetThreshold: ZonedDateTime,
        ferdigstiltThreshold: ZonedDateTime,
        limit: Int
    ): List<ArkivertVarsling> {

        val arkivVarsler = getEksternVarslingForArchival(
            opprettetThreshold = opprettetThreshold,
            ferdigstiltThreshold = ferdigstiltThreshold,
            limit = limit
        )

        if (arkivVarsler.isNotEmpty()) {
            insertArkiverteVarsler(arkivVarsler)
            deleteEksternVarsling(arkivVarsler.map { it.sendingsId })
        }

        return arkivVarsler
    }

    private fun getEksternVarslingForArchival(
        opprettetThreshold: ZonedDateTime,
        ferdigstiltThreshold: ZonedDateTime,
        limit: Int
    ): List<ArkivertVarsling> {
        return database.list {
            queryOf(
                """
                    select 
                        *,
                        ferdigstilt < :ferdigstiltThreshold as ferdigstilt_threshold_passed
                    from 
                        ekstern_varsling 
                    where
                        opprettet < :opprettetThreshold or
                        ferdigstilt < :ferdigstiltThreshold
                    limit :limit
                """,
                mapOf(
                    "opprettetThreshold" to opprettetThreshold,
                    "ferdigstiltThreshold" to ferdigstiltThreshold,
                    "limit" to limit
                )
            ).map(::toArkivertVarsling)
        }
    }

    private fun insertArkiverteVarsler(varsler: List<ArkivertVarsling>) {
        database.batchUpdate(
            """
                insert into ekstern_varsling_arkiv(
                    sendingsId,
                    varselIds,
                    ident,
                    varsling,
                    opprettet,
                    ferdigstilt,
                    arkivert,
                    begrunnelse
                )
                values(
                    :sendingsId,
                    :varselIds,
                    :ident,
                    :varsling,
                    :opprettet,
                    :ferdigstilt,
                    :arkivert,
                    :begrunnelse
                )
            """,
            varsler.map {
                val serializedData = it.serializedData
                val varselIds = serializedData.varsler.map(Varsel::varselId)

                mapOf(
                    "sendingsId" to it.sendingsId,
                    "ident" to it.ident,
                    "varselIds" to varselIds.toJsonb(),
                    "varsling" to serializedData.toJsonb(),
                    "opprettet" to it.opprettet,
                    "ferdigstilt" to it.ferdigstilt,
                    "arkivert" to ZonedDateTimeHelper.nowAtUtc(),
                    "begrunnelse" to it.begrunnelse.name
                )
            }
        )
    }

    private fun deleteEksternVarsling(sendingsIds: List<String>) {

        database.update {
            val sendingsIdArray = it.createArrayOf("TEXT", sendingsIds)

            queryOf(
                "delete from ekstern_varsling where sendingsId = any(:sendingsIds)",
                mapOf("sendingsIds" to sendingsIdArray)
            )
        }
    }

    private fun toArkivertVarsling(row: Row) =
        ArkivertVarsling(
            sendingsId = row.string("sendingsId"),
            ident = row.string("ident"),
            serializedData = ArkivertVarsling.SerializedData(
                varsler = row.json("varsler"),
                erBatch = row.boolean("erBatch"),
                erUtsattVarsel = row.boolean("erUtsattVarsel"),
                utsending = row.zonedDateTimeOrNull("utsending"),
                status = row.enum("status"),
                bestilling = row.jsonOrNull("bestilling"),
                eksternStatus = row.jsonOrNull("eksternStatus")
            ),
            opprettet = row.zonedDateTime("opprettet"),
            ferdigstilt = row.zonedDateTimeOrNull("ferdigstilt"),
            begrunnelse = if (row.boolean("ferdigstilt_threshold_passed")) {
                ArkivertVarsling.ArkiveringsBegrunnelse.VarslingFerdigstilt
            } else {
                ArkivertVarsling.ArkiveringsBegrunnelse.BestillingForeldet
            }
        )
}

data class ArkivertVarsling(
    val sendingsId: String,
    val ident: String,
    val opprettet: ZonedDateTime,
    val ferdigstilt: ZonedDateTime?,
    val serializedData: SerializedData,
    val begrunnelse: ArkiveringsBegrunnelse
) {
    data class SerializedData(
        val varsler: List<Varsel>,
        val erBatch: Boolean,
        val erUtsattVarsel: Boolean,
        val utsending: ZonedDateTime?,
        val status: Sendingsstatus,
        val bestilling: Bestilling?,
        val eksternStatus: EksternStatus.Oversikt?
    )

    enum class ArkiveringsBegrunnelse {
        BestillingForeldet, VarslingFerdigstilt
    }
}
