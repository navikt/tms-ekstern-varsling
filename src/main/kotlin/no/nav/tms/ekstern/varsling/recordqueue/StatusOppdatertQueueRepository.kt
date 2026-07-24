package no.nav.tms.ekstern.varsling.recordqueue

import kotliquery.queryOf
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper

class StatusOppdatertQueueRepository(private val database: PostgresDatabase) {

    fun enqueueStatusOppdatert(varselId: String, statusnavn: String, statusinnhold: String) {
        database.update {
            queryOf("""
                insert into status_oppdatert_record_queue(varselId, statusnavn, statusinnhold, createdAt)
                values(:varselId, :statusnavn, :statusinnhold, :createdAt)
            """, mapOf(
                "varselId" to varselId,
                "statusnavn" to statusnavn,
                "statusinnhold" to statusinnhold,
                "createdAt" to ZonedDateTimeHelper.nowAtUtc(),
            ))
        }
    }

    fun dequeueStatusOppdatert(id: Long) {
        database.update {
            queryOf("""
                delete from status_oppdatert_record_queue where id = :entryId
            """, mapOf(
                "entryId" to id
            ))
        }
    }

    fun peekStatusOppdatert(batchSize: Int): List<StatusOppdatertDto> {
        return database.list {
            queryOf("""
                select
                    id,
                    varselId,
                    statusnavn,
                    statusinnhold
                from 
                    status_oppdatert_record_queue
                order by createdAt
                limit :batchSize
            """, mapOf("batchSize" to batchSize)
            ).map { row ->
                StatusOppdatertDto(
                    id = row.long("id"),
                    varselId = row.string("varselId"),
                    statusnavn = row.string("statusnavn"),
                    statusinnhold = row.string("statusinnhold")
                )
            }
        }
    }

    fun statusOppdatertQueueSize(): Int {
        return database.single {
            queryOf("select count(*) as antall from status_oppdatert_record_queue")
                .map { row -> row.int("antall") }
        }
    }

    data class StatusOppdatertDto(
        val id: Long,
        val varselId: String,
        val statusnavn: String,
        val statusinnhold: String
    )
}
