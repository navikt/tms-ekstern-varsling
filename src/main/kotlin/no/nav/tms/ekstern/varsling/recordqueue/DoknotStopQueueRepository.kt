package no.nav.tms.ekstern.varsling.recordqueue

import kotliquery.queryOf
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper

class DoknotStopQueueRepository(private val database: PostgresDatabase) {

    fun enqueueDoknotStopp(sendingsId: String) {
        database.update {
            queryOf("""
                insert into doknot_stopp_record_queue(sendingsId, createdAt)
                values(:sendingsId, :createdAt)
            """, mapOf(
                "sendingsId" to sendingsId,
                "createdAt" to ZonedDateTimeHelper.nowAtUtc(),
            ))
        }
    }

    fun doknotStopQueueSize(): Int {
        return database.single {
            queryOf("select count(*) as antall from doknot_stopp_record_queue")
                .map { row -> row.int("antall") }
        }
    }

    fun peekNextDoknotStop(batchSize: Int): List<DoknotStoppDto> {
        return database.list {
            queryOf("""
                select
                    id,
                    sendingsId
                from 
                    doknot_stopp_record_queue
                order by createdAt
                limit :batchSize
            """, mapOf("batchSize" to batchSize)
            ).map { row ->
                DoknotStoppDto(
                    id = row.long("id"),
                    sendingsId = row.string("sendingsId"),
                )
            }
        }
    }

    fun dequeueDoknotStopp(id: Long) {
        database.update {
            queryOf("""
                delete from doknot_stopp_record_queue where id = :entryId
            """, mapOf(
                "entryId" to id
            ))
        }
    }

    data class DoknotStoppDto(
        val id: Long,
        val sendingsId: String
    )
}
