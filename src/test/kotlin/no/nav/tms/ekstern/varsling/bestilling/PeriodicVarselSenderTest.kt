package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.Database
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.toJsonb
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PeriodicVarselSenderTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val repository = EksternVarselRepository(database)
    private val testFnr = "12345678910"

    private val mockProducer = MockProducer(
        false,
        StringSerializer(),
        StringSerializer()
    )

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from eksterne_varsler")
        }
        mockProducer.clear()
    }

    @Test
    fun `behandler batch og sender ekstern varsel på kafka`() = runBlocking<Unit>{
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(),testFnr))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(),testFnr))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(),testFnr))
        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer)
        periodicVarselSender.start()
        delay(2000)
        mockProducer.history().size shouldBe 3
    }

}

private fun Database.insertEksternVarsling(eksternVarsling: EksternVarsling) {
    update {
        queryOf(
            """
                insert into eksterne_varsler(sendingsId, ident, erBatch, erUtsattVarsel, varsler, utsending, kanal, sendt, opprettet)
                values (:sendingsId, :ident, :erBatch, :erUtsattVarsel, :varsler, :utsending, :kanal, :sendt, :opprettet)
            """,
            mapOf(
                "sendingsId" to eksternVarsling.sendingsId,
                "ident" to eksternVarsling.ident,
                "erBatch" to eksternVarsling.erBatch,
                "erUtsattVarsel" to eksternVarsling.erUtsattVarsel,
                "varsler" to eksternVarsling.varsler.toJsonb(),
                "utsending" to eksternVarsling.utsending,
                "kanal" to eksternVarsling.kanal.name,
                "sendt" to eksternVarsling.sendt,
                "opprettet" to eksternVarsling.opprettet,
            )
        )
    }

}
