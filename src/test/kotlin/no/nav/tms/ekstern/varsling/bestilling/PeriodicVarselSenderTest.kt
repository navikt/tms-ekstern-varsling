package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.queryOf
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.ekstern.varsling.setup.Database
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import no.nav.tms.ekstern.varsling.setup.toJsonb
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.ZonedDateTime
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

    private val leaderElection: PodLeaderElection = mockk()

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from ekstern_varsling")
        }
        mockProducer.clear()
    }

    @Test
    fun `behandler batch og sender ekstern varsel på kafka`() = runBlocking<Unit> {
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(2000)
        mockProducer.history().size shouldBe 3
        database.tellAntallSendt() shouldBe 3
    }


    @Test
    fun `behandle kun batch som ikke har blitt behandlet`() = runBlocking<Unit> {
        val tidligereBehandletDato = ZonedDateTimeHelper.nowAtUtc().minusDays(3)
        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                UUID.randomUUID().toString(),
                testFnr,
                ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(2000)
        mockProducer.history().size shouldBe 2
        database.tellAntallSendtFørDato(tidligereBehandletDato.plusHours(2)) shouldBe 3
    }

    @Test
    fun `riktig format på utsendt event`() = runBlocking<Unit>{
        val eksternVarslingData = createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr)
        database.insertEksternVarsling(eksternVarslingData)
        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        mockProducer.history().size shouldBe 1

        val objectMapper = defaultObjectMapper()
        val jsonTree = mockProducer.history().first().let { objectMapper.readTree(it.value()) }
        val tekster = bestemTekster(eksternVarslingData)
        jsonTree["sendingsId"].asText() shouldBe eksternVarslingData.sendingsId
        jsonTree["ident"].asText() shouldBe eksternVarslingData.ident
        jsonTree["kanal"].asText() shouldBeIn Kanal.entries.map { it.name }
        jsonTree["smsVarslingstekst"].asText() shouldBe tekster.smsTekst
        jsonTree["epostVarslingstittel"].asText() shouldBe tekster.epostTittel
        jsonTree["epostVarslingstekst"].asText() shouldBe tekster.epostTekst
        jsonTree["antallRevarslinger"].asInt() shouldBe 0
        jsonTree["revarslingsIntervall"].asInt() shouldBe 0
        jsonTree["produsent"]["cluster"].asText() shouldBe "todo-gcp"
        jsonTree["produsent"]["appnavn"].asText() shouldBe "tms-ekstern-varsling"
        jsonTree["produsent"]["namespace"].asText() shouldBe "min-side"
    }

    @Test
    fun `ignorer batch som kun har inaktive varsler`() = runBlocking<Unit> {

        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = false))))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = true))))
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = true), createVarsel(aktiv = true))))


        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(2000)
        mockProducer.history().size shouldBe 2
        database.tellAntallFeilet() shouldBe 1
        database.tellAntallSendt() shouldBe 2
    }

    @Test
    fun `velger riktig kanal basert på preferanser i varsler`() = runBlocking<Unit> {
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.EPOST)), createVarsel(prefererteKanaler = listOf(Kanal.EPOST))))
        )
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.SMS)), createVarsel(prefererteKanaler = listOf(Kanal.EPOST))))
        )
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.SMS)), createVarsel(prefererteKanaler = listOf(Kanal.SMS))))
        )

        database.tellAntallForKanal(null) shouldBe 3

        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(2000)
        database.tellAntallForKanal(Kanal.EPOST) shouldBe 1
        database.tellAntallForKanal(Kanal.SMS) shouldBe 2
    }

    @Test
    fun `inaktiverte varsler påvirker ikke kanal`() = runBlocking<Unit> {
        database.insertEksternVarsling(createEksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.EPOST), aktiv = true), createVarsel(prefererteKanaler = listOf(Kanal.SMS), aktiv = false)))
        )

        val periodicVarselSender = PeriodicVarselSender(repository, mockProducer, "test-topic", leaderElection, interval = Duration.ofMinutes(1))

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(2000)
        database.tellAntallForKanal(Kanal.EPOST) shouldBe 1
        database.tellAntallForKanal(Kanal.SMS) shouldBe 0
    }
}

private fun Database.tellAntallSendt() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Sendt.name)
    ).map { it.int("antall") }.asSingle
}

private fun Database.tellAntallFeilet() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Kanselert.name)
    ).map { it.int("antall") }.asSingle
}

private fun Database.tellAntallSendtFørDato(sendtEtterDato: ZonedDateTime) = singleOrNull {
    queryOf(
        "select count(*) as antall from ekstern_varsling where ferdigstilt < :sendtEtterDato",
        mapOf("sendtEtterDato" to sendtEtterDato)
    ).map { it.int("antall") }.asSingle
}

private fun Database.tellAntallForKanal(kanal: Kanal?) = singleOrNull {
    if (kanal != null) {
        queryOf(
            "select count(*) as antall from ekstern_varsling where kanal = :kanal",
            mapOf("kanal" to kanal.name)
        )
    } else {
        queryOf(
            "select count(*) as antall from ekstern_varsling where kanal is null"
        )
    }.map { it.int("antall") }.asSingle

}

fun Database.insertEksternVarsling(eksternVarsling: EksternVarsling) {
    update {
        queryOf(
            """
                insert into ekstern_varsling(sendingsId, ident, erBatch, erUtsattVarsel, varsler, utsending, kanal, ferdigstilt, opprettet, status)
                values (:sendingsId, :ident, :erBatch, :erUtsattVarsel, :varsler, :utsending, :kanal, :ferdigstilt, :opprettet, :status)
            """,
            mapOf(
                "sendingsId" to eksternVarsling.sendingsId,
                "ident" to eksternVarsling.ident,
                "erBatch" to eksternVarsling.erBatch,
                "erUtsattVarsel" to eksternVarsling.erUtsattVarsel,
                "varsler" to eksternVarsling.varsler.toJsonb(),
                "utsending" to eksternVarsling.utsending,
                "kanal" to eksternVarsling.kanal?.name,
                "ferdigstilt" to eksternVarsling.ferdigstilt,
                "status" to eksternVarsling.status.name,
                "opprettet" to eksternVarsling.opprettet,
            )
        )
    }

}
