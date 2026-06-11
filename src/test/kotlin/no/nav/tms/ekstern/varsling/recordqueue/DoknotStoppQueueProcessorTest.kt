package no.nav.tms.ekstern.varsling.recordqueue

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStopp
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.ekstern.varsling.setup.DummySerializer
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID

class DoknotStoppQueueProcessorTest {
    private val database = LocalPostgresDatabase.getCleanInstance()
    private val leaderElection: PodLeaderElection = mockk()

    private val mockProducer = mockProducer()
    private val repository = DoknotStopQueueRepository(database)

    private val testTopic = "doknotstopp-topic"

    @AfterEach
    fun cleanUp() {
        clearMocks(leaderElection)
        mockProducer.clear()
        mockProducer.sendException = null
        mockProducer.flushException = null
        LocalPostgresDatabase.resetInstance()
    }

    @Test
    fun `sender henter records fra kø og sender til kafka synkront`() {
        coEvery { leaderElection.isLeader() } returns true

        val queueProcessor = initProcessor(2)

        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()

        repository.enqueueDoknotStopp(sendingsId1)
        repository.enqueueDoknotStopp(sendingsId2)
        repository.enqueueDoknotStopp(sendingsId3)


        queueProcessor.start()

        runBlocking {
            delayUntilQueueEmpty()
        }

        mockProducer.history().size shouldBe 3
        mockProducer.history()
            .map { it.value() }
            .let {
                it[0].bestillerId shouldBe "tms-ekstern-varsling"
                it[0].bestillingsId shouldBe sendingsId1

                it[1].bestillerId shouldBe "tms-ekstern-varsling"
                it[1].bestillingsId shouldBe sendingsId2

                it[2].bestillerId shouldBe "tms-ekstern-varsling"
                it[2].bestillingsId shouldBe sendingsId3
            }
    }

    @Test
    fun `Hopper over eventer der sending til kafka feilet`() {
        coEvery { leaderElection.isLeader() } returns true

        val kafkaProducer = initProcessor(2, Duration.ofMillis(200))

        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()

        repository.enqueueDoknotStopp(sendingsId1)
        repository.enqueueDoknotStopp(sendingsId2)
        repository.enqueueDoknotStopp(sendingsId3)

        mockProducer.sendException = TimeoutException()

        kafkaProducer.start()

        runBlocking {
            delay(500)
        }

        mockProducer.history().size shouldBe 0

        mockProducer.sendException = null

        runBlocking {
            delayUntilQueueEmpty()
        }

        mockProducer.history().size shouldBe 3
    }


    @Test
    fun `Fortsetter prosessering dersom flush av eventer feilet`() {
        coEvery { leaderElection.isLeader() } returns true

        val kafkaProducer = initProcessor(2, Duration.ofMillis(200))

        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()

        repository.enqueueDoknotStopp(sendingsId1)
        repository.enqueueDoknotStopp(sendingsId2)
        repository.enqueueDoknotStopp(sendingsId3)

        mockProducer.flushException = TimeoutException()

        kafkaProducer.start()

        runBlocking {
            delayUntilQueueEmpty()
        }

        mockProducer.history().size shouldBe 3
    }

    @Test
    @Disabled // Denne testen er svært sårbar for race-conditions og lar seg ofte ikke kjøre sammen med andre tester
    fun `Forsøker på nytt senere dersom event ikke er bekreftet lagt på kafka`() {
        coEvery { leaderElection.isLeader() } returns true

        val manualMockProducer = mockProducer(false)

        val kafkaProducer = initProcessor(
            5,
            interval = Duration.ofMillis(200),
            mockedProducer = manualMockProducer
        )

        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()

        repository.enqueueDoknotStopp(sendingsId1)
        repository.enqueueDoknotStopp(sendingsId2)
        repository.enqueueDoknotStopp(sendingsId3)

        manualMockProducer.flushException = TimeoutException()

        kafkaProducer.start()

        runBlocking {
            delay(300)
        }

        manualMockProducer.completeNext()
        manualMockProducer.errorNext(RuntimeException())
        manualMockProducer.completeNext()

        runBlocking {
            delay(100)
        }

        repository.peekNextDoknotStop(5).let {
            it.size shouldBe 1
            it.first().sendingsId shouldBe sendingsId2
        }

        runBlocking {
            delay(300)
        }

        manualMockProducer.completeNext()

        runBlocking {
            delayUntilQueueEmpty()
        }

        repository.peekNextDoknotStop(1).shouldBeEmpty()

        manualMockProducer.history()
            .map { it.value().bestillingsId }
            .let { values ->
                values.shouldContain(sendingsId1)
                values.shouldContain(sendingsId2)
                values.shouldContain(sendingsId3)
            }
    }

    @Test
    @Disabled // Denne testen er svært sårbar for race-conditions og lar seg ofte ikke kjøre sammen med andre tester
    fun `Forsøker på nytt senere dersom kafka ikke svarer i tide ved synkronisering`() {
        coEvery { leaderElection.isLeader() } returns true

        val manualMockProducer = mockProducer(false)

        val kafkaProducer = initProcessor(
            5,
            interval = Duration.ofMillis(200),
            mockedProducer = manualMockProducer,
            syncTimeoutSeconds = 1
        )

        val sendingsId = UUID.randomUUID().toString()

        repository.enqueueDoknotStopp(sendingsId)

        manualMockProducer.flushException = TimeoutException()

        kafkaProducer.start()

        runBlocking {
            delay(1200)
        }

        repository.peekNextDoknotStop(5).let {
            it.size shouldBe 1
            it.first().sendingsId shouldBe sendingsId
        }

        manualMockProducer.flushException = null

        runBlocking {
            delayUntilQueueEmpty()
        }

        repository.peekNextDoknotStop(1).shouldBeEmpty()

        manualMockProducer.history()
            .map { it.value().bestillingsId }
            .first() shouldContain sendingsId
    }

    private suspend fun delayUntilQueueEmpty() {
        withTimeout(5000) {
            while (repository.doknotStopQueueSize() > 0) {
                delay(100)
            }
        }
    }

    private fun initProcessor(
        batchSize: Int,
        interval: Duration = Duration.ofSeconds(3),
        mockedProducer: Producer<String, DoknotifikasjonStopp> = mockProducer,
        syncTimeoutSeconds: Long = 15
    ) = PeriodicDoknotStoppQueueProcessor(
        repository,
        mockedProducer,
        leaderElection,
        testTopic,
        batchSize,
        syncTimeoutSeconds,
        interval
    )


    private fun mockProducer(autoComplete: Boolean = true) = MockProducer(
        autoComplete,
        null,
        StringSerializer(),
        DummySerializer<DoknotifikasjonStopp>()
    )
}

