package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.queryOf
import no.nav.doknotifikasjon.schemas.Doknotifikasjon
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.EksternVarsling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Produsent
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.Varsel
import no.nav.tms.ekstern.varsling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.defaultObjectMapper
import no.nav.tms.ekstern.varsling.insertEksternVarslingWithLegacyVarsel
import no.nav.tms.ekstern.varsling.recordqueue.StatusOppdatertQueueRepository
import no.nav.tms.ekstern.varsling.setup.*
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.ekstern.varsling.utsending.EksternVarslingUtsendingRepository
import no.nav.tms.ekstern.varsling.utsending.PeriodicVarselSender
import no.nav.tms.ekstern.varsling.utsending.PreferertKanalDecider
import no.nav.tms.ekstern.varsling.utsending.bestemTekster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PeriodicVarselSenderTest {
    private val database = LocalPostgresDatabase.getCleanInstance()
    private val testRepository = EksternVarslingBestillingRepository(database)
    private val utsendingRepository = EksternVarslingUtsendingRepository(database)
    private val testFnr = "12345678910"

    private val doknotTopic = MockProducer<String, Doknotifikasjon>(
        true,
        StringSerializer(),
        DummySerializer()
    )

    private val queueRepository = StatusOppdatertQueueRepository(database)
    private val statusProducer = EksternVarslingOppdatertProducer(
        queueRepository
    )

    private val kanalDecider = PreferertKanalDecider(
        smsUtsendingStart = LocalTime.MIN,
        smsUtsendingEnd = LocalTime.MAX,
        timezone = ZoneId.of("Europe/Oslo")
    )

    private val leaderElection: PodLeaderElection = mockk()

    @AfterEach
    fun cleanup() {
        LocalPostgresDatabase.resetInstance()
        doknotTopic.clear()
        unmockkObject(LocalTimeHelper)
    }

    @Test
    fun `behandler batch og sender ekstern varsel på kafka`() = runBlocking<Unit> {
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        doknotTopic.history().size shouldBe 3
        database.tellAntallSendt() shouldBe 3
    }


    @Test
    fun `behandle kun batch som ikke har blitt behandlet`() = runBlocking<Unit> {
        val tidligereBehandletDato = nowAtUtc().minusDays(3)
        testRepository.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(),
                testFnr,
                ferdigstilt = tidligereBehandletDato
            )
        )
        testRepository.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        testRepository.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        doknotTopic.history().size shouldBe 2
        database.tellAntallSendtFørDato(tidligereBehandletDato.plusHours(2)) shouldBe 3
    }

    @Test
    fun `riktig format på utsendt event`() = runBlocking<Unit>{
        val eksternVarslingData = eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr)
        testRepository.insertEksternVarsling(eksternVarslingData)
        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        doknotTopic.history().size shouldBe 1

        val doknot = doknotTopic.history().first().value()
        val tekster = bestemTekster(eksternVarslingData)
        doknot.bestillingsId shouldBe eksternVarslingData.sendingsId
        doknot.fodselsnummer shouldBe eksternVarslingData.ident
        doknot.prefererteKanaler.first().name shouldBeIn Kanal.entries.map { it.name }
        doknot.smsTekst shouldBe tekster.smsTekst
        doknot.tittel shouldBe tekster.epostTittel
        doknot.epostTekst shouldBe tekster.epostTekst
        doknot.antallRenotifikasjoner shouldBe 0
        doknot.renotifikasjonIntervall shouldBe 0
        doknot.bestillerId shouldBe "tms-ekstern-varsling"
    }

    @Test
    fun `ignorer batch som kun har inaktive varsler`() = runBlocking<Unit> {

        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = false))))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = true))))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = true), createVarsel(aktiv = true))))


        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        doknotTopic.history().size shouldBe 2
        database.tellAntallKansellert() shouldBe 1
        database.tellAntallSendt() shouldBe 2
    }

    @Test
    fun `sender status 'kansellert' for avbrutte sendinger`() = runBlocking<Unit> {
        val varselId1 = UUID.randomUUID().toString()
        val varselId2 = UUID.randomUUID().toString()
        val varselId3 = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(varselId = varselId1, aktiv = true))))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(varselId = varselId2, aktiv = false), createVarsel(varselId = varselId3, aktiv = false))))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        val objectMapper = defaultObjectMapper()

        queueRepository.peekStatusOppdatert(10)
            .map { objectMapper.readTree(it.statusinnhold) }
            .let { statusEvents ->
                statusEvents.firstOrNull { it["varselId"].asText() == varselId1 }.shouldBeNull()

                statusEvents.firstOrNull { it["varselId"].asText() == varselId2 }.let {
                    it.shouldNotBeNull()

                    it["status"].asText() shouldBe "kansellert"
                }

                statusEvents.firstOrNull { it["varselId"].asText() == varselId3 }.let {
                    it.shouldNotBeNull()

                    it["status"].asText() shouldBe "kansellert"
                }
            }
    }

    @Test
    fun `velger riktig kanal basert på preferanser i varsler`() = runBlocking<Unit> {
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(preferertKanal = Kanal.EPOST), createVarsel(preferertKanal = Kanal.EPOST)))
        )
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(preferertKanal = Kanal.SMS), createVarsel(preferertKanal = Kanal.EPOST)))
        )
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(preferertKanal = Kanal.SMS), createVarsel(preferertKanal = Kanal.SMS)))
        )

        database.tellAntallForKanal(null) shouldBe 3

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        database.tellAntallForKanal(Kanal.EPOST) shouldBe 1
        database.tellAntallForKanal(Kanal.SMS) shouldBe 2
    }

    @Test
    fun `inaktiverte varsler påvirker ikke kanal`() = runBlocking<Unit> {
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(preferertKanal = Kanal.EPOST, aktiv = true), createVarsel(preferertKanal = Kanal.SMS, aktiv = false)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        database.tellAntallForKanal(Kanal.EPOST) shouldBe 1
        database.tellAntallForKanal(Kanal.SMS) shouldBe 0
    }

    @Test
    fun `Setter revarsling for innboks som ikke batches`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Innboks)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        testRepository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.bestilling?.revarsling.shouldNotBeNull()
            it.bestilling.revarsling.antall shouldBe 1
            it.bestilling.revarsling.intervall shouldBe 4
        }

        doknotTopic.history().first().value().let {
            it.antallRenotifikasjoner shouldBe 1
            it.renotifikasjonIntervall shouldBe 4
        }
    }

    @Test
    fun `Setter revarsling for oppgave som ikke batches`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(
            eksternVarslingDBRow(
                sendingsId, testFnr,
                varsler = listOf(createVarsel(varseltype = Varseltype.Oppgave))
            )
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        testRepository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.bestilling?.revarsling.shouldNotBeNull()
            it.bestilling.revarsling.antall shouldBe 1
            it.bestilling.revarsling.intervall shouldBe 7
        }

        doknotTopic.history().first().value().let {
            it.antallRenotifikasjoner shouldBe 1
            it.renotifikasjonIntervall shouldBe 7
        }
    }

    @Test
    fun `Setter ikke revarsling for beskjed`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        testRepository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.bestilling?.revarsling.shouldBeNull()
        }

        doknotTopic.history().first().value().let {
            it.antallRenotifikasjoner shouldBe 0
            it.renotifikasjonIntervall shouldBe 0
        }
    }

    @Test
    fun `Velger sms hvis preferert kanal er BETINGET_SMS og sms vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, preferertKanal = Kanal.BETINGET_SMS)))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("13:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, sendSmsDuringDaytime, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = testRepository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.bestilling?.preferertKanal shouldBe Kanal.SMS
    }

    @Test
    fun `Velger epost hvis preferert kanal er BETINGET_SMS og sms ikke vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, preferertKanal = Kanal.BETINGET_SMS)))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("01:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, sendSmsDuringDaytime, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = testRepository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.bestilling?.preferertKanal shouldBe Kanal.EPOST
    }

    @Test
    fun `Velger sms hvis preferert kanal er SMS og EPOST og sms kan sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(
                createVarsel(varseltype = Varseltype.Beskjed)
                    .copy(
                        prefererteKanaler = listOf(Kanal.SMS, Kanal.EPOST)
                    )
            ))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("13:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))
        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, sendSmsDuringDaytime, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = testRepository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.bestilling?.preferertKanal shouldBe Kanal.SMS
    }

    @Test
    fun `Velger epost hvis preferert kanal er SMS og EPOST og sms ikke vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(
                createVarsel(varseltype = Varseltype.Beskjed, preferertKanal = Kanal.SMS),
                createVarsel(varseltype = Varseltype.Beskjed, preferertKanal = Kanal.EPOST)
            ))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("01:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, sendSmsDuringDaytime, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = testRepository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.bestilling?.preferertKanal shouldBe Kanal.EPOST
    }

    @Test
    fun `Lagrer info om hvilke tekster som ble spesifisert ved bestilling`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            erBatch = true,
            varsler = listOf(
                createVarsel(varseltype = Varseltype.Beskjed),
                createVarsel(varseltype = Varseltype.Beskjed),
                createVarsel(varseltype = Varseltype.Innboks)
            ))
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = testRepository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.bestilling?.tekster shouldBe bestemTekster(eksternVarsling)
    }

    @Test
    fun `Behandler bestillinger i den rekkefølgen de ble opprettet`() = runBlocking<Unit> {
        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId1, testFnr, opprettet = nowAtUtc()))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId2, testFnr, opprettet = nowAtUtc().minusMinutes(5)))
        testRepository.insertEksternVarsling(eksternVarslingDBRow(sendingsId3, testFnr, opprettet = nowAtUtc().plusMinutes(5)))

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1),
            batchSize = 1
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        testRepository.getEksternVarsling(sendingsId1)?.status shouldBe Sendingsstatus.Venter
        testRepository.getEksternVarsling(sendingsId2)?.status shouldBe Sendingsstatus.Sendt
        testRepository.getEksternVarsling(sendingsId3)?.status shouldBe Sendingsstatus.Venter
    }

    @Test
    fun `prøver igjen senere dersom sending til kafka feiler med RetriableSendException`() = runBlocking<Unit> {
        testRepository.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val failingProducer = MockProducer<String, Doknotifikasjon>(
            false,
            StringSerializer(),
            DummySerializer()
        )

        failingProducer.sendException = TimeoutException()

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, failingProducer, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMillis(200)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        failingProducer.history().size shouldBe 0
        database.tellAntallSendt() shouldBe 0

        failingProducer.sendException = null

        delay(250)
        failingProducer.completeNext()
        delay(250)

        failingProducer.history().size shouldBe 1
        database.tellAntallSendt() shouldBe 1
    }

    @Test
    fun `håndterer at varsler kan komme fra jsonb-kolonne og egen tabell`() = runBlocking<Unit> {
        val legacyVarsel1 = varsel(
            varseltype = Varseltype.Oppgave,
            legacy = true
        )
        val legacyVarsel2 = varsel(
            varseltype = Varseltype.Beskjed,
            legacy = true
        )
        val varsel1 = varsel(
            varseltype = Varseltype.Oppgave,
            legacy = false
        )

        database.insertEksternVarslingWithLegacyVarsel(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(),
                testFnr,
                varsler = listOf(
                    legacyVarsel1,
                    legacyVarsel2,
                    varsel1
                )
            )
        )

        val periodicVarselSender = PeriodicVarselSender(
            utsendingRepository, kanalDecider, doknotTopic, statusProducer,
            "test-topic", leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        doknotTopic.history().size shouldBe 1
        doknotTopic.history().first().let {
            it.value().smsTekst shouldContain "2 oppgave"
            it.value().smsTekst shouldContain "1 beskjed"
        }
    }
}

private fun PostgresDatabase.tellAntallSendt() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Sendt.name)
    ).map { it.int("antall") }
}

private fun PostgresDatabase.tellAntallKansellert() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Kansellert.name)
    ).map { it.int("antall") }
}

private fun PostgresDatabase.tellAntallSendtFørDato(sendtEtterDato: ZonedDateTime) = singleOrNull {
    queryOf(
        "select count(*) as antall from ekstern_varsling where ferdigstilt < :sendtEtterDato",
        mapOf("sendtEtterDato" to sendtEtterDato)
    ).map { it.int("antall") }
}

private fun PostgresDatabase.tellAntallForKanal(kanal: Kanal?) = singleOrNull {
    if (kanal != null) {
        queryOf(
            "select count(*) as antall from ekstern_varsling where bestilling->>'preferertKanal' = :kanal",
            mapOf("kanal" to kanal.name)
        )
    } else {
        queryOf(
            "select count(*) as antall from ekstern_varsling where bestilling->>'preferertKanal' is null"
        )
    }.map { it.int("antall") }

}

private fun varsel(
    varseltype: Varseltype,
    legacy: Boolean
) = Varsel(
    varselId = UUID.randomUUID().toString(),
    varseltype = varseltype,
    preferertKanal = null,
    smsVarslingstekst = null,
    epostVarslingstittel = null,
    epostVarslingstekst = null,
    produsent = Produsent("cluster", "namespace", "appnavn"),
    aktiv = true,
    opprettet = nowAtUtc(),
    inaktivert = null,
    legacyJsonb = legacy,
    prefererteKanaler = emptyList(),
)
