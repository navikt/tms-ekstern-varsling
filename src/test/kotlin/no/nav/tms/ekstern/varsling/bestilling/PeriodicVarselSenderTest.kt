package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.queryOf
import no.nav.doknotifikasjon.schemas.Doknotifikasjon
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.ekstern.varsling.setup.*
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PeriodicVarselSenderTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val repository = EksternVarslingRepository(database)
    private val testFnr = "12345678910"

    private val mockProducer = MockProducer<String, Doknotifikasjon>(
        false,
        StringSerializer(),
        DummySerializer()
    )

    private val kanalDecider = PreferertKanalDecider(
        smsUtsendingStart = LocalTime.MIN,
        smsUtsendingEnd = LocalTime.MAX,
        timezone = ZoneId.of("Europe/Oslo")
    )

    private val leaderElection: PodLeaderElection = mockk()

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from ekstern_varsling")
        }
        mockProducer.clear()
        unmockkObject(LocalTimeHelper)
    }

    @Test
    fun `behandler batch og sender ekstern varsel på kafka`() = runBlocking<Unit> {
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic", leaderElection,
            interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        mockProducer.history().size shouldBe 3
        database.tellAntallSendt() shouldBe 3
    }


    @Test
    fun `behandle kun batch som ikke har blitt behandlet`() = runBlocking<Unit> {
        val tidligereBehandletDato = ZonedDateTimeHelper.nowAtUtc().minusDays(3)
        database.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(),
                testFnr,
                ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(
            eksternVarslingDBRow(
                UUID.randomUUID().toString(), testFnr, ferdigstilt = tidligereBehandletDato
            )
        )
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr))

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        mockProducer.history().size shouldBe 2
        database.tellAntallSendtFørDato(tidligereBehandletDato.plusHours(2)) shouldBe 3
    }

    @Test
    fun `riktig format på utsendt event`() = runBlocking<Unit>{
        val eksternVarslingData = eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr)
        database.insertEksternVarsling(eksternVarslingData)
        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        mockProducer.history().size shouldBe 1

        val doknot = mockProducer.history().first().value()
        val tekster = bestemTekster(eksternVarslingData)
        doknot.bestillingsId shouldBe eksternVarslingData.sendingsId
        doknot.fodselsnummer shouldBe eksternVarslingData.ident
        doknot.prefererteKanaler.first().name shouldBeIn Kanal.entries.map { it.name }
        doknot.smsTekst shouldBe tekster.smsTekst
        doknot.tittel shouldBe tekster.epostTittel
        doknot.epostTekst shouldBe tekster.epostTekst
        doknot.antallRenotifikasjoner shouldBe null
        doknot.renotifikasjonIntervall shouldBe null
        doknot.bestillerId shouldBe "tms-ekstern-varsling"
    }

    @Test
    fun `ignorer batch som kun har inaktive varsler`() = runBlocking<Unit> {

        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = false))))
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = false), createVarsel(aktiv = true))))
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr, varsler = listOf(createVarsel(aktiv = true), createVarsel(aktiv = true))))


        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        mockProducer.history().size shouldBe 2
        database.tellAntallKansellert() shouldBe 1
        database.tellAntallSendt() shouldBe 2
    }

    @Test
    fun `velger riktig kanal basert på preferanser i varsler`() = runBlocking<Unit> {
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.EPOST)), createVarsel(prefererteKanaler = listOf(Kanal.EPOST))))
        )
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.SMS)), createVarsel(prefererteKanaler = listOf(Kanal.EPOST))))
        )
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.SMS)), createVarsel(prefererteKanaler = listOf(Kanal.SMS))))
        )

        database.tellAntallForKanal(null) shouldBe 3

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        database.tellAntallForKanal(Kanal.EPOST) shouldBe 1
        database.tellAntallForKanal(Kanal.SMS) shouldBe 2
    }

    @Test
    fun `inaktiverte varsler påvirker ikke kanal`() = runBlocking<Unit> {
        database.insertEksternVarsling(eksternVarslingDBRow(UUID.randomUUID().toString(), testFnr,
            varsler = listOf(createVarsel(prefererteKanaler = listOf(Kanal.EPOST), aktiv = true), createVarsel(prefererteKanaler = listOf(Kanal.SMS), aktiv = false)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
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

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Innboks)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        repository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.revarsling.shouldNotBeNull()
            it.revarsling!!.antall shouldBe 1
            it.revarsling!!.intervall shouldBe 4
        }

        mockProducer.history().first().value().let {
            it.antallRenotifikasjoner shouldBe 1
            it.renotifikasjonIntervall shouldBe 4
        }
    }

    @Test
    fun `Setter revarsling for oppgave som ikke batches`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(
            eksternVarslingDBRow(
                sendingsId, testFnr,
                varsler = listOf(createVarsel(varseltype = Varseltype.Oppgave))
            )
        )

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        repository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.revarsling.shouldNotBeNull()
            it.revarsling!!.antall shouldBe 1
            it.revarsling!!.intervall shouldBe 7
        }

        mockProducer.history().first().value().let {
            it.antallRenotifikasjoner shouldBe 1
            it.renotifikasjonIntervall shouldBe 7
        }
    }

    @Test
    fun `Setter ikke revarsling for beskjed`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)

        repository.getEksternVarsling(sendingsId).let {
            it.shouldNotBeNull()

            it.revarsling.shouldBeNull()
        }

        mockProducer.history().first().value().let {
            it.antallRenotifikasjoner shouldBe null
            it.renotifikasjonIntervall shouldBe null
        }
    }


    @Test
    fun `Ignorerer varsler som er markert behandlet av tms-ekstern-varselbestiller`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, behandletAvLegacy = true)))
        )

        val periodicVarselSender = PeriodicVarselSender(
            repository, kanalDecider, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = repository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.status shouldBe Sendingsstatus.Kansellert
    }

    @Test
    fun `Velger sms hvis preferert kanal er BETINGET_SMS og sms vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, prefererteKanaler = listOf(Kanal.BETINGET_SMS))))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("13:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            repository, sendSmsDuringDaytime, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = repository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.kanal shouldBe Kanal.SMS
    }

    @Test
    fun `Velger epost hvis preferert kanal er BETINGET_SMS og sms ikke vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, prefererteKanaler = listOf(Kanal.BETINGET_SMS))))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("01:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            repository, sendSmsDuringDaytime, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = repository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.kanal shouldBe Kanal.EPOST
    }

    @Test
    fun `Velger sms hvis preferert kanal er SMS og EPOST og sms vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, prefererteKanaler = listOf(Kanal.SMS, Kanal.EPOST))))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("13:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))
        val periodicVarselSender = PeriodicVarselSender(
            repository, sendSmsDuringDaytime, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = repository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.kanal shouldBe Kanal.SMS
    }

    @Test
    fun `Velger epost hvis preferert kanal er SMS og EPOST og sms ikke vil sendes umiddelbart`() = runBlocking<Unit> {
        val sendingsId = UUID.randomUUID().toString()

        database.insertEksternVarsling(eksternVarslingDBRow(sendingsId, testFnr,
            varsler = listOf(createVarsel(varseltype = Varseltype.Beskjed, prefererteKanaler = listOf(Kanal.SMS, Kanal.EPOST))))
        )

        val smsStart = LocalTime.parse("06:00:00")
        val smsEnd = LocalTime.parse("18:00:00")

        mockkObject(LocalTimeHelper)

        every { LocalTimeHelper.nowAt(any()) } returns LocalTime.parse("01:00:00")

        val sendSmsDuringDaytime = PreferertKanalDecider(smsStart, smsEnd, ZoneId.of("Europe/Oslo"))

        val periodicVarselSender = PeriodicVarselSender(
            repository, sendSmsDuringDaytime, mockProducer, "test-topic",
            leaderElection, interval = Duration.ofMinutes(1)
        )

        coEvery { leaderElection.isLeader() } returns true

        periodicVarselSender.start()
        delay(500)
        val eksternVarsling = repository.getEksternVarsling(sendingsId)

        eksternVarsling.shouldNotBeNull()
        eksternVarsling.kanal shouldBe Kanal.EPOST
    }
}

private fun Database.tellAntallSendt() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Sendt.name)
    ).map { it.int("antall") }.asSingle
}

private fun Database.tellAntallKansellert() = singleOrNull {
    queryOf(
        "select count(*) filter(where status = :status) as antall from ekstern_varsling where ferdigstilt is not Null",
        mapOf("status" to Sendingsstatus.Kansellert.name)
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
                insert into ekstern_varsling(sendingsId, ident, erBatch, erUtsattVarsel, varsler, utsending, kanal, ferdigstilt, opprettet, status, revarsling)
                values (:sendingsId, :ident, :erBatch, :erUtsattVarsel, :varsler, :utsending, :kanal, :ferdigstilt, :opprettet, :status, :revarsling)
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
                "revarsling" to eksternVarsling.revarsling.toJsonb(),
                "opprettet" to eksternVarsling.opprettet,
            )
        )
    }

}
