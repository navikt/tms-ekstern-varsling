package no.nav.tms.ekstern.varsling.arkiv

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotliquery.Row
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.arkiv.ArkivertVarsling.ArkiveringsBegrunnelse.BestillingForeldet
import no.nav.tms.ekstern.varsling.arkiv.ArkivertVarsling.ArkiveringsBegrunnelse.VarslingFerdigstilt
import no.nav.tms.ekstern.varsling.Bestilling
import no.nav.tms.ekstern.varsling.EksternStatus
import no.nav.tms.ekstern.varsling.EksternVarsling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Produsent
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.Tekster
import no.nav.tms.ekstern.varsling.Varsel
import no.nav.tms.ekstern.varsling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.common.enum
import no.nav.tms.ekstern.varsling.common.updateInTx
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.TestRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.String
import kotlin.math.absoluteValue

internal class PeriodicArchiverTest {

    private val database = LocalPostgresDatabase.getCleanInstance()
    private val archiveRepository = ArkivRepository(database)
    private val leaderElection: PodLeaderElection = mockk()

    private val testRepository = TestRepository(database)
    private val arkivTestRepository = ArkivTestRepository(database)

    private val testIdent = "01234567890"

    private val gammelUbehandeltVarsling =
        varsling(sendingsId = "s-1", opprettet = nowAtUtc().minusDays(25), ferdigstilt = null)
    private val gammelFerdigstiltVarsling =
        varsling(sendingsId = "s-2", opprettet = nowAtUtc().minusDays(25), ferdigstilt = nowAtUtc().minusDays(10))
    private val nyUbehandletVarsling =
        varsling(sendingsId = "s-3", opprettet = nowAtUtc().minusDays(5), ferdigstilt = null)
    private val nyFerdigstiltVarsling =
        varsling(sendingsId = "s-4", opprettet = nowAtUtc().minusDays(5), ferdigstilt = nowAtUtc().minusDays(5))

    @BeforeEach
    fun setup() {
        createVarsling(gammelUbehandeltVarsling, gammelFerdigstiltVarsling, nyUbehandletVarsling, nyFerdigstiltVarsling)
    }

    @AfterEach
    fun cleanUp() {
        clearMocks(leaderElection)
        LocalPostgresDatabase.resetInstance()
    }


    private fun createVarsling(vararg varsler: EksternVarsling) {
        varsler.forEach { testRepository.insertEksternVarsling(it) }
    }

    @Test
    fun `arkiverer varslinger ferdigstilt før gitt grense`() {

        coEvery { leaderElection.isLeader() } returns true

        val opprettetAgeTreshold = 30L
        val ferdigstiltAgeThreshold = 3L

        runArchiverToCompletion(
            opprettetThreshold = opprettetAgeTreshold,
            ferdigstiltThreshold = ferdigstiltAgeThreshold
        )

        val arkiverteVarsler = arkivTestRepository.getAllArchivedVarsel()
        arkiverteVarsler.size shouldBe 2
        arkiverteVarsler.forEach {
            it.ferdigstilt.shouldNotBeNull()
            daysBetween(nowAtUtc(), it.ferdigstilt) shouldBeGreaterThan ferdigstiltAgeThreshold
            it.begrunnelse shouldBe VarslingFerdigstilt
        }
    }

    @Test
    fun `arkiverer alle varslinger opprette før gitt grense`() {

        coEvery { leaderElection.isLeader() } returns true

        val opprettetAgeTreshold = 20L
        val ferdigstiltAgeThreshold = 15L

        runArchiverToCompletion(
            opprettetThreshold = opprettetAgeTreshold,
            ferdigstiltThreshold = ferdigstiltAgeThreshold
        )

        val arkiverteVarsler = arkivTestRepository.getAllArchivedVarsel()
        arkiverteVarsler.size shouldBe 2
        arkiverteVarsler.forEach {
            daysBetween(nowAtUtc(), it.opprettet) shouldBeGreaterThan opprettetAgeTreshold
            it.begrunnelse shouldBe BestillingForeldet
        }
    }

    @Test
    fun `VarslingFerdigstilt tar presedens som begrunnelse der begge grenser er oversteget`() {

        coEvery { leaderElection.isLeader() } returns true

        val opprettetAgeTreshold = 0L
        val ferdigstiltAgeThreshold = 0L

        runArchiverToCompletion(
            opprettetThreshold = opprettetAgeTreshold,
            ferdigstiltThreshold = ferdigstiltAgeThreshold
        )

        val arkiverteVarsler = arkivTestRepository.getAllArchivedVarsel()
        arkiverteVarsler.size shouldBe 4
        arkiverteVarsler.forEach {
            if (it.ferdigstilt != null) {
                it.begrunnelse shouldBe VarslingFerdigstilt
            } else {
                it.begrunnelse shouldBe BestillingForeldet
            }
        }
    }

    @Test
    fun `arkiverer data om varsling`() = runBlocking<Unit> {

        coEvery { leaderElection.isLeader() } returns true

        runArchiverToCompletion(
            opprettetThreshold = 30,
            ferdigstiltThreshold = 8
        )

        val arkiverteVarsler = arkivTestRepository.getAllArchivedVarsel()

        arkiverteVarsler.size shouldBe 1
        arkiverteVarsler.first().apply {
            sendingsId shouldBe gammelFerdigstiltVarsling.sendingsId
            ident shouldBe gammelFerdigstiltVarsling.ident
            opprettet shouldBeSameInstantAs gammelFerdigstiltVarsling.opprettet
            ferdigstilt shouldBeSameInstantAs gammelFerdigstiltVarsling.ferdigstilt
            serializedData.apply {
                varsler shouldBe gammelFerdigstiltVarsling.varsler
                erBatch shouldBe gammelFerdigstiltVarsling.erBatch
                erUtsattVarsel shouldBe gammelFerdigstiltVarsling.erUtsattVarsel
                utsending shouldBe gammelFerdigstiltVarsling.utsending
                status shouldBe gammelFerdigstiltVarsling.status
                bestilling shouldBe gammelFerdigstiltVarsling.bestilling
                eksternStatus shouldBe gammelFerdigstiltVarsling.eksternStatus
            }
        }
    }

    @Test
    fun `Kaster exception og ruller tilbake insert i ekstern_varsling_arkiv hvis delete fra ekstern_varsling feiler`() {
        mockkStatic("no.nav.tms.ekstern.varsling.common.DbTransactionsKt") {
            every { any<TransactionalSession>().updateInTx(any()) } throws RuntimeException("simulert feil ved delete")
            shouldThrow<Exception> {
                archiveRepository.archiveEntriesByThresholds(
                    opprettetThreshold = nowAtUtc(),
                    ferdigstiltThreshold = nowAtUtc(),
                    limit = 10
                )
            }
        }

        arkivTestRepository.getAllArchivedVarsel().size shouldBe 0
        varslingInDbCount() shouldBe 4
    }

    @Test
    fun `lagrer varselId-er i egen indeksert kolonne for lettere oppslag`() = runBlocking {

        coEvery { leaderElection.isLeader() } returns true

        runArchiverToCompletion(
            opprettetThreshold = 0,
            ferdigstiltThreshold = 0
        )

        val arkiverteVarsler = arkivTestRepository.getAllArchivedVarsel()

        arkiverteVarsler.size shouldBe 4
        arkiverteVarsler.forEach {
            val varselIdsFromColumn = arkivTestRepository.varselIdsColumn(it.sendingsId)
            val varselIdsFromSerializedData = it.serializedData.varsler.map(Varsel::varselId)

            varselIdsFromColumn shouldContainExactly varselIdsFromSerializedData
        }
    }

    @Test
    fun `ignorerer duplikate varsler i arkiv-tabell`() = runBlocking<Unit> {
        coEvery { leaderElection.isLeader() } returns true

        val sendingsId = "id-x"
        val varsling = varsling(sendingsId = sendingsId, opprettet = nowAtUtc().minusDays(100), ferdigstilt = null)

        createVarsling(varsling)

        arkivTestRepository.varslingExists(sendingsId) shouldBe true
        arkivTestRepository.getAllArchivedVarsel().size shouldBe 0

        runArchiverToCompletion(
            opprettetThreshold = 50,
            ferdigstiltThreshold = 50
        )

        arkivTestRepository.varslingExists(sendingsId) shouldBe false
        arkivTestRepository.getAllArchivedVarsel().size shouldBe 1


        // Simuler feilaktig arkivering
        createVarsling(varsling)

        arkivTestRepository.varslingExists(sendingsId) shouldBe true
        arkivTestRepository.getAllArchivedVarsel().size shouldBe 1

        runArchiverToCompletion(
            opprettetThreshold = 50,
            ferdigstiltThreshold = 50
        )

        arkivTestRepository.varslingExists(sendingsId) shouldBe false
        arkivTestRepository.getAllArchivedVarsel().size shouldBe 1
    }

    @Test
    fun `håndterer at varsel kan ligge i legacy jsonb-kolonne og egen tabell`() = runBlocking<Unit> {
        coEvery { leaderElection.isLeader() } returns true

        val sendingsId = "id-x"
        val legacyVarsel = varsel().copy(legacyJsonb = true)
        val varsel = varsel()

        val varsling = varsling(
            sendingsId,
            opprettet = nowAtUtc().minusDays(100),
            ferdigstilt = null,
            varsler = listOf(legacyVarsel, varsel)
        )

        testRepository.insertEksternVarslingWithLegacyVarsel(varsling)

        runArchiverToCompletion(
            opprettetThreshold = 50,
            ferdigstiltThreshold = 50
        )

        arkivTestRepository.getAllArchivedVarsel().let {
            it.size shouldBe 1

            val archivedVarsler = it.first().serializedData.varsler
            archivedVarsler.size shouldBe 2
            archivedVarsler.map { it.varselId }.shouldContain(legacyVarsel.varselId)
            archivedVarsler.map { it.varselId }.shouldContain(varsel.varselId)
        }
    }

    @Test
    fun `does nothing when not leader`() = runBlocking<Unit> {
        coEvery { leaderElection.isLeader() } returns false

        runArchiverToCompletion(0, 0)

        varslingInDbCount() shouldBe 4
        arkivTestRepository.getAllArchivedVarsel().size shouldBe 0
    }

    private fun runArchiverToCompletion(opprettetThreshold: Long, ferdigstiltThreshold: Long) = runBlocking {
        val archiver = PeriodicArchiver(
            arkivRepository = archiveRepository,
            ageThresholdDaysOpprettet = opprettetThreshold,
            ageThresholdDaysFerdigstilt = ferdigstiltThreshold,
            interval = Duration.ofMillis(50),
            leaderElection = leaderElection,
        )

        var lastArchived = 0

        archiver.start()
        withTimeout(3000) {
            delay(200)
            while (true) {
                val currentArchived = arkivTestRepository.getAllArchivedVarsel().size

                if (currentArchived == lastArchived) {
                    break
                } else {
                    lastArchived = currentArchived
                    delay(100)
                }
            }
        }
        archiver.stop()
    }

    private fun varslingInDbCount(): Int {
        return database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        }?: 0
    }

    private fun varsling(
        sendingsId: String,
        opprettet: ZonedDateTime,
        ferdigstilt: ZonedDateTime?,
        varsler: List<Varsel> = listOf(
            varsel(),
            varsel(),
            varsel()
        )
    ) = EksternVarsling(
        sendingsId = sendingsId,
        ident = testIdent,
        erBatch = false,
        erUtsattVarsel = false,
        varsler = varsler,
        utsending = null,
        ferdigstilt = ferdigstilt,
        status = if (ferdigstilt != null) {
            Sendingsstatus.Sendt
        } else {
            Sendingsstatus.Venter
        },
        eksternStatus = if (ferdigstilt != null) {
            EksternStatus.Oversikt(
                sendt = true,
                renotifikasjonSendt = false,
                kanal = "SMS",
                historikk = emptyList(),
                sistOppdatert = ferdigstilt,
            )
        } else {
            null
        },
        bestilling = if (ferdigstilt != null) {
            Bestilling(
                preferertKanal = Kanal.SMS,
                tekster = Tekster(
                    smsTekst = "Sms-tekst",
                    epostTittel = "Epost-tittel",
                    epostTekst = "Epost-tekst"
                ),
                revarsling = null,
            )
        } else {
            null
        },
        opprettet = opprettet,
    )

    private fun varsel() = Varsel(
        varselId = UUID.randomUUID().toString(),
        varseltype = Varseltype.Beskjed,
        preferertKanal = Kanal.SMS,
        smsVarslingstekst = "Sms-tekst",
        epostVarslingstittel = "Epost-tittel",
        epostVarslingstekst = "Epost-tekst",
        produsent = Produsent(
            cluster = "test-cluster",
            namespace = "test-namespace",
            appnavn = "test-appnavn"
        ),
        aktiv = false,
        opprettet = nowAtUtc().minusDays(30)
    )

    private fun daysBetween(date1: ZonedDateTime, date2: ZonedDateTime): Long {
        val deltaSeconds = date1.toEpochSecond() - date2.toEpochSecond()

        val secondsInDay = Duration.ofDays(1).toSeconds()

        return deltaSeconds.absoluteValue / secondsInDay
    }

    private infix fun ZonedDateTime?.shouldBeSameInstantAs(other: ZonedDateTime?) {
        this?.toInstant() shouldBe other?.toInstant()
    }
}

class ArkivTestRepository(private val database: PostgresDatabase) {
    fun getAllArchivedVarsel(): List<ArkivertVarsling> {
        return database.list {
            queryOf("select * from ekstern_varsling_arkiv")
                .map(::toArkivertVarsling)
        }
    }

    fun varslingExists(sendingsId: String): Boolean {
        return database.singleOrNull {
            queryOf(
                "select true from ekstern_varsling where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId)
            ).map {
                it.boolean(1)
            }
        } ?: false
    }

    private fun toArkivertVarsling(row: Row) = ArkivertVarsling(
        sendingsId = row.string("sendingsId"),
        ident = row.string("ident"),
        opprettet = row.zonedDateTime("opprettet"),
        ferdigstilt = row.zonedDateTimeOrNull("ferdigstilt"),
        serializedData = row.json("varsling"),
        begrunnelse = row.enum("begrunnelse")
    )

    fun varselIdsColumn(sendingsId: String): List<String> {
        return database.single {
            queryOf(
                "select varselIds from ekstern_varsling_arkiv where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId)
            ).map {
                it.json<List<String>>("varselIds")
            }
        }
    }
}
