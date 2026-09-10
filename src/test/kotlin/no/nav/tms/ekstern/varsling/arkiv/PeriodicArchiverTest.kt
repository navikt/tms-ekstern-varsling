package no.nav.tms.ekstern.varsling.arkiv

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotliquery.Row
import kotliquery.queryOf
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.common.postgres.PostgresDatabase
import no.nav.tms.ekstern.varsling.arkiv.ArkivertVarsling.ArkiveringsBegrunnelse.BestillingForeldet
import no.nav.tms.ekstern.varsling.arkiv.ArkivertVarsling.ArkiveringsBegrunnelse.VarslingFerdigstilt
import no.nav.tms.ekstern.varsling.bestilling.Bestilling
import no.nav.tms.ekstern.varsling.bestilling.EksternStatus
import no.nav.tms.ekstern.varsling.bestilling.EksternVarsling
import no.nav.tms.ekstern.varsling.bestilling.EksternVarslingRepository
import no.nav.tms.ekstern.varsling.bestilling.Kanal
import no.nav.tms.ekstern.varsling.bestilling.Produsent
import no.nav.tms.ekstern.varsling.bestilling.Sendingsstatus
import no.nav.tms.ekstern.varsling.bestilling.Tekster
import no.nav.tms.ekstern.varsling.bestilling.Varsel
import no.nav.tms.ekstern.varsling.bestilling.Varseltype
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.common.enum
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
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

    private val testRepository = ArkivTestRepository(database)

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
        createVarsel(gammelUbehandeltVarsling, gammelFerdigstiltVarsling, nyUbehandletVarsling, nyFerdigstiltVarsling)
    }

    @AfterEach
    fun cleanUp() {
        clearMocks(leaderElection)
        LocalPostgresDatabase.resetInstance()
    }


    fun createVarsel(vararg varsler: EksternVarsling) {
        val varselRepository = EksternVarslingRepository(database)

        varsler.forEach { varselRepository.insertEksternVarsling(it) }
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

        val arkiverteVarsler = testRepository.getAllArchivedVarsel()
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

        val arkiverteVarsler = testRepository.getAllArchivedVarsel()
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

        val arkiverteVarsler = testRepository.getAllArchivedVarsel()
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

        val arkiverteVarsler = testRepository.getAllArchivedVarsel()

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
    fun `lagrer varselId-er i egen indeksert kolonne for lettere oppslag`() = runBlocking {

        coEvery { leaderElection.isLeader() } returns true

        runArchiverToCompletion(
            opprettetThreshold = 0,
            ferdigstiltThreshold = 0
        )

        val arkiverteVarsler = testRepository.getAllArchivedVarsel()

        arkiverteVarsler.size shouldBe 4
        arkiverteVarsler.forEach {
            val varselIdsFromColumn = testRepository.varselIdsColumn(it.sendingsId)
            val varselIdsFromSerializedData = it.serializedData.varsler.map(Varsel::varselId)

            varselIdsFromColumn shouldContainExactly varselIdsFromSerializedData
        }
    }

    @Test
    fun `ignorerer duplikate varsler i arkiv-tabell`() = runBlocking<Unit> {
        coEvery { leaderElection.isLeader() } returns true

        val sendingsId = "id-x"
        val varsling = varsling(sendingsId = sendingsId, opprettet = nowAtUtc().minusDays(100), ferdigstilt = null)

        createVarsel(varsling)

        testRepository.varslingExists(sendingsId) shouldBe true
        testRepository.getAllArchivedVarsel().size shouldBe 0

        runArchiverToCompletion(
            opprettetThreshold = 50,
            ferdigstiltThreshold = 50
        )

        testRepository.varslingExists(sendingsId) shouldBe false
        testRepository.getAllArchivedVarsel().size shouldBe 1


        // Simuler feilaktig arkivering
        createVarsel(varsling)

        testRepository.varslingExists(sendingsId) shouldBe true
        testRepository.getAllArchivedVarsel().size shouldBe 1

        runArchiverToCompletion(
            opprettetThreshold = 50,
            ferdigstiltThreshold = 50
        )

        testRepository.varslingExists(sendingsId) shouldBe false
        testRepository.getAllArchivedVarsel().size shouldBe 1
    }

    @Test
    fun `does nothing when not leader`() = runBlocking<Unit> {
        coEvery { leaderElection.isLeader() } returns false

        runArchiverToCompletion(0, 0)

        varslingInDbCount() shouldBe 4
        testRepository.getAllArchivedVarsel().size shouldBe 0
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
        withTimeout(50000) {
            delay(200)
            while (true) {
                val currentArchived = testRepository.getAllArchivedVarsel().size

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
        ferdigstilt: ZonedDateTime?
    ) = EksternVarsling(
        sendingsId = sendingsId,
        ident = testIdent,
        erBatch = false,
        erUtsattVarsel = false,
        varsler = listOf(
            varsel(),
            varsel(),
            varsel()
        ),
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
        prefererteKanaler = listOf(Kanal.SMS),
        smsVarslingstekst = "Sms-tekst",
        epostVarslingstittel = "Epost-tittel",
        epostVarslingstekst = "Epost-tekst",
        produsent = Produsent(
            cluster = "test-cluster",
            namespace = "test-namespace",
            appnavn = "test-appnavn"
        ),
        aktiv = false,
        behandletAvLegacy = false
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
