package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.Bestilling
import no.nav.tms.ekstern.varsling.Kanal
import no.nav.tms.ekstern.varsling.Revarsling
import no.nav.tms.ekstern.varsling.Sendingsstatus
import no.nav.tms.ekstern.varsling.recordqueue.DoknotStopQueueRepository
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.TestRepository
import no.nav.tms.kafka.application.MessageBroadcaster
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.*

class InaktivertVarselSubscriberTest {
    private val database = LocalPostgresDatabase.getCleanInstance()
    private val testFnr = "12345678910"

    private val testRepository = TestRepository(database)
    private val repository = EksternVarslingBestillingRepository(database)
    private val queueRepository = DoknotStopQueueRepository(database)
    private val broadcaster = MessageBroadcaster(
        OpprettetVarselSubscriber(repository, mockk(relaxed = true), enableBatch = true),
        InaktivertVarselSubscriber(repository, queueRepository)
    )

    @AfterEach
    fun cleanup() {
        LocalPostgresDatabase.resetInstance()
    }

    @Test
    fun `Plukker opp inaktivert kafka-eventer og inaktiverer varsler i basen`() {
        val inarkivertVarselIdEn = UUID.randomUUID().toString()
        val inarkivertVarselIdTwo = UUID.randomUUID().toString()

        broadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )
        broadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )
        broadcaster.broadcastJson(
            varselOpprettetEvent(
                id = inarkivertVarselIdEn,
                kanBatches = true,
                ident = testFnr
            )
        )
        broadcaster.broadcastJson(
            varselOpprettetEvent(
                id = inarkivertVarselIdTwo,
                kanBatches = true,
                ident = testFnr
            )
        )
        broadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )

        broadcaster.broadcastJson(inaktivertEvent(id = inarkivertVarselIdEn))
        broadcaster.broadcastJson(inaktivertEvent(id = inarkivertVarselIdTwo))

        database.singleOrNull {
            queryOf(
                "select count(*) as antall from ekstern_varsling as ev join varsel as v on ev.sendingsId = v.sendingsId where ev.ident = :ident and v.aktiv",
                mapOf("ident" to testFnr)
            )
                .map { it.int("antall") }
        } shouldBe 3
    }

    @Test
    fun `Legger doknotifikasjon-stopp i outbox-kø hvis revarsling er satt`() {
        val sendingsId = UUID.randomUUID().toString()
        val varselId = UUID.randomUUID().toString()

        testRepository.insertEksternVarsling(
            eksternVarslingDBRow(
                sendingsId,
                testFnr,
                status = Sendingsstatus.Sendt,
                ferdigstilt = ZonedDateTimeHelper.nowAtUtc().minusHours(1),
                varsler = listOf(createVarsel(varselId = varselId)),
                bestilling = Bestilling(
                    preferertKanal = Kanal.SMS,
                    tekster = null,
                    revarsling = Revarsling(1, 7)
                )
            )
        )

        broadcaster.broadcastJson(inaktivertEvent(id = varselId))

        queueRepository.peekNextDoknotStop(1).firstOrNull().let {
            it.shouldNotBeNull()

            it.sendingsId shouldBe sendingsId
        }
    }
}
