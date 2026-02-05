package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import kotliquery.queryOf
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStopp
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.ekstern.varsling.setup.DummySerializer
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.kafka.application.MessageBroadcaster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.*

class InaktivertVarselSubscriberTest {
    private val database = LocalPostgresDatabase.getCleanInstance()
    private val testFnr = "12345678910"

    private val stopTopic = MockProducer<String, DoknotifikasjonStopp>(
        false,
        StringSerializer(),
        DummySerializer()
    )

    private val repository = EksternVarslingRepository(database)
    private val broadcaster = MessageBroadcaster(
        OpprettetVarselSubscriber(repository, mockk(relaxed = true), enableBatch = true),
        InaktivertVarselSubscriber(repository, stopTopic, "dummyTopic")
    )

    @AfterEach
    fun cleanup() {
        LocalPostgresDatabase.resetInstance()
        stopTopic.clear()
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
                "select varsler from ekstern_varsling where ident = :ident",
                mapOf("ident" to testFnr)
            )
                .map { it.json<List<Varsel>>("varsler").count { it.aktiv } }
        } shouldBe 3
    }

    @Test
    fun `Sender doknotifikasjon-stopp hvis revarsling er satt`() {
        val sendingsId = UUID.randomUUID().toString()
        val varselId = UUID.randomUUID().toString()

        database.insertEksternVarsling(
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

        stopTopic.history().firstOrNull().let {
            it.shouldNotBeNull()

            val doknotStopp = it.value()

            doknotStopp.bestillerId shouldBe "tms-ekstern-varsling"
            doknotStopp.bestillingsId shouldBe sendingsId
        }
    }
}
