package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotliquery.queryOf
import no.nav.doknotifikasjon.schemas.DoknotifikasjonStopp
import no.nav.tms.ekstern.varsling.setup.DummySerializer
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.json
import no.nav.tms.kafka.application.MessageBroadcaster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.*

class InaktivertVarselSubscriberTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val testFnr = "12345678910"

    private val mockProducer = MockProducer<String, DoknotifikasjonStopp>(
        false,
        StringSerializer(),
        DummySerializer()
    )


    private val repository = EksternVarslingRepository(database)
    private val opprettetVarselBroadcaster = MessageBroadcaster(listOf(OpprettetVarselSubscriber(repository, enableBatch = true)))
    private val inaktiverVarselBroadcaster = MessageBroadcaster(listOf(InaktivertVarselSubscriber(repository, mockProducer, "dummyTopic")))

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from ekstern_varsling")
        }
        mockProducer.clear()
    }

    @Test
    fun `Plukker opp inaktivert kafka-eventer og inaktiverer varsler i basen`() {
        val inarkivertVarselIdEn = UUID.randomUUID().toString()
        val inarkivertVarselIdTwo = UUID.randomUUID().toString()

        opprettetVarselBroadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            varselOpprettetEvent(
                id = inarkivertVarselIdEn,
                kanBatches = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            varselOpprettetEvent(
                id = inarkivertVarselIdTwo,
                kanBatches = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            varselOpprettetEvent(
                id = UUID.randomUUID().toString(),
                kanBatches = true,
                ident = testFnr
            )
        )

        inaktiverVarselBroadcaster.broadcastJson(inaktivertEvent(id = inarkivertVarselIdEn))
        inaktiverVarselBroadcaster.broadcastJson(inaktivertEvent(id = inarkivertVarselIdTwo))



        database.singleOrNull {
            queryOf(
                "select varsler from ekstern_varsling where ident = :ident",
                mapOf("ident" to testFnr)
            )
                .map { it.json<List<Varsel>>("varsler").count { it.aktiv } }.asSingle
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
                revarsling = Revarsling(1, 7)
            )
        )

        inaktiverVarselBroadcaster.broadcastJson(inaktivertEvent(id = varselId))

        mockProducer.history().firstOrNull().let {
            it.shouldNotBeNull()

            val doknotStopp = it.value()

            doknotStopp.bestillerId shouldBe "tms-ekstern-varsling"
            doknotStopp.bestillingsId shouldBe sendingsId
        }
    }

    private fun tellAktiveVarsler(sendingsId: String) = database.singleOrNull {
            queryOf(
                "select varsler from ekstern_varsling where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId)
            )
                .map { it.json<List<Varsel>>("varsler").count { it.aktiv } }.asSingle
        }

}
