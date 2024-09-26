package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.shouldBe
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.json
import no.nav.tms.kafka.application.MessageBroadcaster
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

class InaktivertVarselSubscriberTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val testFnr = "12345678910"

    private val repository = EksternVarselRepository(database)
    private val opprettetVarselBroadcaster = MessageBroadcaster(listOf(OpprettetVarselSubscriber(repository)))
    private val inaktiverVarselBroadcaster = MessageBroadcaster(listOf(InaktivertVarselSubscriber(repository)))

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from ekstern_varsling")
        }
    }

    @Test
    fun `Plukker opp inaktivert kafka-eventer og inaktiverer varsler i basen`() {
        val inarkivertVarselIdEn = UUID.randomUUID().toString()
        val inarkivertVarselIdTwo = UUID.randomUUID().toString()

        opprettetVarselBroadcaster.broadcastJson(
            createEksternVarslingEvent(
                id = UUID.randomUUID().toString(),
                erBatch = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            createEksternVarslingEvent(
                id = UUID.randomUUID().toString(),
                erBatch = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            createEksternVarslingEvent(
                id = inarkivertVarselIdEn,
                erBatch = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            createEksternVarslingEvent(
                id = inarkivertVarselIdTwo,
                erBatch = true,
                ident = testFnr
            )
        )
        opprettetVarselBroadcaster.broadcastJson(
            createEksternVarslingEvent(
                id = UUID.randomUUID().toString(),
                erBatch = true,
                ident = testFnr
            )
        )

        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = inarkivertVarselIdEn))
        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = inarkivertVarselIdTwo))



        database.singleOrNull {
            queryOf(
                "select varsler from ekstern_varsling where ident = :ident",
                mapOf("ident" to testFnr)
            )
                .map { it.json<List<Varsel>>("varsler").count { it.aktiv } }.asSingle
        } shouldBe 3
    }

    @Test
    fun `Ignorer varseler som tilhører ferdigstilte sendinger`() {

        val sendingsId = UUID.randomUUID().toString()
        val varselId = UUID.randomUUID().toString()

        val sendingsId2 = UUID.randomUUID().toString()
        val varselId2 = UUID.randomUUID().toString()

        val sendingsId3 = UUID.randomUUID().toString()
        val varselId3 = UUID.randomUUID().toString()

        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                sendingsId,
                testFnr,
                status = Sendingsstatus.Sendt,
                ferdigstilt = ZonedDateTimeHelper.nowAtUtc().minusHours(1),
                varsler = listOf(createVarsel(varselId = varselId))
            )
        )
        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                sendingsId2,
                testFnr,
                status = Sendingsstatus.Kanselert,
                ferdigstilt = ZonedDateTimeHelper.nowAtUtc().minusHours(1),
                varsler = listOf(createVarsel(varselId = varselId2))
            )
        )

        database.insertEksternVarsling(
            createEksternVarslingDBRow(
                sendingsId3,
                testFnr,
                varsler = listOf(createVarsel(varselId = varselId3))
            )
        )

        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = varselId))
        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = varselId2))
        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = varselId3))


        tellAktiveVarsler(sendingsId) shouldBe 1
        tellAktiveVarsler(sendingsId2) shouldBe 1
        tellAktiveVarsler(sendingsId3) shouldBe 0

    }

    private fun tellAktiveVarsler(sendingsId: String) = database.singleOrNull {
            queryOf(
                "select varsler from ekstern_varsling where sendingsId = :sendingsId",
                mapOf("sendingsId" to sendingsId)
            )
                .map { it.json<List<Varsel>>("varsler").count { it.aktiv } }.asSingle
        }

}