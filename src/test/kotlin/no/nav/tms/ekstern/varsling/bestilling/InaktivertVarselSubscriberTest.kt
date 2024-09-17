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

class InaktivertVarselSubscriberTest{
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
    fun `Plukker opp inaktiver kafka-eventer og inaktiverer varsler i basen`(){
        val inarkivertVarselIdEn= UUID.randomUUID().toString()
        val inarkivertVarselIdTwo = UUID.randomUUID().toString()

        opprettetVarselBroadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), erBatch=true, ident = testFnr))
        opprettetVarselBroadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), erBatch=true, ident = testFnr))
        opprettetVarselBroadcaster.broadcastJson(createEksternVarslingEvent(id = inarkivertVarselIdEn, erBatch=true, ident = testFnr))
        opprettetVarselBroadcaster.broadcastJson(createEksternVarslingEvent(id = inarkivertVarselIdTwo, erBatch=true, ident = testFnr))
        opprettetVarselBroadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), erBatch=true, ident = testFnr))

        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = inarkivertVarselIdEn))
        inaktiverVarselBroadcaster.broadcastJson(createInaktiverEvent(id = inarkivertVarselIdTwo))



        database.singleOrNull {
            queryOf(
                "select varsler from ekstern_varsling where ident = :ident",
                mapOf("ident" to testFnr)
            )
                .map { it.json<List<Varsel>>("varsler").filter { it.aktiv }.count() }.asSingle
        } shouldBe 3
    }
}