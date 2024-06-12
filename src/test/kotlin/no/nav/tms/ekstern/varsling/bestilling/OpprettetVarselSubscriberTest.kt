package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.shouldBe
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.database.LocalPostgresDatabase
import no.nav.tms.kafka.application.MessageBroadcaster
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpprettetVarselSubscriberTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val testFnr = "12345678910"

    private val repository = EksternVarselRepository(database)
    private val broadcaster = MessageBroadcaster(listOf(OpprettetVarselSubscriber(repository)))

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from eksterne_varsler")
        }
    }

    @Test
    fun `plukker opp opprettet varsel`() {

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from eksterne_varsler")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 5
    }

    @Test
    fun `Ignorer varsel uten ekstern varsling`(){

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from eksterne_varsler")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 2
    }
}
