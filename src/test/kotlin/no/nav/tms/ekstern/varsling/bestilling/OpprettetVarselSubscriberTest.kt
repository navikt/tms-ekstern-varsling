package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.date.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.ktor.client.utils.EmptyContent.status
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.json
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
            queryOf("delete from ekstern_varsling")
        }
    }

    @Test
    fun `plukker opp opprettet varsel og legger til status venter`() {

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))

        database.singleOrNull {
            queryOf(
                "select count(*) filter(where status = :status) as antall from ekstern_varsling",
                mapOf("status" to Sendingsstatus.Venter.name)
            )
                .map { it.int("antall") }
                .asSingle
        } shouldBe 5


    }

    @Test
    fun `legger varsel som kan batches i eksisterende batch`() {

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = true))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = false))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = false))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = true))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = true))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = true))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr, erBatch = true))


        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 3

        database.singleOrNull {
            queryOf("select varsler from ekstern_varsling where erBatch")
                .map { it.json<List<Varsel>>("varsler").size }
                .asSingle
        } shouldBe 5

        val utsending = database.singleOrNull {
            queryOf("select utsending from ekstern_varsling where erBatch")
                .map { it.zonedDateTimeOrNull("utsending") }
                .asSingle
        }

        utsending.shouldNotBeNull()
        utsending.shouldBeBetween(ZonedDateTimeHelper.nowAtUtc().plusMinutes(59), ZonedDateTimeHelper.nowAtUtc().plusMinutes(60))

    }

    @Test
    fun `Ignorer varsel uten ekstern varsling`(){

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(opprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 2
    }

    @Test
    fun `bruker dato i utsettSendingTil til å sette utsendingsdato på ekstern varsling`(){

        val utsettSendingTil = ZonedDateTimeHelper.nowAtUtc().plusDays(7)

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), utsettSendingTil = utsettSendingTil,ident = testFnr))

        database.singleOrNull {
            queryOf("select utsending from ekstern_varsling where utsending is not null ")
                .map { it.zonedDateTime("utsending") }
                .asSingle
        }?.toEpochSecond() shouldBe utsettSendingTil.toEpochSecond()
    }

    @Test
    fun `ikke bruk utsattsending til batch`(){

        val utsettSendingTil = ZonedDateTimeHelper.nowAtUtc().plusDays(7)

        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(), utsettSendingTil = utsettSendingTil,ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = UUID.randomUUID().toString(),ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 2
    }

    @Test
    fun `ignorerer duplikate varsler`() {
        val eventId = UUID.randomUUID().toString()

        broadcaster.broadcastJson(createEksternVarslingEvent(id = eventId, ident = testFnr))
        broadcaster.broadcastJson(createEksternVarslingEvent(id = eventId, ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
                .asSingle
        } shouldBe 1

        database.singleOrNull {
            queryOf("select varsler from ekstern_varsling")
                .map { it.json<List<Varsel>>("varsler").size }
                .asSingle
        } shouldBe 1
    }

}
