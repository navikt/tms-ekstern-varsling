package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.date.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.json
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.setup.defaultObjectMapper
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.kafka.application.MessageBroadcaster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.ZonedDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpprettetVarselSubscriberTest {
    private val database = LocalPostgresDatabase.getInstance()
    private val testFnr = "12345678910"

    private val statusTopic = MockProducer(
        false,
        StringSerializer(),
        StringSerializer()
    )

    private val statusProducer = EksternVarslingOppdatertProducer(statusTopic, "dummy-topic")

    private val repository = EksternVarslingRepository(database)
    private val broadcaster = MessageBroadcaster(
        OpprettetVarselSubscriber(repository, statusProducer, enableBatch = true),
        enableTracking = true
    )

    @AfterEach
    fun cleanup() {
        database.update {
            queryOf("delete from ekstern_varsling")
        }
        broadcaster.clearHistory()
        statusTopic.clear()
    }

    @Test
    fun `plukker opp opprettet varsel og legger til status venter`() {

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))

        database.singleOrNull {
            queryOf(
                "select count(*) filter(where status = :status) as antall from ekstern_varsling",
                mapOf("status" to Sendingsstatus.Venter.name)
            )
                .map { it.int("antall") }
        } shouldBe 5

        statusTopic.history().size shouldBe 5
    }

    @Test
    fun `sender status 'venter' til internt topic`() {
        val varselId = UUID.randomUUID().toString()

        broadcaster.broadcastJson(varselOpprettetEvent(id = varselId, ident = testFnr, type = "oppgave"))

        val objectMapper = defaultObjectMapper()

        statusTopic.history()
            .first()
            .value()
            .let { objectMapper.readTree(it) }
            .let { event ->
                event["@event_name"].asText() shouldBe "eksternVarslingStatusOppdatert"
                event["status"].asText() shouldBe "venter"
                event["ident"].asText() shouldBe testFnr
                event["varseltype"].asText() shouldBe "oppgave"
            }
    }

    @Test
    fun `legger varsel som kan batches i eksisterende batch`() {

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = false))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = false))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))


        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 3

        database.singleOrNull {
            queryOf("select varsler from ekstern_varsling where erBatch")
                .map { it.json<List<Varsel>>("varsler").size }
        } shouldBe 5

        val utsending = database.singleOrNull {
            queryOf("select utsending from ekstern_varsling where erBatch")
                .map { it.zonedDateTimeOrNull("utsending") }
        }

        utsending.shouldNotBeNull()
        utsending.shouldBeBetween(ZonedDateTimeHelper.nowAtUtc().plusMinutes(59), ZonedDateTimeHelper.nowAtUtc().plusMinutes(60))

    }

    @Test
    fun `tillater å skru av all batching`() {

        val localBroadcaster = MessageBroadcaster(listOf(OpprettetVarselSubscriber(repository, statusProducer, enableBatch = false)))

        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = false))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = false))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        localBroadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))


        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 7

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling where utsending is null")
                .map { it.int("antall") }
        } shouldBe 7
    }

    @Test
    fun `hvis utsatt sending er satt ignoreres kanBatches-flagget`() {

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true, utsettSendingTil = ZonedDateTime.now().plusDays(1)))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true, utsettSendingTil = ZonedDateTime.now().plusDays(1)))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr, kanBatches = true))


        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 3

        database.singleOrNull {
            queryOf("select varsler from ekstern_varsling where erBatch")
                .map { it.json<List<Varsel>>("varsler").size }
        } shouldBe 3

        database.list {
            queryOf("select varsler from ekstern_varsling where not erBatch")
                .map { it.json<List<Varsel>>("varsler").size }
        }.all { it == 1 } shouldBe true
    }

    @Test
    fun `Ignorer varsel uten ekstern varsling`(){

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEventUtenEksternVarsling(id = UUID.randomUUID().toString(), ident = testFnr))

        broadcaster.history().collectAggregate(OpprettetVarselSubscriber::class).let {
            it.shouldNotBeNull()
            it.accepted shouldBe 2
            it.ignored shouldBe 3
        }

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 2
    }

    @Test
    fun `bruker dato i utsettSendingTil til å sette utsendingsdato på ekstern varsling`(){

        val utsettSendingTil = ZonedDateTimeHelper.nowAtUtc().plusDays(7)

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), utsettSendingTil = utsettSendingTil,ident = testFnr))

        database.singleOrNull {
            queryOf("select utsending from ekstern_varsling where utsending is not null ")
                .map { it.zonedDateTime("utsending") }
        }?.toEpochSecond() shouldBe utsettSendingTil.toEpochSecond()
    }

    @Test
    fun `ikke bruk utsattsending til batch`(){

        val utsettSendingTil = ZonedDateTimeHelper.nowAtUtc().plusDays(7)

        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(), utsettSendingTil = utsettSendingTil,ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = UUID.randomUUID().toString(),ident = testFnr))

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 2
    }

    @Test
    fun `ignorerer duplikate varsler`() {
        val eventId = UUID.randomUUID().toString()

        broadcaster.broadcastJson(varselOpprettetEvent(id = eventId, ident = testFnr))
        broadcaster.broadcastJson(varselOpprettetEvent(id = eventId, ident = testFnr))

        broadcaster.history().findFailedOutcome(OpprettetVarselSubscriber::class) {
            it["varselId"].asText() == eventId
        }.let {
            it.shouldNotBeNull()
            it.cause::class shouldBe DuplicateVarselException::class
        }

        database.singleOrNull {
            queryOf("select count(*) as antall from ekstern_varsling")
                .map { it.int("antall") }
        } shouldBe 1

        database.singleOrNull {
            queryOf("select varsler from ekstern_varsling")
                .map { it.json<List<Varsel>>("varsler").size }
        } shouldBe 1
    }

    @Test
    fun `oppretter ny sendingsId hvis sending er batch`() {
        val varselId = UUID.randomUUID().toString()

        broadcaster.broadcastJson(varselOpprettetEvent(id = varselId, ident = testFnr, kanBatches = true))

        database.singleOrNull {
            queryOf(
                "select sendingsId from ekstern_varsling"
            )
            .map { it.string("sendingsId") }
        } shouldNotBe varselId
    }

    @Test
    fun `bruker varselId som sendingsId hvis sending ikke er batch`() {
        val varselId = UUID.randomUUID().toString()

        broadcaster.broadcastJson(varselOpprettetEvent(id = varselId, ident = testFnr, kanBatches = false))

        database.singleOrNull {
            queryOf(
                "select sendingsId from ekstern_varsling"
            )
                .map { it.string("sendingsId") }
        } shouldBe varselId
    }
}
