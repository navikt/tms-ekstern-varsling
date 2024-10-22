package no.nav.tms.ekstern.varsling.status

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.bestilling.*
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.ekstern.varsling.bestilling.ZonedDateTimeHelper.nowAtUtc
import no.nav.tms.ekstern.varsling.status.DoknotifikasjonStatusEnum.*
import no.nav.tms.kafka.application.MessageBroadcaster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.*


class EksternVarslingStatusSubscriberTest {

    private val database = LocalPostgresDatabase.cleanDb()
    private val repository = EksternVarslingRepository(database)

    private val ident = "12345678901"

    private val mockProducer = MockProducer(
        false,
        StringSerializer(),
        StringSerializer()
    )

    private val eksternVarslingOppdatertProducer = EksternVarslingOppdatertProducer(mockProducer, "testtopic")
    private val eksternVarslingStatusUpdater =
        EksternStatusUpdater(
            repository,
            eksternVarslingOppdatertProducer
        )
    private val testBroadcaster =
        MessageBroadcaster(
            listOf(
                EksternVarslingStatusSubscriber(eksternVarslingStatusUpdater),
            )
        )

    @BeforeEach
    fun resetDb() {
        database.update { queryOf("delete from ekstern_varsling") }
        mockProducer.clear()
    }

    @Test
    fun `Lagrer ekstern varsling-status`() {

        val melding = "Sendt via epost"
        val distribusjonsId = 123L
        val kanal = "EPOST"

        val sendingsId = UUID.randomUUID().toString()

        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId, ident))

        val doknotEvent = eksternVarslingStatus(
            eventId = sendingsId,
            status = FERDIGSTILT,
            melding = melding,
            distribusjonsId = distribusjonsId,
            kanal = kanal
        )

        testBroadcaster.broadcastJson(doknotEvent)

        val varsling = repository.getEksternVarsling(sendingsId)

        varsling?.eksternStatus.shouldNotBeNull()

        varsling?.eksternStatus?.let {
            it shouldNotBe null

            it.sendt shouldBe true

            it.historikk.size shouldBe 1
            it.historikk.first().melding shouldBe melding
            it.historikk.first().distribusjonsId shouldBe distribusjonsId
            it.kanal shouldBe kanal
        }


        mockProducer.history().size shouldBe 1
    }

    @Test
    fun `ignorer eventer for andre apper`() {

        val melding = "Sendt via epost"
        val distribusjonsId = 123L
        val kanal = "EPOST"

        val sendingsId = UUID.randomUUID().toString()

        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId, ident))

        val doknotEvent = eksternVarslingStatus(
            bestillerAppnavn = "annen-app",
            eventId = sendingsId,
            status = FERDIGSTILT,
            melding = melding,
            distribusjonsId = distribusjonsId,
            kanal = kanal
        )

        testBroadcaster.broadcastJson(doknotEvent)

        val varsling = repository.getEksternVarsling(sendingsId)

        varsling?.eksternStatus.shouldBeNull()


        mockProducer.history().size shouldBe 0
    }

    @Test
    fun `Flere ekstern varsling-statuser oppdaterer basen`() {
        val sendingsId = UUID.randomUUID().toString()

        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId, ident))

        val infoEvent = eksternVarslingStatus(sendingsId, status = INFO)
        val epostEvent = eksternVarslingStatus(sendingsId, status = FERDIGSTILT, kanal = "EPOST")
        val smsEvent = eksternVarslingStatus(sendingsId, status = FERDIGSTILT, kanal = "SMS")

        testBroadcaster.broadcastJson(infoEvent)
        testBroadcaster.broadcastJson(epostEvent)
        testBroadcaster.broadcastJson(smsEvent)

        val status = repository.getEksternVarsling(sendingsId)?.eksternStatus
        status.shouldNotBeNull()
        status.kanal shouldBe "EPOST"

        status.sendt shouldBe true

        mockProducer.history().size shouldBe 3
    }

    @Test
    fun `gjør ingenting hvis sendingsId er ukjent`() {

        val sendingsId = UUID.randomUUID().toString()

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId))

        repository.getEksternVarsling(sendingsId)?.eksternStatus shouldBe null

        mockProducer.history().size shouldBe 0
    }

    @Test
    fun `sender eventer om oppdaterte varsler`() {
        val sendingsId = UUID.randomUUID().toString()
        val varselId = UUID.randomUUID().toString()

        val oppgave = createVarsel(varselId, Varseltype.Oppgave)

        val eksternVarsling = sendtEksternVarsling(sendingsId, ident, varsler = listOf(oppgave))

        repository.insertEksternVarsling(eksternVarsling)

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, OVERSENDT))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, INFO))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, FEILET))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, FERDIGSTILT, kanal = "SMS"))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, FERDIGSTILT, kanal = null))

        mockProducer.verifyOutput { output ->
            output.find { it["status"].textValue() == "bestilt" } shouldNotBe null
            output.find { it["status"].textValue() == "info" } shouldNotBe null
            output.find { it["status"].textValue() == "feilet" } shouldNotBe null
            output.find { it["status"].textValue() == "sendt" } shouldNotBe null
            output.find { it["status"].textValue() == "ferdigstilt" } shouldNotBe null

            val ferdigstilt = output.find { it["status"].textValue() == "sendt" }!!
            ferdigstilt["@event_name"].textValue() shouldBe "eksternVarslingStatusOppdatert"
            ferdigstilt["varselId"].textValue() shouldBe oppgave.varselId
            ferdigstilt["ident"].textValue() shouldBe ident
            ferdigstilt["kanal"].textValue() shouldBe "SMS"
            ferdigstilt["renotifikasjon"].booleanValue() shouldBe false
            ferdigstilt["tidspunkt"].textValue().let { ZonedDateTime.parse(it) } shouldNotBe null
            ferdigstilt["batch"].booleanValue() shouldBe false
        }
    }

    @Test
    fun `sender status oppdatert event for hvert varsel i batch`() {
        val sendingsId = UUID.randomUUID().toString()
        val varselId1 = UUID.randomUUID().toString()
        val varselId2 = UUID.randomUUID().toString()

        val beskjed1 = createVarsel(varselId1, Varseltype.Beskjed)
        val beskjed2 = createVarsel(varselId2, Varseltype.Beskjed)

        val eksternVarsling = sendtEksternVarsling(sendingsId, ident, varsler = listOf(beskjed1, beskjed2))

        repository.insertEksternVarsling(eksternVarsling)

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, FERDIGSTILT, kanal = "SMS"))

        mockProducer.verifyOutput { output ->
            output.filter { it["status"].textValue() == "sendt" }.size shouldBe 2

            output.find { it["varselId"].textValue() == varselId1 }.let {
                it.shouldNotBeNull()
                it["@event_name"].textValue() shouldBe "eksternVarslingStatusOppdatert"
                it["status"].textValue() shouldBe "sendt"
                it["batch"].booleanValue() shouldBe true
            }

            output.find { it["varselId"].textValue() == varselId2 }.let {
                it.shouldNotBeNull()
                it["@event_name"].textValue() shouldBe "eksternVarslingStatusOppdatert"
                it["status"].textValue() shouldBe "sendt"
                it["batch"].booleanValue() shouldBe true
            }
        }
    }

    @Test
    fun `sjekker om status kommer fra renotifikasjon`() {

        val sendingsId1 = UUID.randomUUID().toString()
        val sendingsId2 = UUID.randomUUID().toString()
        val sendingsId3 = UUID.randomUUID().toString()
        val sendingsId4 = UUID.randomUUID().toString()

        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId1, ident))
        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId2, ident))
        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId3, ident))
        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId4, ident))

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId1, OVERSENDT, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId1,
                FERDIGSTILT,
                kanal = "SMS",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId2, OVERSENDT, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId2, FERDIGSTILT, kanal = "SMS", tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId2,
                FERDIGSTILT,
                kanal = "SMS",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId2,
                FERDIGSTILT,
                kanal = "EPOST",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId3, OVERSENDT, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId3, FEILET, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId3,
                FERDIGSTILT,
                kanal = "SMS",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId4, OVERSENDT, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId4, FEILET, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId4, INFO, tidspunktZ = nowAtUtc().plusDays(1)))

        val status1 = repository.getEksternVarsling(sendingsId1)?.eksternStatus
        status1!!.sendt shouldBe true
        status1.renotifikasjonSendt shouldBe false

        val status2 = repository.getEksternVarsling(sendingsId2)?.eksternStatus
        status2!!.sendt shouldBe true
        status2.renotifikasjonSendt shouldBe true

        val status3 = repository.getEksternVarsling(sendingsId3)?.eksternStatus
        status3!!.sendt shouldBe true
        status3.renotifikasjonSendt shouldBe true

        val status4 = repository.getEksternVarsling(sendingsId4)?.eksternStatus
        status4!!.sendt shouldBe false
        status4.renotifikasjonSendt shouldBe false


        mockProducer.verifyOutput { output ->
            output.filter {
                it["status"].textValue() == "sendt" && it["renotifikasjon"].asBoolean()
            }.size shouldBe 3

            output.filter {
                it["status"].textValue() == "sendt" && it["renotifikasjon"].asBoolean().not()
            }.size shouldBe 2

            output.filter {
                it["renotifikasjon"] == null
            }.size shouldBe 7
        }
    }

    @Test
    fun `sender med feilmelding for status feilet til kafka`() {

        val sendingsId = UUID.randomUUID().toString()

        repository.insertEksternVarsling(sendtEksternVarsling(sendingsId, ident))

        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, OVERSENDT, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(eksternVarslingStatus(sendingsId, INFO, tidspunktZ = nowAtUtc()))
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId,
                FERDIGSTILT,
                kanal = "SMS",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )
        testBroadcaster.broadcastJson(
            eksternVarslingStatus(
                sendingsId,
                FEILET,
                melding = "Ugyldig kontaktinfo",
                tidspunktZ = nowAtUtc().plusDays(1)
            )
        )

        mockProducer.verifyOutput { output ->
            output.filter {
                it["status"].textValue() != "feilet"
            }.forEach {
                it["feilmelding"].shouldBeNull()
            }

            output.filter {
                it["status"].textValue() == "feilet"
            }.also {
                it.size shouldBe 1
            }.first().let {
                it["feilmelding"].textValue() shouldBe "Ugyldig kontaktinfo"
            }
        }
    }

    private fun sendtEksternVarsling(
        sendingsId: String,
        ident: String,
        varsler: List<Varsel> = listOf(createVarsel()),
        sendingsstatus: Sendingsstatus = Sendingsstatus.Sendt
    ) = eksternVarslingDBRow(
        sendingsId = sendingsId,
        ident = ident,
        status = sendingsstatus,
        varsler = varsler
    )

}

private fun MockProducer<String, String>.verifyOutput(verifier: (List<JsonNode>) -> Unit) {
    val objectMapper = jacksonObjectMapper()
    history().map { it.value() }.map { objectMapper.readTree(it) }.let(verifier)
}
