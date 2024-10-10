package no.nav.tms.ekstern.varsling.status

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotliquery.queryOf
import no.nav.tms.ekstern.varsling.bestilling.EksternVarslingRepository
import no.nav.tms.ekstern.varsling.bestilling.createVarsel
import no.nav.tms.ekstern.varsling.bestilling.eksternVarslingDBRow
import no.nav.tms.ekstern.varsling.setup.LocalPostgresDatabase
import no.nav.tms.kafka.application.MessageBroadcaster
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

class BehandletAvLegacySubscriberTest {
    private val database = LocalPostgresDatabase.cleanDb()
    private val repository = EksternVarslingRepository(database)

    private val ident = "12345678901"

    private val mockProducer = MockProducer(
        false,
        StringSerializer(),
        StringSerializer()
    )

    private val testBroadcaster =
        MessageBroadcaster(
            listOf(
                BehandletAvLegacySubscriber(repository),
            )
        )

    @BeforeEach
    fun resetDb() {
        database.update { queryOf("delete from ekstern_varsling") }
        mockProducer.clear()
    }


    @Test
    fun `Markerer varsler som er behandlet av tms-ekstern-varselbestiller`() {

        val melding = "Sendt via epost"
        val distribusjonsId = 123L
        val kanal = "EPOST"

        val sendingsId = UUID.randomUUID().toString()

        val varsel = createVarsel(UUID.randomUUID().toString())

        repository.insertEksternVarsling(eksternVarslingDBRow(sendingsId, ident, varsler = listOf(varsel)))

        val doknotEvent = eksternVarslingStatus(
            bestillerAppnavn = "annen-app",
            eventId = varsel.varselId,
            status = DoknotifikasjonStatusEnum.OVERSENDT,
            melding = melding,
            distribusjonsId = distribusjonsId,
            kanal = kanal
        )

        testBroadcaster.broadcastJson(doknotEvent)

        val varsling = repository.getEksternVarsling(sendingsId)

        varsling.shouldNotBeNull()

        varsling.varsler.first().behandletAvLegacy shouldBe true

        varsling.eksternStatus.shouldBeNull()
        mockProducer.history().size shouldBe 0
    }
}
