package no.nav.tms.ekstern.varsling

import io.confluent.kafka.serializers.KafkaAvroSerializer
import kotlinx.coroutines.runBlocking
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.postgres.Postgres
import no.nav.tms.ekstern.varsling.bestilling.*
import no.nav.tms.ekstern.varsling.recordqueue.DoknotStopQueueRepository
import no.nav.tms.ekstern.varsling.recordqueue.PeriodicDoknotStoppQueueProcessor
import no.nav.tms.ekstern.varsling.recordqueue.PeriodicStatusOppdatertQueueProcessor
import no.nav.tms.ekstern.varsling.recordqueue.StatusOppdatertQueueRepository
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.ekstern.varsling.status.EksternVarslingStatusSubscriber
import no.nav.tms.kafka.application.Domain
import no.nav.tms.kafka.application.KafkaApplication
import no.nav.tms.kafka.producer.KafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer
import org.flywaydb.core.Flyway


fun main() {
    val environment = Environment()

    val database = Postgres.connectToJdbcUrl(environment.jdbcUrl)
    val eksternVarselRepository = EksternVarslingRepository(database)

    val doknotStopQueueRepository = DoknotStopQueueRepository(database)
    val statusOppdatertQueueRepository = StatusOppdatertQueueRepository(database)

    val kanalDecider = PreferertKanalDecider(
        environment.smsSendingsStart,
        environment.smsSendingsEnd,
        environment.smsTimezone
    )

    val statusOppdatertProducer = EksternVarslingOppdatertProducer(
        queueRepository = statusOppdatertQueueRepository,
    )

    val leaderElection = PodLeaderElection()

    val varselSender = PeriodicVarselSender(
        varslingRepository = eksternVarselRepository,
        kanalDecider = kanalDecider,
        kafkaProducer = KafkaProducerBuilder.producer(
            keySerializer = StringSerializer::class,
            valueSerializer = KafkaAvroSerializer::class
        ),
        doknotTopic = environment.doknotTopic,
        leaderElection = leaderElection,
        statusProducer = statusOppdatertProducer
    )

    val eksternStatusUpdater = EksternStatusUpdater(
        repository = eksternVarselRepository,
        eksternVarslingOppdatertProducer = statusOppdatertProducer
    )

    val doknotStopQueueProcessor = PeriodicDoknotStoppQueueProcessor(
        repository = doknotStopQueueRepository,
        recordProducer = KafkaProducerBuilder.producer(
            keySerializer = StringSerializer::class,
            valueSerializer = KafkaAvroSerializer::class
        ),
        leaderElection = leaderElection,
        doknotStoppTopic = environment.doknotStoppTopic,
    )

    val statusOppdatertQueueProcessor = PeriodicStatusOppdatertQueueProcessor(
        repository = statusOppdatertQueueRepository,
        recordProducer = KafkaProducerBuilder.stringProducer(),
        leaderElection = leaderElection,
        varseltopic = environment.varseltopic
    )

    KafkaApplication.build {
        kafkaConfig {
            groupId = environment.groupId
            readTopics(environment.varseltopic)
        }
        subscribers(
            OpprettetVarselSubscriber(eksternVarselRepository, statusOppdatertProducer, environment.enableBatch),
            InaktivertVarselSubscriber(
                eksternVarselRepository,
                doknotStopQueueRepository
            ),
            EksternVarslingStatusSubscriber(eksternStatusUpdater),
        )

        onStartup {
            Flyway.configure()
                .dataSource(database.dataSource)
                .load()
                .migrate()
        }

        onReady {
            varselSender.start()
            doknotStopQueueProcessor.start()
            statusOppdatertQueueProcessor.start()
        }

        onShutdown {
            runBlocking {
                varselSender.stop()
                doknotStopQueueProcessor.stop()
                doknotStopQueueProcessor.flushAndClose()
                statusOppdatertQueueProcessor.stop()
                statusOppdatertQueueProcessor.flushAndClose()
            }
        }

        healthCheck("Varselsender", varselSender::isHealthy)
        healthCheck("DoknotStopQueueProcessor", doknotStopQueueProcessor::isHealthy)
        healthCheck("StatusOppdatertQueueProcessor", statusOppdatertQueueProcessor::isHealthy)

        minSideMdc {
            domain = Domain.varsel

            idSupplier { message ->
                val varselIdNode = message.json.get("varselId")
                val eventIdNode = message.json.get("eventId")

                if (varselIdNode?.isTextual == true) {
                    varselIdNode.asText()
                } else if (eventIdNode?.isTextual == true) {
                    eventIdNode.asText()
                } else {
                    "N/A"
                }
            }

            producedBySupplier { message ->
                val produsentNode = message.json.get("produsent")

                if (produsentNode?.isObject == true) {
                    val cluster = produsentNode["cluster"].asText()
                    val namespace = produsentNode["namespace"].asText()
                    val appnavn = produsentNode["appnavn"].asText()

                    "$cluster:$namespace:$appnavn"
                } else {
                    val bestillerNode = message.json.get("bestillerAppnavn")

                    if (bestillerNode?.isTextual == true) {
                        bestillerNode.asText()
                    } else {
                        null
                    }
                }
            }
        }

    }.start()
}

object TmsEksternVarsling {
    const val appnavn = "tms-ekstern-varsling"
}
