package no.nav.tms.ekstern.varsling

import kotlinx.coroutines.runBlocking
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.common.postgres.Postgres
import no.nav.tms.ekstern.varsling.bestilling.*
import no.nav.tms.ekstern.varsling.setup.initializeKafkaProducer
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.ekstern.varsling.status.EksternVarslingStatusSubscriber
import no.nav.tms.kafka.application.Domain
import no.nav.tms.kafka.application.KafkaApplication
import org.flywaydb.core.Flyway


fun main() {
    val environment = Environment()

    val database = Postgres.connectToJdbcUrl(environment.jdbcUrl) {
        transactionIsolation = "TRANSACTION_READ_COMMITTED"
    }
    val eksternVarselRepository = EksternVarslingRepository(database)

    val kanalDecider = PreferertKanalDecider(
        environment.smsSendingsStart,
        environment.smsSendingsEnd,
        environment.smsTimezone
    )

    val statusOppdatertProducer = EksternVarslingOppdatertProducer(
        kafkaProducer = initializeKafkaProducer(),
        topicName = environment.varselTopic
    )

    val varselSender = PeriodicVarselSender(
        repository = eksternVarselRepository,
        kanalDecider = kanalDecider,
        kafkaProducer = initializeKafkaProducer(useAvroSerializer = true),
        doknotTopic = environment.doknotTopic,
        leaderElection = PodLeaderElection(),
        statusProducer = statusOppdatertProducer
    )

    val eksternStatusUpdater = EksternStatusUpdater(
        repository = eksternVarselRepository,
        eksternVarslingOppdatertProducer = statusOppdatertProducer
    )

    KafkaApplication.build {
        kafkaConfig {
            groupId = environment.groupId
            readTopics(environment.varselTopic)
        }
        subscribers(
            OpprettetVarselSubscriber(eksternVarselRepository, statusOppdatertProducer, environment.enableBatch),
            InaktivertVarselSubscriber(
                eksternVarselRepository,
                initializeKafkaProducer(useAvroSerializer = true),
                environment.doknotStoppTopic
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
        }

        onShutdown {
            runBlocking {
                varselSender.stop()
                statusOppdatertProducer.flushAndClose()
            }
        }

        healthCheck("Varselsender", varselSender::isHealthy)

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
