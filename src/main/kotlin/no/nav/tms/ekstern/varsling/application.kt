package no.nav.tms.ekstern.varsling

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.routing.*
import kotlinx.coroutines.runBlocking
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.ekstern.varsling.bestilling.*
import no.nav.tms.ekstern.varsling.setup.Flyway
import no.nav.tms.ekstern.varsling.setup.PostgresDatabase
import no.nav.tms.ekstern.varsling.setup.initializeKafkaProducer
import no.nav.tms.ekstern.varsling.status.BehandletAvLegacySubscriber
import no.nav.tms.ekstern.varsling.status.EksternStatusUpdater
import no.nav.tms.ekstern.varsling.status.EksternVarslingOppdatertProducer
import no.nav.tms.ekstern.varsling.status.EksternVarslingStatusSubscriber
import no.nav.tms.kafka.application.AppHealth
import no.nav.tms.kafka.application.KafkaApplication


fun main() {
    val eksternVarselRepository = EksternVarslingRepository(PostgresDatabase())
    val environment = Environment()

    val varselSender = PeriodicVarselSender(
        repository = eksternVarselRepository,
        kafkaProducer = initializeKafkaProducer(useAvroSerializer = true),
        doknotTopic = environment.doknotTopic,
        leaderElection = PodLeaderElection()
    )

    val statusOppdatertProducer = EksternVarslingOppdatertProducer(
        kafkaProducer = initializeKafkaProducer(),
        topicName = environment.varselTopic
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
            OpprettetVarselSubscriber(eksternVarselRepository, environment.enableBatch),
            InaktivertVarselSubscriber(
                eksternVarselRepository,
                initializeKafkaProducer(useAvroSerializer = true),
                environment.doknotStoppTopic
            ),
            BehandletAvLegacySubscriber(eksternVarselRepository),
            EksternVarslingStatusSubscriber(eksternStatusUpdater),
        )
        onStartup {
            Flyway.runFlywayMigrations()
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

        ktorModule {
            routing {
                post("/application/kill") {
                    KotlinLogging.logger {  }.info { "Killing application" }
                    ApplicationHealth.kill()
                }
            }
        }

        healthCheck(varselSender::isHealthy)
        healthCheck(ApplicationHealth::health)

    }.start()
}

object TmsEksternVarsling {
    const val appnavn = "tms-ekstern-varsling"
}

object ApplicationHealth {
    private var appHealth = AppHealth.Healthy

    fun kill() {
        appHealth = AppHealth.Unhealthy
    }

    val health = appHealth
}
