package no.nav.tms.ekstern.varsling

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
import no.nav.tms.kafka.application.KafkaApplication


fun main() {
    val eksternVarselRepository = EksternVarslingRepository(PostgresDatabase())
    val environment = Environment()

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

        healthCheck("Varselsender", varselSender::isHealthy)

    }.start()
}

object TmsEksternVarsling {
    const val appnavn = "tms-ekstern-varsling"
}
