package no.nav.tms.ekstern.varsling

import kotlinx.coroutines.runBlocking
import no.nav.tms.common.kubernetes.PodLeaderElection
import no.nav.tms.ekstern.varsling.bestilling.EksternVarselRepository
import no.nav.tms.ekstern.varsling.bestilling.InaktivertVarselSubscriber
import no.nav.tms.ekstern.varsling.bestilling.OpprettetVarselSubscriber
import no.nav.tms.ekstern.varsling.bestilling.PeriodicVarselSender
import no.nav.tms.ekstern.varsling.setup.Flyway
import no.nav.tms.ekstern.varsling.setup.PostgresDatabase
import no.nav.tms.ekstern.varsling.setup.initializeKafkaProducer
import no.nav.tms.kafka.application.KafkaApplication
import org.apache.kafka.clients.producer.KafkaProducer


fun main() {
    val eksternVarselRepository = EksternVarselRepository(PostgresDatabase())
    val environment = Environment()

    val varselSender = PeriodicVarselSender(
        repository = eksternVarselRepository,
        kafkaProducer = initializeKafkaProducer(),
        kafkaTopic = environment.varselTopic,
        leaderElection = PodLeaderElection()
    )

    KafkaApplication.build {
        kafkaConfig {
            groupId = environment.groupId
            readTopics(environment.varselTopic)
        }
        subscribers(
            OpprettetVarselSubscriber(eksternVarselRepository),
            InaktivertVarselSubscriber(eksternVarselRepository),
        )
        onStartup {
            Flyway.runFlywayMigrations()
            varselSender.start()
        }
        onShutdown {
            runBlocking {
                varselSender.stop()
            }
        }

    }.start()
}

