package no.nav.tms.ekstern.varsling

import no.nav.tms.ekstern.varsling.bestilling.EksternVarselRepository
import no.nav.tms.ekstern.varsling.bestilling.OpprettetVarselSubscriber
import no.nav.tms.ekstern.varsling.setup.Flyway
import no.nav.tms.ekstern.varsling.setup.PostgresDatabase
import no.nav.tms.kafka.application.KafkaApplication


fun main() {
    val eksternVarselRepository = EksternVarselRepository(PostgresDatabase())
    val environment = Environment()
    KafkaApplication.build {
        kafkaConfig {
            groupId = environment.groupId
            readTopics(environment.varselTopic)
        }
        subscribers(
            OpprettetVarselSubscriber(eksternVarselRepository)
        )
        onStartup {
            Flyway.runFlywayMigrations()
        }

    }.start()
}

