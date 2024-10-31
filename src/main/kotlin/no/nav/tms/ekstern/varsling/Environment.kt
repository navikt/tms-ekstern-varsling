package no.nav.tms.ekstern.varsling

import no.nav.tms.common.util.config.BooleanEnvVar.getEnvVarAsBoolean
import no.nav.tms.common.util.config.StringEnvVar.getEnvVar

data class Environment(
    val varselTopic: String = getEnvVar("VARSEL_TOPIC"),
    val doknotTopic: String = getEnvVar("DOKNOTIFIKASJON_TOPIC"),
    val doknotStoppTopic: String = getEnvVar("DOKNOTIFIKASJON_STOP_TOPIC"),
    val groupId: String = getEnvVar("KAFKA_GROUP_ID"),
    val enableBatch: Boolean = getEnvVarAsBoolean("ENABLE_BATCH")
)
