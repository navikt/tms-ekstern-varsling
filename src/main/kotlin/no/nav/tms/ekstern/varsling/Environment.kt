package no.nav.tms.ekstern.varsling

import no.nav.tms.common.util.config.BooleanEnvVar.getEnvVarAsBoolean
import no.nav.tms.common.util.config.StringEnvVar.getEnvVar
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId

data class Environment(
    val varselTopic: String = getEnvVar("VARSEL_TOPIC"),
    val doknotTopic: String = getEnvVar("DOKNOTIFIKASJON_TOPIC"),
    val doknotStoppTopic: String = getEnvVar("DOKNOTIFIKASJON_STOP_TOPIC"),
    val groupId: String = getEnvVar("KAFKA_GROUP_ID"),
    val enableBatch: Boolean = getEnvVarAsBoolean("ENABLE_BATCH"),
    val smsSendingsStart: LocalTime = getEnvVar("SMS_SENDING_START").let(LocalTime::parse),
    val smsSendingsEnd: LocalTime = getEnvVar("SMS_SENDING_END").let(LocalTime::parse),
    val smsTimezone: ZoneId = getEnvVar("SMS_TIMEZONE").let(ZoneId::of)
)
