package no.nav.tms.ekstern.varsling

import no.nav.tms.common.util.config.BooleanEnvVar.getEnvVarAsBoolean
import no.nav.tms.common.util.config.StringEnvVar.getEnvVar

data class Environment(
    val varselTopic: String = "min-side.brukervarsel-v1",
    val groupId: String = "ekstern-varsling-02"
)
