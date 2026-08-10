package no.nav.tms.ekstern.varsling.common

import kotliquery.Row

inline fun <reified T: Enum<T>> Row.enum(name: String): T {
    return string(name)
        .let { enumValueOf<T>(it) }
}
