package no.nav.tms.ekstern.varsling.setup

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import com.fasterxml.jackson.module.kotlin.readValue
import com.zaxxer.hikari.HikariDataSource
import kotliquery.Query
import kotliquery.Row
import kotliquery.action.ListResultQueryAction
import kotliquery.action.NullableResultQueryAction
import kotliquery.sessionOf
import kotliquery.using
import org.postgresql.util.PGobject

interface Database {
    val dataSource: HikariDataSource
    fun update(queryBuilder: () -> Query) {
        using(sessionOf(dataSource)) {
            it.run(queryBuilder.invoke().asUpdate)
        }
    }

    fun <T> list(action: () -> ListResultQueryAction<T>): List<T> =
        using(sessionOf(dataSource)) {
            it.run(action.invoke())
        }

    fun <T> singleOrNull(action: () -> NullableResultQueryAction<T>): T? =
        using(sessionOf(dataSource)) {
            it.run(action.invoke())
        }


    fun batch(statement: String, params: List<Map<String, Any?>>) =
        using(sessionOf(dataSource)) {
            it.batchPreparedNamedStatement(statement, params)
        }
}

fun defaultObjectMapper() = jacksonMapperBuilder()
    .addModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

inline fun <reified T> Row.json(label: String, objectMapper: ObjectMapper = defaultObjectMapper()): T {
    return objectMapper.readValue(string(label))
}

inline fun <reified T> Row.jsonOrNull(label: String, objectMapper: ObjectMapper = defaultObjectMapper()): T? {
    return stringOrNull(label)?.let { objectMapper.readValue(it) }
}

fun Any?.toJsonb(objectMapper: ObjectMapper = defaultObjectMapper()): PGobject? {
    return if (this == null) {
        null
    } else {
        objectMapper.writeValueAsString(this).let {
            PGobject().apply {
                type = "jsonb"
                value = it
            }
        }
    }
}
