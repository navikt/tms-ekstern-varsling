package no.nav.tms.ekstern.varsling.common

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.treeToValue
import kotliquery.Row
import java.time.ZonedDateTime

inline fun <reified T: Enum<T>> Row.enum(name: String): T {
    return string(name)
        .let { enumValueOf<T>(it) }
}

class JsonAgg(private val jsonNode: JsonNode) {
    fun <T> map(mapper: (JsonAggElement) -> T): List<T> {
        return jsonNode
            .map { JsonAggElement(it) }
            .map(mapper)
    }

    companion object {
        fun Row.jsonAgg(name: String): JsonAgg {
            return string(name)
                .let(objectMapper::readTree)
                .let { JsonAgg(it) }
        }

        val objectMapper = jacksonObjectMapper()
    }

    class JsonAggElement(val jsonNode: JsonNode) {
        fun string(name: String): String {
            return jsonNode[name.lowercase()]
                .takeIf { it.isTextual }
                ?.asText()
                ?: throw JsonAggColumnException(name)
        }

        fun stringOrNull(name: String): String? {
            return jsonNode[name.lowercase()]
                .takeIf { it.isTextual }
                ?.asText()
        }

        fun boolean(name: String): Boolean {
            return jsonNode[name.lowercase()]
                .takeIf { it.isBoolean }
                ?.asBoolean()
                ?: throw JsonAggColumnException(name)
        }

        fun zonedDateTime(name: String): ZonedDateTime {
            return jsonNode[name.lowercase()]
                .takeIf { it.isTextual }
                ?.asText()
                ?.let(ZonedDateTime::parse)
                ?: throw JsonAggColumnException(name)
        }

        fun zonedDateTimeOrNull(name: String): ZonedDateTime? {
            return jsonNode[name.lowercase()]
                .takeIf { it.isTextual }
                ?.asText()
                ?.let(ZonedDateTime::parse)
        }

        inline fun <reified T> json(name: String ): T {
            return jsonNode[name.lowercase()]
                ?.let { objectMapper.treeToValue(it) }
                ?: throw JsonAggColumnException(name)
        }
    }
}

class JsonAggColumnException(name: String):
    IllegalArgumentException("Fant ikke felt '$name' i json_agg resultat. Påse at feltet finnes og har riktig type")
