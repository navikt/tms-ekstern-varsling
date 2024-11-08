package no.nav.tms.ekstern.varsling.bestilling

import com.fasterxml.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

object ZonedDateTimeHelper {
    fun nowAtUtc(): ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS)

    fun JsonNode.asZonedDateTime(): ZonedDateTime {
        return parseZonedDateTimeDefaultUtc(asText())
    }

    fun JsonNode.asOptionalZonedDateTime(): ZonedDateTime? {
        return takeIf(JsonNode::isTextual)
            ?.asText()
            ?.takeIf(String::isNotEmpty)
            ?.let { parseZonedDateTimeDefaultUtc(it) }
    }



    private fun parseZonedDateTimeDefaultUtc(dateTimeString: String): ZonedDateTime {
        return try {
            return ZonedDateTime.parse(dateTimeString)
        } catch (e: DateTimeParseException) {
            LocalDateTime.parse(dateTimeString).atZone(ZoneId.of("UTC"))
        }
    }
}

object LocalTimeHelper {
    fun nowAt(timezone: ZoneId) = LocalTime.now(timezone).truncatedTo(ChronoUnit.MILLIS)
}
