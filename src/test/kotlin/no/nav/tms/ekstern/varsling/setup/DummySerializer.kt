package no.nav.tms.ekstern.varsling.setup

import org.apache.kafka.common.serialization.Serializer

class DummySerializer<T>: Serializer<T> {

    override fun serialize(topic: String?, data: T): ByteArray? = null
}
