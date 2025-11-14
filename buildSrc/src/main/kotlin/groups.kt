import default.*

// Managed by tms-dependency-admin. Overrides and additions should be placed in separate file

object Flyway: FlywayDefaults
object Hikari: HikariDefaults
object JacksonDatatype: JacksonDatatypeDefaults
object JunitJupiter: JunitJupiterDefaults
object JunitPlatform: JunitPlatformDefaults
object Jjwt: JjwtDefaults
object Kafka: KafkaDefaults
object Kluent: KluentDefaults
object Kotest: KotestDefaults
object Kotlin: KotlinDefaults
object KotlinLogging: KotlinLoggingDefaults
object Kotlinx: KotlinxDefaults
object KotliQuery: KotliQueryDefaults
object Ktor {
    object Server: KtorDefaults.ServerDefaults
    object Client: KtorDefaults.ClientDefaults
    object Serialization: KtorDefaults.SerializationDefaults
    object Test: KtorDefaults.TestDefaults
}
object Logback: LogbackDefaults
object Logstash: LogstashDefaults
object Micrometer: MicrometerDefaults
object Mockk: MockkDefaults
object Postgresql: PostgresqlDefaults
object Prometheus: PrometheusDefaults
object TestContainers: TestContainersDefaults
object TmsCommonLib: TmsCommonLibDefaults
object TmsKafkaTools: TmsKafkaToolsDefaults
object TmsKtorTokenSupport: TmsKtorTokenSupportDefaults

object TmsJarBundling {
    const val plugin = "no.nav.tms.jar-bundling"
}
