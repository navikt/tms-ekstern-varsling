import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    alias(adminLib.plugins.kotlin)
    alias(adminLib.plugins.tms.jarBundling)
    application
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

jarBundling {
    applicationName = "app"
    outputDirectory = "libs"
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
    maven {
        url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }
    mavenLocal()
}

dependencies {
    runtimeOnly(libs.ktor.server.auth.jwt)

    runtimeOnly(adminLib.kotlinLogging)
    runtimeOnly(adminLib.ktor.server.core)
    runtimeOnly(adminLib.ktor.server.statusPages)
    runtimeOnly(adminLib.ktor.server.auth)
    runtimeOnly(adminLib.ktor.client.contentNegotiation)
    runtimeOnly(adminLib.ktor.serialization.jackson)

    implementation(libs.avroSerializer)
    implementation(libs.doknotifikasjonAvro)

    implementation(adminLib.flyway.core)
    implementation(adminLib.flyway.postgres)
    implementation(adminLib.hikariCp)
    implementation(adminLib.jackson.datatypeJsr310)
    implementation(adminLib.kafka.clients)
    implementation(adminLib.kotliQuery)
    implementation(adminLib.ktor.server.netty)
    implementation(adminLib.ktor.server.html)
    implementation(adminLib.ktor.client.apache5)
    implementation(adminLib.logback.encoder)
    implementation(adminLib.postgresql)
    implementation(adminLib.tms.commonLib.utils)
    implementation(adminLib.tms.commonLib.kubernetes)
    implementation(adminLib.tms.commonLib.observability)
    implementation(adminLib.tms.commonLib.postgres)
    implementation(adminLib.tms.commonLib.teamLogger)
    implementation(adminLib.tms.kafka.application)
    implementation(adminLib.tms.kafka.producerUtils)
    implementation(adminLib.prometheus.metricsCore)
    implementation(adminLib.jackson.moduleKotlin)

    testImplementation(adminLib.junit.launcher)
    testImplementation(adminLib.junit.api)
    testImplementation(adminLib.junit.engine)
    testImplementation(adminLib.mockk)
    testImplementation(adminLib.testContainers.postgresql)
    testImplementation(adminLib.kotest.runner)
    testImplementation(adminLib.kotest.assertion)
}

application {
    mainClass.set("no.nav.tms.ekstern.varsling.ApplicationKt")
}

tasks {
    withType<Test> {
        useJUnitPlatform()
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
    }
}
