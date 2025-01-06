plugins {
    `kotlin-dsl`
    `maven-publish`
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

gradlePlugin {
    plugins {
        create("jar-bundling") {
            id = "no.nav.tms.jar-bundling"
            implementationClass = "JarBundling"
        }
    }
}

publishing {
    repositories {
        mavenLocal()
    }
}
