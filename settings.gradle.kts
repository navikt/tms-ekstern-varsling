rootProject.name = "tms-ekstern-varsling"

dependencyResolutionManagement {
    pluginManagement {
        repositories {
            gradlePluginPortal()
            maven(url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release"))
        }
    }
    repositories {
        mavenCentral()
        maven(url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release"))
        versionCatalogs {
            create("adminLib") {
                from("no.nav.tms:tms-dependency-admin:20261029.13.1")
            }
        }
    }
}