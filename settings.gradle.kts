rootProject.name = "tms-ekstern-varsling"

dependencyResolutionManagement {
    pluginManagement {
        repositories {
            gradlePluginPortal()
            maven(url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release"))
        }
    }
}