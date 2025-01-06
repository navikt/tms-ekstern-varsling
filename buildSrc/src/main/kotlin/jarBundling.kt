import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaApplication
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.jvm.tasks.Jar
import java.io.File

// Managed by tms-dependency-admin.

abstract class JarBundling : Plugin<Project> {

    override fun apply(target: Project) {

        val configureTask = target.tasks.register("configureJar", ConfigureJarTask::class.java) {
            this.application = target

            val classes = target.tasks.named("classes")

            dependsOn(classes)
        }

        val packageTask = target.tasks.register("packageJar", BundleJarsTask::class.java) {
            this.application = target
        }

        target.tasks.withType(Jar::class.java) {
            dependsOn(configureTask)
            finalizedBy(packageTask)
        }
    }
}

abstract class ConfigureJarTask : DefaultTask() {
    @Input
    lateinit var application: Project

    @TaskAction
    fun action() {
        application.tasks.withType(Jar::class.java) {

            val javaApplication = application.extensions.getByType(JavaApplication::class.java)

            val mainClassName = javaApplication.mainClass

            archiveBaseName.set("app")
            manifest {
                val classpath = application.configurations.getByName("runtimeClasspath")
                attributes["Main-Class"] = mainClassName
                attributes["Class-Path"] = classpath.joinToString(separator = " ") {
                    it.name
                }
            }
        }
    }
}

abstract class BundleJarsTask : DefaultTask() {
    @Input
    lateinit var application: Project

    @TaskAction
    fun action() {
        val classpath = application.configurations.getByName("runtimeClasspath")

        classpath.forEach {
            val file = File("${application.layout.buildDirectory.get()}/libs/${it.name}")
            if (!file.exists()) it.copyTo(file)
        }
    }
}
