plugins {
  kotlin ("jvm") version "1.7.21"
  application
}

group = "pt.davidafsilva.vertx"
version = "1.0.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:4.4.4"))
  implementation("io.vertx:vertx-kafka-client")
}

kotlin {
  jvmToolchain(17)
}

application {
  mainClass.set("pt.davidafsilva.vertx.MainKt")
}

tasks.named<JavaExec>("run") {
  standardInput = System.`in`
}
