import sbtassembly.AssemblyPlugin.autoImport.assembly

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val coordinator = (project in file("coordinator")).settings(
  assembly / assemblyJarName := "coordinator.jar",
  assembly / assemblyMergeStrategy := {
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)
lazy val worker = (project in file("worker")).settings(
  assembly / assemblyJarName := "worker.jar",
  assembly / mainClass := Some("Worker"),
  assembly / assemblyMergeStrategy := {
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-multi-runner"
  )
  .aggregate(coordinator, worker)

resolvers += Resolver.mavenLocal

ThisBuild / libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.5.0-SNAPSHOT"
ThisBuild / libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.0"
ThisBuild / libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.5"
