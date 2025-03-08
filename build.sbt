import xerial.sbt.Sonatype.*

import scala.sys.process.Process
import scala.util.{Failure, Try}

val sparkVersion     = "3.5.4"
val deltaLakeVersion = "3.2.1"
val scala2Version    = "2.12.18"
val githubUser       = "fernanluyano"
val projectName      = "db-zpark"
val email            = "fernando.berlanga1@gmail.com"

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / lintUnusedKeysOnLoad := false

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq("--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")
ThisBuild / scalaVersion           := scala2Version
ThisBuild / version                := getVersion.value
ThisBuild / organization           := s"io.github.$githubUser"
ThisBuild / description            := "A code-first approach to manage Spark/Scala jobs, built on the ZIO framework and geared for Databricks environments"
ThisBuild / licenses               := List("MIT" -> new URL("https://opensource.org/license/mit"))
ThisBuild / homepage               := Some(url(s"https://github.com/$githubUser"))
ThisBuild / sonatypeProjectHosting := Some(GitHubHosting(githubUser, projectName, email))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/$githubUser/$projectName"),
    s"scm:git@github.com:$githubUser/$projectName.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = githubUser,
    name = "Fernando",
    email = email,
    url = url(s"https://github.com/$githubUser")
  )
)

ThisBuild / publishMavenStyle      := true
ThisBuild / versionScheme          := Some("early-semver")
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / sonatypeRepository     := sonatypeCentralHost
ThisBuild / publishTo              := sonatypePublishToBundle.value

/**
 * Normally the dependencies included in the Databricks Runtime (latest LTS, not ML), or expected to be provided by
 * clients. See https://docs.databricks.com/aws/en/release-notes/
 */
lazy val providedDependencies = Seq(
  "io.delta"         %% "delta-spark"     % deltaLakeVersion % Provided,
  "org.apache.spark" %% "spark-core"      % sparkVersion     % Provided,
  "org.apache.spark" %% "spark-sql"       % sparkVersion     % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion     % Provided,
  "org.apache.kafka"  % "kafka-clients"   % "3.9.0"          % Provided
)
lazy val nonProvidedDependencies = Seq(
  "dev.zio" %% "zio"         % "2.1.16",
  "dev.zio" %% "zio-logging" % "2.5.0",
  "dev.zio" %% "zio-json"    % "0.7.39"
)
lazy val testDependencies = Seq(
  "org.scalatest"     %% "scalatest"    % "3.2.19"    % Test,
  "org.scalatestplus" %% "mockito-3-4"  % "3.2.10.0"  % Test,
  "dev.zio"           %% "zio-test"     % "2.1.16"    % Test,
  "dev.zio"           %% "zio-test-sbt" % "2.1.16"    % Test
)
lazy val allDependencies = providedDependencies ++ nonProvidedDependencies ++ testDependencies

lazy val root = (project in file("."))
  .settings(
    name                  := projectName,
    idePackagePrefix      := Some("dev.fb.dbzpark"),
    Test / scalaSource    := baseDirectory.value / "src/test/scala",
    Compile / scalaSource := baseDirectory.value / "src/main/scala",
    libraryDependencies ++= allDependencies
  )

lazy val getVersion = settingKey[String]("get current version")
getVersion := {
  val branchName = Try(Process("git branch --show-current"))
    .orElse(Try(Process("git rev-parse --abbrev-ref HEAD"))) match {
    case Failure(exception)        => sys.env("BRANCH_NAME")
    case scala.util.Success(value) => value.lineStream.head.trim
  }
  println(s"Git Branch: $branchName")
  val branchParts = branchName.split("/").take(2)
  val head        = branchParts.head.trim
  val tail        = branchParts.last.trim
  head match {
    case "release"            => tail
    case "develop" | "master" => s"0.0.0-$head-SNAPSHOT"
    case _                    => s"0.0.0-$tail-SNAPSHOT"
  }
}
