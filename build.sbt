import xerial.sbt.Sonatype._

import scala.sys.process.Process
import scala.util.{Failure, Try}

val sparkVersion = "3.5.4"
val deltaLakeVersion = "3.2.1"
val scala2Version = "2.12.18"
val githubUser = "fernanluyano"
val projectName = "db-zpark"
val email = "fernando.berlanga1@gmail.com"

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / lintUnusedKeysOnLoad := false

ThisBuild / scalaVersion := scala2Version
ThisBuild / version := getVersion.value
ThisBuild / organization := s"io.github.$githubUser"
ThisBuild / description := "A code-first approach to manage Spark/Scala jobs, built on the ZIO framework and geared for Databricks environments"
ThisBuild / licenses := List("MIT" -> new URL("https://opensource.org/license/mit"))
ThisBuild / homepage := Some(url(s"https://github.com/$githubUser"))
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

ThisBuild / publishMavenStyle := true
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / sonatypeRepository := sonatypeCentralHost
ThisBuild / publishTo := sonatypePublishToBundle.value

/**
 * Normally the dependencies included in the Databricks Runtime (latest LTS, not ML).
 * See https://docs.databricks.com/aws/en/release-notes/
 */
lazy val providedDependencies = Seq(
  "io.delta" %% "delta-spark" % deltaLakeVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided
)

lazy val nonProvidedDependencies = Seq(
  "dev.zio" %% "zio" % "2.1.16",
  "dev.zio" %% "zio-logging" % "2.5.0",
)

lazy val root = (project in file("."))
  .settings(
    name := projectName,
    idePackagePrefix := Some("dev.fb.dbzpark"),
    Test / scalaSource := baseDirectory.value / "src/test/scala",
    Compile / scalaSource := baseDirectory.value / "src/main/scala",
    libraryDependencies ++= (providedDependencies ++ nonProvidedDependencies)
  )


lazy val getVersion = settingKey[String]("get current version")
getVersion := {
  val branchName = Try(Process("git branch --show-current"))
    .orElse(Try(Process("git rev-parse --abbrev-ref HEAD"))) match {
      case Failure(exception) => throw exception
      case scala.util.Success(value) => value.lineStream.head.trim
  }
  val branchParts = branchName.split("/").take(2)
  val head = branchParts.head.trim
  val tail = branchParts.last.trim
  head match {
    case "release" => tail
    case "develop" | "master" => s"0.0.0-$head-SNAPSHOT"
    case _ => s"0.0.0-$tail-SNAPSHOT"
  }
}