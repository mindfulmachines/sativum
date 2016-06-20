import sbt._

lazy val commonSettings = Seq(
  organization := "io.mindfulmachines",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.8"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided" exclude("org.apache.hadoop", "hadoop-client")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided"  excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4" % "provided"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"

libraryDependencies += "io.mindfulmachines" %% "peapod" % "0.7-SNAPSHOT"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

pomIncludeRepository := { _ => false }

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra :=
  <description>ETL framework for Spark and Scala.</description>
  <url>https://github.com/mindfulmachines/sativum</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>http://www.opensource.org/licenses/mit-license.php</url>
    </license>
  </licenses>
  <developers>
    <developer>
      <name>Marcin Mejran</name>
      <email>marcin@mindfulmachines.io</email>
      <organization>Mindful Machines</organization>
      <organizationUrl>http://www.mindfulmachines.io</organizationUrl>
    </developer>
    <developer>
      <name>Zoe Afshar</name>
      <email>zoe@mindfulmachines.io</email>
      <organization>Mindful Machines</organization>
      <organizationUrl>http://www.mindfulmachines.io</organizationUrl>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:git@github.com:mindfulmachines/sativum.git</connection>
    <developerConnection>scm:git:git@github.com:mindfulmachines/sativum.git</developerConnection>
    <url>git@github.com:mindfulmachines/sativum.git</url>
  </scm>

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Sativum"
  )