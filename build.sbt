name := "ngsstream"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.sonatypeRepo("snapshots")

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.2" changing()
libraryDependencies += "com.github.biopet" %% "spark-utils" % "0.2" changing()

//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.broadinstitute" % "gatk-bwamem-jni" % "1.0.3"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark2" % "0.22.0"
libraryDependencies += "org.bdgenomics.utils" %% "utils-misc-spark2" % "0.2.13"

mainClass in assembly := Some("ngsstream.Main")

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "pom.xml" =>
    MergeStrategy.first
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case _ => MergeStrategy.first
}
