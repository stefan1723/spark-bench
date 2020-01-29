name         := "spark-bench"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"

libraryDependencies += "org.scala-lang" %% "scala-library" % "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.3.2"
//libraryDependencies += "org.apache.spark" % "spark-mllib-local_2.11" % "2.3.2"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

resolvers += Resolver.mavenLocal

