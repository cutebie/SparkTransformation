lazy val root = (project in file(".")).
  settings(
    name := "SparkTransformation",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("SparkJobOnAzure.ColumnModification")        
  )

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)

//libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.2.0"

libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.3"

// META-INF discarding
/*mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
*/


