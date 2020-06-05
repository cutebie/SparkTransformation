package SparkJobOnAzure.Utils

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object FileUtils {

  /*
    Read the parquet file into DataFrame
    input: SparkSession, parquet file path, type as json, csv or parquet is supported for now
    output: DataFrame
    */
  def readFromFile(spark:SparkSession, inputFilePath:String, fileOfType:String) : DataFrame = {
    println("readFromFile() " + inputFilePath)
    // read parquet file
    var parqDF:DataFrame = null
    if (fileOfType == "parquet") {
      parqDF = spark.read.parquet(inputFilePath)
    } else if (fileOfType == "csv") {
      parqDF = spark.read.csv(inputFilePath)
    } else if (fileOfType == "json") {
    parqDF = spark.read.json(inputFilePath)
  }
    parqDF
  }

  /*
  Write the DataFrame to the parquet file
  input: DataFrame, parquet output file path
  output: DataFrame
  */
  def writeToFile(df:DataFrame, outputFilePath:String, numPartition:Int) : Boolean = {
    println("writeToFile() " + outputFilePath)

    // write out parquet file
    if (df == null) {
      false
    }

    df.coalesce(numPartition).write.parquet(outputFilePath)
    true
  }

  /*
  Export the schema to json file
  input: DataFrame, output file path to store the schema in json format
  output: None
   */
  def exportSchema(df:DataFrame, outpath:String) {
    val schemaJson = df.schema.json
    //println(schemaJson)
    val writer = new PrintWriter(new File(outpath))
    writer.write(schemaJson)
    writer.close()
  }
}
