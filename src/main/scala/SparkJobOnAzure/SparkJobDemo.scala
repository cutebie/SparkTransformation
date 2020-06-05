package SparkJobOnAzure

import SparkJobOnAzure.Utils.{FileUtils, DatabricksUtils}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJobDemo {
  // hardcode for now
  val basedir = DatabricksUtils.basedir
  val inContainerName = "level2"
  val outContainerName = "level3"
  val configContainerName = "config"
  val storageAccountName = "aqstoragetransform"
  val keyvault_scope = "sparkjob"
  val keyvault_secret = "secret-sas-storagetransform"
  val old_colname = "cc"
  val new_colname = "cc_mod"

  // default information
  val default_inputFilename = "userdata1.parquet"
  val default_outputFilename = "out_userdata1.parquet"
  val default_expectedSchemaname = "expected_schema.json"
  val default_config_path = "config.json"

  private var sasToken = ""

  // to print out sample command for help
  def help() {
    println("Usage: \n" +
      "SparkJobOnAzure.SparkJobDemo <input_file_path> <expected_schema_path> <output_file_path>")
  }


  def main(args:Array[String]): Unit = {
    try {
      println("Initializing ... ")
      val spark = init()
      if (spark == null) {
        println("Failed to initialize")
        return
      }
      println("Successfully initializing ... ")

      val parsedArgs = parseArgs(args)
      val inputFilePath = basedir + inContainerName + "/" + parsedArgs._1
      val expectedSchemaPath = basedir + configContainerName + "/" + parsedArgs._2
      val outputFilePath = basedir + outContainerName + "/" + parsedArgs._3

      // read the parquet file
      println("Read the file as" + inputFilePath)
      val inDF:DataFrame = FileUtils.readFromFile(spark,inputFilePath,"parquet")
      if (inDF == null) {
        println("Failed to read from file")
        return
      }

      // validate schema of the input DataFrame
      println("Validate schema of " + inputFilePath + " using " + expectedSchemaPath)
      if (!Transformation.TransformedDataFrame.isValidateSchema(expectedSchemaPath, inDF.schema)) {
        println("Failed to validate the expected schema")
        return
      }

      // Rename the column of the input DataFrame
      println("Transforming ... ")
      val rnColDF = Transformation.TransformedDataFrame.renameColumnWithTag(inDF, old_colname, new_colname)

      // Drop the duplicated rows in the input DataFrame
      val dropRows = Transformation.TransformedDataFrame.dropDuplicatedRows(rnColDF)
      println("Successfully transforming ... ")

      // export the transformed data to parquet file
      println("Write to file as " + outputFilePath)
      val numOfPartition = 1
      var retVal = FileUtils.writeToFile(dropRows, outputFilePath, numOfPartition)
      if (!retVal) {
        println("Failed to write to file")
      }

      // export the expected schema. Just for getting the schema to use later
      // val outSchemaPath = "out_schema.json"
      // exportSchema(outdf, outSchemaPath)
    } finally (
      cleanup()
    )
  }

  // initialize to get spark session shared by Databricks and mount to DBFS on DataBricks
  def init(): SparkSession = {

    //To test locally using IntelIJ
    /*val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        //.master("spark://192.168.94.160:7077")
        .appName("SparkTransformation")
        .getOrCreate()
*/

    // The DataBricks does have its SpartSession and SparkContext so that need to reuse them
    println("init() - get spark session and spark context")
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    println("init() - start getting sas token")
    sasToken = DatabricksUtils.getSAS(keyvault_scope, keyvault_secret)

    println("init() - mount to " + basedir + inContainerName)
    var retVal = DatabricksUtils.mountToPath(inContainerName, storageAccountName, sasToken)
    if (retVal != true) {
      null
    }

    println("init() - mount to " + basedir + configContainerName)
    retVal = DatabricksUtils.mountToPath(configContainerName, storageAccountName, sasToken)
    if (retVal != true) {
      null
    }

    println("init() - mount to " + basedir + outContainerName)
    retVal = DatabricksUtils.mountToPath(outContainerName, storageAccountName, sasToken)
    if (retVal != true) {
      null
    }

    println("init() - Ending")
    spark
  }


  def cleanup() {
    println("Unmount the input path: " + basedir + inContainerName)
    DatabricksUtils.unMountToPath(basedir + inContainerName)
    println("Unmount the config path: " + basedir + configContainerName)
    DatabricksUtils.unMountToPath(basedir + configContainerName)
    println("Unmount the output path: " + basedir + outContainerName)
    DatabricksUtils.unMountToPath(basedir + outContainerName)
    println("Complete the SparkTranformation job")
  }


  def parseArgs(args:Array[String]): (String, String, String) = {
    var inputFilePath = default_inputFilename
    if (args.length >= 1) {
      inputFilePath = args(0)
    }

    var expectedSchemaPath = default_expectedSchemaname
    if (args.length >= 2) {
      expectedSchemaPath = args(1)
    }

    var outputFilePath = default_outputFilename
    if (args.length >= 3) {
      outputFilePath = args(2)
    }

    (inputFilePath, expectedSchemaPath, outputFilePath)
  }
}
