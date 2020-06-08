package SparkJobOnAzure.Transformation

import org.apache.spark.sql
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import scala.util.control.Breaks

object TransformedDataFrame {

  /*
   Transform the parquet file as changing the column name, adding metadata for renamed column
   input: SparkSession, parquet file path, old column name, new column name after renaming
   output: DataFrame after renaming the column
   */
  def renameColumnWithTag(parqDF:DataFrame, old_colname:String, new_colname:String): DataFrame = {
    // change column name from cc to cc_mod and add tag
    val parq_rn_DF= parqDF.withColumnRenamed(old_colname,new_colname)
    val tag = "this column has been modified"
    val metadata = new sql.types.MetadataBuilder().putString("tag", tag).build()
    val newColumn = parq_rn_DF.col(new_colname).as("cc", metadata)
    val parq_rn_col_DF = parq_rn_DF.withColumn(new_colname, newColumn)
    //note: the column name in the schema is the same, just change to have the alias
    //parq_rn_col_DF.printSchema()

    // print metadata to see the new tag added
    //parq_rn_col_DF.schema.foreach(field => println(s"${field.name}: metadata=${field.metadata}"))

    parq_rn_col_DF
  }


  /*
   Transform the parquet file as dropping the duplicated rows and writing out to a parquet file
   input: SparkSession, parquet file path, old column name, new column name after renaming
   output: DataFrame after renaming the column
   */
  def dropDuplicatedRows(parqDF:DataFrame): DataFrame = {
    // drop duplicated rows on all columns
    // still not test this case. Need parquet file to test duplicated rows
    val parq_drop_rows_DF = parqDF.distinct()
    // can use  dropDuplicates(<list column name>) to distinct on some columns instead of all columns
    //println("Before dropping, rows = " + parq_rn_col_DF.schema.fields.length)
    //println("After  dropping, rows = " + parq_drop_rows_DF.schema.fields.length)

    parq_drop_rows_DF
  }


  /*
  Validate the real schema vs the expected schema. False is returned if
    + number of columns is greater or smaller than the expected one
    + field name is incorrect
    + data type is incorrect
  input: expected schema in json file, real schema to validate
  output: true or false
  */
  def isValidateSchema(expectedSchemaPath:String, realSchema:StructType): Boolean = {
    var retVal = true

    // val url = ClassLoader.getSystemResource(expectedSchemaPath)
    // Source.fromFile causes FILE NOT FOUND if not using /dbfs after dbutils.fs.mount()
    //val expectedSchemaPath_upd = "/dbfs" + expectedSchemaPath
    // use wasbs url instead of dbutils.fs.mount so that don't need to append /dbfs
    //val url = ClassLoader.getSystemResource(expectedSchemaPath)
    val schemaSource = Source.fromFile(expectedSchemaPath).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    //println(schemaFromJson.treeString)

    val loop = new Breaks;
    loop.breakable {
      for (x <- schemaFromJson.fields) {
        // validate the field name
        if (!realSchema.fieldNames.contains(x.name)) {
          retVal = false
          println("Cannot find field name as " + x.name)
          loop.break
        }

        // validate the data type
        if (realSchema.fields(realSchema.fieldIndex(x.name)).dataType != x.dataType) {
          retVal = false
          println("Incorrect data type of field name as " + x.name)
          loop.break
        }
      }
    }

    // validate the number of columns
    if (schemaFromJson.fields.length != realSchema.fields.length) {
      println("Mismatch the number of columns: " + schemaFromJson.fields.length + " columns expected vs " + realSchema.fields.length + " real columns ")
      retVal = false
    }
    retVal
  }


  /*
  Select N rows from the DataFrame to narrow down the size of parquet file. Just to use for testing smaller parquet file.
  input: SparkSession, original DataFrame and number of rows to output
  output: the output DataFrame
   */
  def exportNRowParquet(spark:SparkSession, inDF:DataFrame, nRows:String) : DataFrame = {
    inDF.createOrReplaceTempView("NColTable")
    val sqlStr = "SELECT * FROM NColTable WHERE ID <= " + nRows
    val sqlDF = spark.sql(sqlStr)
    sqlDF
  }
}
