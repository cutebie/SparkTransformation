package SparkJobOnAzure.Utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object DatabricksUtils {
  val basedir = "/mnt/"

  def main(args:Array[String]) {
    if (args.length != 2) {
      println("Incorrect arguments. See below usage.")
      help()
    }

    // hardcode for testing
    val storageAccountName = "aqstoragetransform"
    val keyvault_scope = "sparkjob"
    val keyvault_secret = "secret-sas-storagetransform"

    if (args(0) != "mount" && args(0) != "unmount" && args(0) != "ls") {
      println("Incorrect arguments. See below usage.")
      help()
      return
    }

    val typeAction = args(0)
    val containerName = args(1)

    val sasStr = getSAS(keyvault_scope, keyvault_secret)
    println(sasStr)

    var retVal: Boolean = true
    if (typeAction == "mount") {
      retVal = mountToPath(containerName,storageAccountName, sasStr)
    } else if (typeAction == "unmount") {
      retVal = unMountToPath(basedir + containerName)
    } else {
      val retStr = lsPath(basedir + containerName)
      println(retStr)
    }

    if (retVal) {
      println("Successfully " + typeAction + " DBFS = " + basedir + containerName)
    } else {
      println("Failed " + typeAction + " DBFS = " + basedir + containerName)
    }
  }

  // to print out sample command for help
  def help() {
    println("Usage: \n" +
      "SparkJobOnAzure.DatabricksUtils mount <container_name> \n" +
      "SparkJobOnAzure.DatabricksUtils unmount <container_name> \n" +
      "SparkJobOnAzure.DatabricksUtils ls <container_name>")
  }

  // to get SAS of a storage account. SAS is stored as the value in the secret of key-vault
  def getSAS(keyvault_scope:String, keyvault_secret:String):String = {
    val sasToken = dbutils.secrets.get(scope = keyvault_scope, key = keyvault_secret)
    sasToken
  }

  // to mount to a path
  def mountToPath(containerName:String, storageAccountName:String, sasToken:String): Boolean = {
    // Generate sas (shared access signature) from your storage account, then get the sas token.
    // Carefully note the expired date of this sas token (default is one day starting from the time you generated sas)
    val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
    val config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

    val fullPath = basedir + containerName
    val retVal = dbutils.fs.mount(
      source = url,
      mountPoint = fullPath,
      extraConfigs = Map(config -> sasToken)
    )
    retVal
  }

  // to unmount a path
  def unMountToPath(path:String): Boolean = {
   val retVal = dbutils.fs.unmount(path)
    retVal
  }

  // to list a path
  def lsPath(path:String): String = {
    val retVal = dbutils.fs.ls(path)
    retVal.toString()
  }
}
