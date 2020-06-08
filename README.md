# SparkTransformation
A demo of spark transformation job is performed on Aure Databricks cluster

# Usage: 
      SparkJobOnAzure.SparkJobDemo <input_file_path> <expected_schema_path> <output_file_path>
in which input_file_path = userdata1.parquet, expected_schema_path = extected_schema.json and output_file_path = out_userdata1.parquet will be used by default if no argument is filled in.

# Note: 
Due to time constraint, it's hard-code to use following setting:

| Description |  Hard-coded value |  
|---|---|
| Input container to store parquet files  |  "level2" |
| Output container to store transformed output parquet files  | "level3"   |
| Config container to store expected schema in json format  |  "config" |
| The storage account name | "aqstoragetransform" |
| The secret scope in databricks | "sparkjob" |
| The keyvault secret to store sas token to access storage account | "secret-sas-storagetransform" |
| The old column name | "cc" |
| The new column name to be changed in the transformation step | "cc_mod"
|  |  |
