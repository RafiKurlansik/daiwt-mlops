# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Get Dataset
# MAGIC We need to first load the dataset in so that we can train models off of it

# COMMAND ----------

# DBTITLE 1,Load libraries
import shutil
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType,StructField,DoubleType, StringType, IntegerType, FloatType

# COMMAND ----------

# DBTITLE 1,Obtail Dataset
# MAGIC %sh 
# MAGIC
# MAGIC wget https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv -O /databricks/driver/Telco-Customer-Churn.csv

# COMMAND ----------

# DBTITLE 1,Setup variables
database_name = 'sr_ibm_telco_churn'

# COMMAND ----------

# DBTITLE 1,Calculated variables
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

filepath = 'file:/databricks/driver/Telco-Customer-Churn.csv'

driver_to_dbfs_path = 'dbfs:/home/{}/ibm-telco-churn/Telco-Customer-Churn.csv'.format(username)
dbutils.fs.cp(filepath, driver_to_dbfs_path)

tbl_path = '/home/{}/ibm-telco-churn/bronze/'.format(username)

tbl_name = 'bronze_customers'

# COMMAND ----------

# DBTITLE 1,Clean Up Old Files
_ = spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(database_name))
_ = spark.sql('CREATE DATABASE {}'.format(database_name))

shutil.rmtree('/dbfs'+tbl_path, ignore_errors=True)


# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Table Structures

# COMMAND ----------

# DBTITLE 1,Define schema
schema = StructType([
  StructField('customerID', StringType()),
  StructField('gender', StringType()),
  StructField('seniorCitizen', DoubleType()),
  StructField('partner', StringType()),
  StructField('dependents', StringType()),
  StructField('tenure', DoubleType()),
  StructField('phoneService', StringType()),
  StructField('multipleLines', StringType()),
  StructField('internetService', StringType()), 
  StructField('onlineSecurity', StringType()),
  StructField('onlineBackup', StringType()),
  StructField('deviceProtection', StringType()),
  StructField('techSupport', StringType()),
  StructField('streamingTV', StringType()),
  StructField('streamingMovies', StringType()),
  StructField('contract', StringType()),
  StructField('paperlessBilling', StringType()),
  StructField('paymentMethod', StringType()),
  StructField('monthlyCharges', DoubleType()),
  StructField('totalCharges', DoubleType()),
  StructField('churnString', StringType())
  ])

# COMMAND ----------

# DBTITLE 1,Read CSV, write to Delta and take a look
bronze_df = spark.read.format('csv').schema(schema).option('header','true')\
               .load(driver_to_dbfs_path)

bronze_df.write.format('delta').mode('overwrite').save(tbl_path)

# COMMAND ----------

# DBTITLE 1,Create bronze table
_ = spark.sql('''
  CREATE TABLE `{}`.{}
  USING DELTA 
  LOCATION '{}'
  '''.format(database_name,tbl_name,tbl_path))