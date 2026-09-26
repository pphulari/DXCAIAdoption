# Databricks notebook source
# MAGIC     %md # process-raw-navig-sids-continuation3w-data
# MAGIC
# MAGIC Version History of  process-raw-navig-sids-continuation3w-data
# MAGIC    
# MAGIC    Changes:
# MAGIC
# MAGIC      Developer: Pradeep Phulari / Venkat Boyapati
# MAGIC      Date Created: 01/04/2023
# MAGIC      Date updated: 04/14/2023 
# MAGIC      Purpose: Read NAVAID - Sids continuation3w data from Raw zone and Load into Delta-Struct Zone

# COMMAND ----------

# Libraries to import
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_date
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import locate, reverse, length, substring
from datetime import datetime, timedelta
import json

# COMMAND ----------

# DBTITLE 1,Removing all existing widgets
#Removing all widgets used
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create widgets to pass parameters in the Param Notebook to authenticate Blob storage
# Defining widget with default value and Labelname
# dbutils.widgets.text("param_file_loc","wasbs://config@baneausstgnavigdev.blob.core.windows.net/","Parameter_File_Location")
# dbutils.widgets.text("param_file_name","navig_properties.json","Parameter_File_Name")
# dbutils.widgets.text("st_acct_name_config","baneausstgnavigdev","Storage_Account_Name")
# dbutils.widgets.text("srvc_principle_client_id","897a9154-0fe4-4e6d-b76d-7c80a000f75a","Service_Principle_Client ID")
# dbutils.widgets.text("srvc_principle_dir_id","49793faf-eb3f-4d99-a0cf-aef7cce79dc1","Service_Princilpe_Directory_ID")
# dbutils.widgets.text("adb_sp_sect_scopename","n-navig-sp-secret-scope","DataBricks_Scope_Name")
# dbutils.widgets.text("keyvault_sp_sect_name","ba-n-navig-001-sp-secret","Key_Vault_SP_Secret_Name")
# dbutils.widgets.text("keyvault_blob_key_sect_name","dev-blob-storage-access-key","Key_Vault_Blob_Key_Secret_Name")
# dbutils.widgets.text("FILE_CYCLE_RELEAS_NBR","2303","FILE_CYCLE_RELEAS_NBR")

dbutils.widgets.text("param_file_loc","","Parameter_File_Location")
dbutils.widgets.text("param_file_name","","Parameter_File_Name")
dbutils.widgets.text("st_acct_name_config","","Storage_Account_Name")
dbutils.widgets.text("srvc_principle_client_id","","Service_Principle_Client ID")
dbutils.widgets.text("srvc_principle_dir_id","","Service_Princilpe_Directory_ID")
dbutils.widgets.text("adb_sp_sect_scopename","","DataBricks_Scope_Name")
dbutils.widgets.text("keyvault_sp_sect_name","","Key_Vault_SP_Secret_Name")
dbutils.widgets.text("keyvault_blob_key_sect_name","","Key_Vault_Blob_Key_Secret_Name")
dbutils.widgets.text("FILE_CYCLE_RELEAS_NBR","","FILE_CYCLE_RELEAS_NBR")

# Assiging the widget value to variable
param_file_loc = dbutils.widgets.get("param_file_loc")
param_file_name = dbutils.widgets.get("param_file_name")
st_acct_name_config = dbutils.widgets.get("st_acct_name_config")
srvc_principle_client_id = dbutils.widgets.get("srvc_principle_client_id")
srvc_principle_dir_id = dbutils.widgets.get("srvc_principle_dir_id")
databricks_scopename = dbutils.widgets.get("adb_sp_sect_scopename")
keyvault_sp_sect_name = dbutils.widgets.get("keyvault_sp_sect_name")
keyvault_blob_key_sect_name = dbutils.widgets.get("keyvault_blob_key_sect_name")
FILE_CYCLE_RELEAS_NBR = dbutils.widgets.get("FILE_CYCLE_RELEAS_NBR")

# COMMAND ----------

# DBTITLE 1,Load the NAVIG Utilities
# MAGIC %run ../navig_utility/navig_util

# COMMAND ----------

# DBTITLE 1,Load the NAVIG Parameters
# MAGIC %run ../navig_utility/navig_parameter_setting

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# COMMAND ----------

# DBTITLE 1,Initialize Logging Variables
# Defining Parameters for mocam logger function
SYS_NM = "navig"
NOTEBOOK_NM = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
CLUSTER_NM = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
MSG_DESC = ""
START_TMS = str(datetime.now())
TARGET_NM = ""
END_TMS = ""
STATUS_CD = ""
TARGET_ADLS_ZONE = 'struct'
RUN_ID = str(get_notebook_run_id())
NOTEBOOK_JOB_URL = get_notebook_job_url()
SRC_FILE_NM = get_drain_file_name(base_path + "raw/")
TARGET_TYPE_CD = "T"
RT_SUCCESS_PATH = log_base_path + "sids-cr3w/"
RT_FAIL_PATH = log_base_path + "sids-cr3w/"
SUCCESS_PATH = RT_SUCCESS_PATH + "success"
FAIL_PATH = RT_FAIL_PATH + "failure"
STRUCT_DB_NAME = navig_struct_db_name

print('RT_Success Path: ' + RT_SUCCESS_PATH)
print('RT_Fail Path: ' + RT_FAIL_PATH)
print('Success Path: ' + SUCCESS_PATH)
print('Fail Path: ' + FAIL_PATH)
print('Run ID: ' + RUN_ID)
print('Notebook URL: ' + NOTEBOOK_JOB_URL)
print('Susyem Name: ' + SYS_NM)
print('Source File Name: ' + SRC_FILE_NM)
print('Struct DB Name: ' + STRUCT_DB_NAME)

# COMMAND ----------

# DBTITLE 1,Log the Starting of this Notebook
# Write the first log with start time

# The log will be written in the success folder 
SAVE_PATH = SUCCESS_PATH
STATUS_CD = "R"
MSG_DESC = "Notebook starting"

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)

# COMMAND ----------

# DBTITLE 1,Define ADLS Read/Write Data Paths
# Defining ADLS Paths

#base_path = "abfss://oasis@aabaoriondlsnp.dfs.core.windows.net"
rawReadPath = navig_raw_path
structWritePath = navig_deltastruct_path

print("Raw Read Path: " + rawReadPath)
print("Struct Write Path: " + structWritePath)

# COMMAND ----------

  # Read aalfltkey.txt file
try :
    rawNavigDF = (spark.read.format("csv").load(rawReadPath+src_file_name.rstrip(".txt")+"_"+FILE_CYCLE_RELEAS_NBR+".txt"))
    tmpRawNavigDF = rawNavigDF.selectExpr( "substring(_c0,1,1) as RecordType",
                                           "substring(_c0,2,3) as CustomerAreaCode",
                                           "substring(_c0,5,1) as SectionCode",
                                           "substring(_c0,6,1) as BlankSpacing1",
   										"substring(_c0,7,4) as AirportIdentifier",
										"substring(_c0,11,2) as ICAOCode",
										"substring(_c0,13,1) as SubsectionCode",
										"substring(_c0,14,6) as SID_STAR_ApproachIdentifier",
										"substring(_c0,20,1) as RouteType",
										"substring(_c0,21,5) as TransitionIdentifier",
										"substring(_c0,26,1) as ProcedureDesignAircraftCategory_or_Type",
										"substring(_c0,27,3) as SequenceNumber",
										"substring(_c0,30,5) as FixIdentifier",
										"substring(_c0,35,2) as ICAOCode2",
										"substring(_c0,37,1) as Fix_SectionCode",
										"substring(_c0,38,1) as Fix_SubsectionCode",
										"substring(_c0,39,1) as ContinuationRecordNumber_CR5",
										"substring(_c0,40,1) as ApplicationType_CR5",
										"substring(_c0,41,1) as FAS_BlockProvidedAuthorized_CR5",
										"substring(_c0,42,10) as FAS_BlockProvided_Lev_of_ServiceName_CR5",
										"substring(_c0,52,1) as LNAV_VNAV_Authorized_CR5",
										"substring(_c0,53,10) as LNAV_VNAV_Level_of_ServiceName_CR5",
										"substring(_c0,63,1) as LNAV_Authorized_CR5",
										"substring(_c0,64,10) as LNAV_Level_of_ServiceName_CR5",
										"substring(_c0,74,14) as Blank_Spacing3_CR5",
										"substring(_c0,89,1) as RNP_Authorized1_CR5",
										"substring(_c0,90,3) as RNP_Level_of_Service_value1_CR5",
										"substring(_c0,93,1) as RNP_Authorized2_CR5",
										"substring(_c0,94,3) as RNP_Level_of_Service_value2_CR5",
										"substring(_c0,97,1) as RNP_Authorized3_CR5",
										"substring(_c0,98,3) as RNP_Level_of_Service_value3_CR5",
										"substring(_c0,101,1) as RNP_Authorized4_CR5",
										"substring(_c0,102,3) as RNP_Level_of_Service_value4_CR5",
										"substring(_c0,105,14) as Blank_Spacing4_CR5",
										"substring(_c0,119,1) as RouteQualifier1_CR5",
										"substring(_c0,120,1) as RouteQualifier2_CR5",
										"substring(_c0,121,3) as BlankSpacing5_CR5",
										"substring(_c0,124,5) as FileRecordNumber_CR5",
										"substring(_c0,129,4) as CycleDate_CR5" ).where( ( col("_c0").substr(39, 1).isin('3') )  &  ( col("_c0").substr(5,1) == "P" ) &  ( col("_c0").substr(13,1) == "E" ) &  ( col("_c0").substr(40,1).isin('W') )  )        
  
  # Create record create date and timestamp, inputfile name column for cycle
    tmpRawNavigDF = (tmpRawNavigDF
              .withColumn("UPDT_UTC_TMS", current_timestamp())
              .withColumn("INPUT_FILE_URL", input_file_name())
             )
    tmpRawNavigDF = tmpRawNavigDF.selectExpr("*", "substr(INPUT_FILE_URL, (length(INPUT_FILE_URL) - locate('/', reverse(INPUT_FILE_URL))) + 2, (length(INPUT_FILE_URL) - locate('/', reverse(INPUT_FILE_URL)))) as INBOUND_FILE_NAME").drop("INPUT_FILE_URL")
    #tmpRawNavigDF = tmpRawNavigDF.selectExpr("*", "left(right(INBOUND_FILE_NAME, 12),4) as FILE_CYCLE_RELEAS_NBR").drop("INBOUND_FILE_NAME")
    tmpRawNavigDF = tmpRawNavigDF.selectExpr("*","right(regexp_replace(INBOUND_FILE_NAME,'.txt',''),4) as FILE_CYCLE_RELEAS_NBR").drop("INBOUND_FILE_NAME")
    #tmpRawNavigDF = tmpRawNavigDF.selectExpr("*", "to_date(from_unixtime(unix_timestamp(concat_ws('-', substr(SNAPSHOT_DATE,1,4), substr(SNAPSHOT_DATE,5,2), substr(SNAPSHOT_DATE,7,2)), 'yyyy-MM-dd'), 'yyyy-MM-dd')) as SNAPSHOT_DT").drop("SNAPSHOT_DATE")   
except Exception as e:
    MSG_DESC = "Failed to read aalfltkey records from Raw zone. Error:" + " " + str(e)
    END_TMS = str(datetime.now())
    STATUS_CD = "E"
    SAVE_PATH = FAIL_PATH
    log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
    raise e

# COMMAND ----------

# DBTITLE 1,Log Message for Successfully Reading Data From Raw Zone
# Write the first log with start time

# The log will be written in the success folder 
SAVE_PATH = SUCCESS_PATH
STATUS_CD = "S"
MSG_DESC = "All Sids continuation3w records has been successfully read"

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)

# COMMAND ----------

# DBTITLE 1,Get the distinct snapshot date from incoming Sids Primary  data
# snapshot_dt_from_src = list(tmpRawNavigDF.select("SNAPSHOT_DT").distinct().toPandas()['SNAPSHOT_DT'])
# if len(snapshot_dt_from_src) != 0:
#   snapshot_dt = snapshot_dt_from_src[0]
# else:
#   snapshot_dt = None  
# print(snapshot_dt)

# COMMAND ----------

# DBTITLE 1,Save Sids Primary Records
# Save Sids Records
table_location = structWritePath + "sids-continuation3w"
try:
  tmpRawNavigDF\
  .write\
  .format("delta")\
  .partitionBy("FILE_CYCLE_RELEAS_NBR")\
  .mode("overwrite")\
  .option("replaceWhere", """FILE_CYCLE_RELEAS_NBR = '{0}'""".format(FILE_CYCLE_RELEAS_NBR))\
  .option("path", table_location)\
  .save()
  
  # Log the success message 
  SAVE_PATH = SUCCESS_PATH
  STATUS_CD = "S"
  MSG_DESC = "Sids records has been successfully written in Struct zone"
  TARGET_NM = ""

#  log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
except Exception as e:
  MSG_DESC = "Failed to write Sids continuation3w records in Struct zone. Error:" + " " + str(e)
  END_TMS = str(datetime.now())
  STATUS_CD = "E"
  SAVE_PATH = FAIL_PATH
  
  log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
  raise e  

# COMMAND ----------

# DBTITLE 1,Create the Sids primary delta table (if not already exist) in Struct database
table_name = "sids_continuation3w"
try:
  if (sqlContext.sql("show tables in {0}".format(STRUCT_DB_NAME))
      .filter(col("tableName") == table_name)
      .count() <= 0):
        
    sqlContext.sql("create table if not exists {0}.{2} \
                  using delta \
                  location '{1}'".format(STRUCT_DB_NAME, table_location,table_name))
  
    # Log the success message 
    SAVE_PATH = SUCCESS_PATH
    STATUS_CD = "S"
    MSG_DESC = "Sids continuation3w Delta table has been successfully created in Struct database"
    TARGET_NM = ""

    log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
except Exception as e:
  MSG_DESC = "Failed to create Sids continuation3w Delta table in Struct database. Error:" + " " + str(e)
  END_TMS = str(datetime.now())
  STATUS_CD = "E"
  SAVE_PATH = FAIL_PATH
  
  log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
  raise e

# COMMAND ----------

# DBTITLE 1,Write Final Success Log
# Write the final log with end time

# The log will be written in the success folder 
SAVE_PATH = SUCCESS_PATH
STATUS_CD = "S"
MSG_DESC = "Notebook completed processing all Sids continuation3w records"
END_TMS = str(datetime.now())

# Empty the Target Name so that it does not calculate insert/update/delete counts for this log
TARGET_NM = STRUCT_DB_NAME + "." + table_name

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)