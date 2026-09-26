# Databricks notebook source
# MAGIC     %md # process_airports_continuation2_raw_to_struct_data
# MAGIC
# MAGIC Version History of  process-raw-navig-airport-continuation2-data
# MAGIC    
# MAGIC    Changes:
# MAGIC
# MAGIC      Developer: Venkat Boyapati / Pradeep Phulari
# MAGIC      Date Created: 2/1/2023
# MAGIC      Date updated: 04/14/2023 
# MAGIC      Purpose: Read NAVAID - ARINC data from Raw zone and Load into Delta-Struct Zone

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
dbutils.widgets.text("param_file_loc","wasbs://arinc424v18@baneausstgfltcorepkg.blob.core.windows.net/dev/config/","Parameter_File_Location")
dbutils.widgets.text("param_file_name","navig_properties.json","Parameter_File_Name")
dbutils.widgets.text("st_acct_name_config","baneausstgfltcorepkg","Storage_Account_Name")
dbutils.widgets.text("srvc_principle_client_id","6fa03f43-6e82-4f41-9773-f74a84610118","Service_Principle_Client ID")
dbutils.widgets.text("srvc_principle_dir_id","49793faf-eb3f-4d99-a0cf-aef7cce79dc1","Service_Princilpe_Directory_ID")
dbutils.widgets.text("adb_sp_sect_scopename","n-fltcorepkg-sp-secret-scope","DataBricks_Scope_Name")
dbutils.widgets.text("keyvault_sp_sect_name","ba-n-fltcorepkg-001-sp-secret","Key_Vault_SP_Secret_Name")
dbutils.widgets.text("keyvault_blob_key_sect_name","blob-storage-access-key","Key_Vault_Blob_Key_Secret_Name")
dbutils.widgets.text("FILE_CYCLE_RELEAS_NBR","2405","FILE_CYCLE_RELEAS_NBR")

# dbutils.widgets.text("param_file_loc","","Parameter_File_Location")
# dbutils.widgets.text("param_file_name","","Parameter_File_Name")
# dbutils.widgets.text("st_acct_name_config","","Storage_Account_Name")
# dbutils.widgets.text("srvc_principle_client_id","","Service_Principle_Client ID")
# dbutils.widgets.text("srvc_principle_dir_id","","Service_Princilpe_Directory_ID")
# dbutils.widgets.text("adb_sp_sect_scopename","","DataBricks_Scope_Name")
# dbutils.widgets.text("keyvault_sp_sect_name","","Key_Vault_SP_Secret_Name")
# dbutils.widgets.text("keyvault_blob_key_sect_name","","Key_Vault_Blob_Key_Secret_Name")
# dbutils.widgets.text("FILE_CYCLE_RELEAS_NBR","","FILE_CYCLE_RELEAS_NBR")

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
RT_SUCCESS_PATH = log_base_path + "airport-cr2-v18/"
RT_FAIL_PATH = log_base_path + "airport-cr2-v18/"
SUCCESS_PATH = RT_SUCCESS_PATH + "success/"
FAIL_PATH = RT_FAIL_PATH + "failure/"
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

# Write the first log with start time

# The log will be written in the success folder 
SAVE_PATH = SUCCESS_PATH
STATUS_CD = "R"
MSG_DESC = "Notebook starting"
print(SAVE_PATH)

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)

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
rawReadPath = navig_raw_path+env+"/"
structWritePath = navig_deltastruct_path

print("Raw Read Path: " + rawReadPath)
print("Struct Write Path: " + structWritePath)

# COMMAND ----------

# DBTITLE 1,Read aalfltkey.txt file from Blob storage/Raw Zone
# Read aalfltkey.txt file
try :
  #rawNavigDF = (spark.read.format("csv").load(rawReadPath+src_file_name.rstrip(".txt")+"_"+FILE_CYCLE_RELEAS_NBR+".txt"))
  rawNavigDF = spark.read.text(rawReadPath+src_file_name.rstrip(".txt")+"_"+FILE_CYCLE_RELEAS_NBR+".txt").select(col("value").alias("DATA_RECORD"))
  rawNavigDF.createOrReplaceTempView("rawNavigvw")
  tmpRawNavigDF = spark.sql("""SELECT
                                 substring(DATA_RECORD,1,1) as RecordType,
                                 substring(DATA_RECORD,2,3) as CustomerAreaCode,
                                 substring(DATA_RECORD,5,1) as SectionCode,
                                 substring(DATA_RECORD,6,1) as BlankSpacing1,
                                 substring(DATA_RECORD,7,4) as AirportICAOIdentifier,
                                 substring(DATA_RECORD,11,2) as ICAOCode,
                                 substring(DATA_RECORD,13,1) as SubsectionCode,
                                 substring(DATA_RECORD,14,3) as ATA_IATADesignator,
                                 substring(DATA_RECORD,17,2) as ReservedExpansion,
                                 substring(DATA_RECORD,19,3) as BlankSpacing2,
                                 cast(substring(DATA_RECORD,22,1)as int) as ContinuationRecordNo_CR2,
                                 substring(DATA_RECORD,23,1) as ApplicationType_CR2,
                                 substring(DATA_RECORD,24,24) as JS_City_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,48,2) as JS_State_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,50,3) as JS_Country_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,53,14) as JS_ReservedExpansion_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,67,1) as JS_CTLDARSPIndicator_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,68,4) as JS_CTLDARSPAirportID_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,72,2) as JS_CTLDARSPAirportICAO_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,74,14) as JS_FuelTypes_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,88,5) as JS_Oxygen_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,93,6) as JS_Repairs_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,99,1) as JS_LandingFee_CR2, --Jeppesen Supplemental JS records
                                 substring(DATA_RECORD,100,4) as JS_PatternAltitude_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,104,1) as JS_JASU_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,105,1) as JS_LLWAS_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,106,1) as JS_BeaconLight_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,107,1) as JS_Customs_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,108,10) as JS_ReservedExpansion2_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,118,1) as JS_SurfaceType_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,119,1) as JS_PublicMilitaryInd_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,120,3) as JS_TimeZone_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,123,1) as JS_DaylightTimeInd_CR2, --Jeppesen Supplemental JS records
                                substring(DATA_RECORD,124,5) as FileRecordNo_CR2,
                                substring(DATA_RECORD,129,4) as CycleDate_CR2														 
																FROM rawNavigvw
                                WHERE  (SUBSTRING(DATA_RECORD, 23, 1) IN ('J')) AND (SUBSTRING(DATA_RECORD, 22, 1) IN ('2')) AND(concat(SUBSTRING(DATA_RECORD, 5, 1),SUBSTRING(DATA_RECORD, 13, 1)) IN ('PA')) """)
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

# MAGIC %sql describe table navig_dev_struct.airports_continuation2

# COMMAND ----------

# DBTITLE 1,Log Message for Successfully Reading Data From Raw Zone
# Write the first log with start time

# The log will be written in the success folder 
SAVE_PATH = SUCCESS_PATH
STATUS_CD = "S"
MSG_DESC = "All Airport continuation2 records has been successfully read"

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)

# COMMAND ----------

# DBTITLE 1,Get the distinct snapshot date from incoming Airport Continuation 2 data
# snapshot_dt_from_src = list(tmpRawNavigDF.select("SNAPSHOT_DT").distinct().toPandas()['SNAPSHOT_DT'])
# if len(snapshot_dt_from_src) != 0:
#   snapshot_dt = snapshot_dt_from_src[0]
# else:
#   snapshot_dt = None  
# print(snapshot_dt)

# COMMAND ----------

# DBTITLE 1,Save Airport Continuation 2 Records
# Save Airport Continuation 2 Records
table_location = structWritePath + "airports-continuation2"
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
  MSG_DESC = "Airways records has been successfully written in Struct zone"
  TARGET_NM = ""

#  log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
except Exception as e:
  MSG_DESC = "Failed to write Airport Continuation 2  records in Struct zone. Error:" + " " + str(e)
  END_TMS = str(datetime.now())
  STATUS_CD = "E"
  SAVE_PATH = FAIL_PATH
  
  log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
  raise e  

# COMMAND ----------

# DBTITLE 1,Create the Airport CR2 delta table (if not already exist) in Struct database
table_name = "airports_continuation2"
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
    MSG_DESC = "Airport continuation2  Delta table has been successfully created in Struct database"
    TARGET_NM = ""

    log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)
except Exception as e:
  MSG_DESC = "Failed to create Airport continuation2 Delta table in Struct database. Error:" + " " + str(e)
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
MSG_DESC = "Notebook completed processing all Airport continuation2 records"
END_TMS = str(datetime.now())

# Empty the Target Name so that it does not calculate insert/update/delete counts for this log
TARGET_NM = STRUCT_DB_NAME + "." + table_name

log_operational_data(SYS_NM, NOTEBOOK_NM, CLUSTER_NM, CLUSTER_ID, SRC_FILE_NM, TARGET_NM, START_TMS, END_TMS, TARGET_TYPE_CD, STATUS_CD, MSG_DESC, TARGET_ADLS_ZONE, RUN_ID, NOTEBOOK_JOB_URL, SAVE_PATH)