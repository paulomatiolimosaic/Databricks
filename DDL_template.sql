-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Retailer_Client", "", "Retailer_Client")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Retailer_Client")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("ADLS_Account_Name", "", "ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("ADLS_Account_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("Drop_Create_All_Y_N_Flag", "N", "Drop_Create_All_Y_N_Flag")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("Drop_Create_All_Y_N_Flag")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exit if Drop_Create_All_Y_N_Flag is not Y or N

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (! List("y", "n").contains(dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase())){
-- MAGIC   dbutils.notebook.exit("Error: Invalid value found for Drop_Create_All_Y_N_Flag. Valid values are 'Y' and 'N'")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Create Data Vault Objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Drop All the objects if the Drop_Create_All_Y_N_Flag is set to Y

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("DROP DATABASE IF EXISTS " + dbutils.widgets.get("Retailer_Client") + "_dv CASCADE")
-- MAGIC }

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${Retailer_Client}_dv;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/hub_retailer_item")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS HUB_Retailer_Item
(
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    RETAILER_ITEM_ID STRING
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/hub_retailer_item'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/sat_retailer_item")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS SAT_RETAILER_ITEM
(
    HUB_RETAILER_ITEM_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    SAT_RETAILER_ITEM_HDIFF STRING,
    RETAILER_ITEM_DESC STRING
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/sat_retailer_item'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/hub_organization_unit")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS HUB_ORGANIZATION_UNIT
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    ORGANIZATION_UNIT_NUM INT
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/hub_organization_unit'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/sat_organization_unit")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS SAT_ORGANIZATION_UNIT
(
    HUB_ORGANIZATION_UNIT_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    SAT_ORGANIZATION_UNIT_HDIFF STRING,
    ORGANIZATION_UNIT_NM STRING
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/sat_organization_unit'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/link_epos_summary")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS LINK_EPOS_SUMMARY
(
    LINK_EPOS_SUMMARY_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    HUB_ORGANIZATION_UNIT_HK STRING,
    HUB_RETAILER_ITEM_HK STRING,
    SALES_DT DATE
)
USING com.databricks.spark.csv
PARTITIONED BY (SALES_DT)
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/link_epos_summary'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") + "_dv.LINK_ePOS_Summary")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/sat_link_epos_summary")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS SAT_LINK_EPOS_SUMMARY
(
    LINK_EPOS_SUMMARY_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    SAT_LINK_EPOS_SUMMARY_HDIFF STRING,
    POS_ITEM_QTY DECIMAL(15,2),
    POS_AMT	DECIMAL(15,2),
    ON_HAND_INVENTORY_QTY DECIMAL(15,2),
    SALES_DT DATE
)
USING com.databricks.spark.csv
PARTITIONED BY (SALES_DT)
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `escape` '"',
  `quote` '"',
  `delimiter` '\t',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/sat_link_epos_summary'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") + "_dv.SAT_LINK_EPOS_SUMMARY")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/sat_retailer_item_unit_price")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS SAT_RETAILER_ITEM_UNIT_PRICE
(
    LINK_EPOS_SUMMARY_HK STRING,
    LOAD_TS TIMESTAMP,
    RECORD_SOURCE_CD STRING,
    SAT_RETAILER_ITEM_UNIT_PRICE_HDIFF STRING,
    UNIT_PRICE_AMT DECIMAL(15,2),
    SALES_DT DATE
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (SALES_DT)
OPTIONS (
  'compression' 'snappy',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/sat_retailer_item_unit_price'
);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC if (dbutils.widgets.get("Drop_Create_All_Y_N_Flag").toString().toLowerCase() == "y"){
-- MAGIC   spark.sql("MSCK REPAIR TABLE " + dbutils.widgets.get("Retailer_Client") + "_dv.SAT_RETAILER_ITEM_UNIT_PRICE")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.mkdirs("adl://" + dbutils.widgets.get("ADLS_Account_Name") + ".azuredatalakestore.net/datavault/" + dbutils.widgets.get("Retailer_Client") + "/audit_driver_sales_dates")

-- COMMAND ----------

USE ${Retailer_Client}_dv;

CREATE TABLE IF NOT EXISTS AUDIT_DRIVER_SALES_DATES
(
    SALES_DT DATE,
    LOAD_TS TIMESTAMP,
    CREATE_TS TIMESTAMP
)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `serialization.format` '1',
  `header` 'false',
  `delimiter` ',',
  path 'adl://${ADLS_Account_Name}.azuredatalakestore.net/datavault/${Retailer_Client}/audit_driver_sales_dates'
);

REFRESH TABLE AUDIT_DRIVER_SALES_DATES;

-- COMMAND ----------

USE ${Retailer_Client}_dv;
CREATE OR REPLACE VIEW VW_SAT_LINK_EPOS_SUMMARY
AS
SELECT
          SALES_DT
        , HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , LINK_ePOS_Summary_HK
        , SAT_LINK_EPOS_SUMMARY_HDIFF
        , LOAD_TS
        , RECORD_SOURCE_CD
        , POS_ITEM_QTY
        , POS_AMT
        , ON_HAND_INVENTORY_QTY
FROM
(
SELECT
        SAT.SALES_DT
        , SAT.LINK_ePOS_Summary_HK
        , LINK.HUB_ORGANIZATION_UNIT_HK
        , LINK.HUB_RETAILER_ITEM_HK
        , SAT.LOAD_TS
        , SAT.RECORD_SOURCE_CD
        , SAT.SAT_LINK_EPOS_SUMMARY_HDIFF
        , SAT.POS_ITEM_QTY
        , SAT.POS_AMT
        , SAT.ON_HAND_INVENTORY_QTY
        , SAT.SALES_DT
        , ROW_NUMBER() OVER(PARTITION BY SAT.LINK_EPOS_SUMMARY_HK, SAT.SALES_DT ORDER BY SAT.LOAD_TS DESC) AS RNK
FROM 
        link_epos_summary LINK,
        sat_link_epos_summary SAT
WHERE
        LINK.LINK_EPOS_SUMMARY_HK = SAT.LINK_EPOS_SUMMARY_HK AND
        LINK.SALES_DT = SAT.SALES_DT
) S
WHERE
S.RNK = 1;

-- COMMAND ----------

USE ${Retailer_Client}_dv;
CREATE OR REPLACE VIEW VW_SAT_RETAILER_ITEM_UNIT_PRICE
AS
SELECT
          SALES_DT
        , HUB_ORGANIZATION_UNIT_HK
        , HUB_RETAILER_ITEM_HK
        , LOAD_TS
        , RECORD_SOURCE_CD
        , UNIT_PRICE_AMT
FROM
(
SELECT
        SAT.SALES_DT
        , SAT.LINK_EPOS_SUMMARY_HK
        , LINK.HUB_ORGANIZATION_UNIT_HK
        , LINK.HUB_RETAILER_ITEM_HK
        , SAT.LOAD_TS
        , SAT.RECORD_SOURCE_CD
        , SAT.UNIT_PRICE_AMT
        , ROW_NUMBER() OVER(PARTITION BY SAT.LINK_EPOS_SUMMARY_HK, SAT.SALES_DT ORDER BY SAT.LOAD_TS DESC) AS RNK
FROM 
        link_epos_summary LINK,
        SAT_RETAILER_ITEM_UNIT_PRICE SAT
WHERE
        LINK.LINK_EPOS_SUMMARY_HK = SAT.LINK_EPOS_SUMMARY_HK AND
        LINK.SALES_DT = SAT.SALES_DT
) S
WHERE
S.RNK = 1;