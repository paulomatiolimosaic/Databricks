-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.widgets.text("SQL_Server_Table_Name", "", "SQL_Server_Table_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("SQL_Server_Table_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.text("SQL_Server_DB_Name", "", "SQL_Server_DB_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.get("SQL_Server_DB_Name")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC def createSQLServerExternalTable(db: String, table: String): Unit = {
-- MAGIC 
-- MAGIC   val url = f"jdbc:sqlserver://azu-eus2-dev-sqldb-informreplica.database.windows.net:1433;database=${db}"
-- MAGIC   val user = dbutils.secrets.get(scope = "blob", key = "dbr-azueus2-sqldb-informreplica-user")
-- MAGIC   val pass =  dbutils.secrets.get(scope = "blob", key = "dbr-azueus2-sqldb-informreplica-password")
-- MAGIC   
-- MAGIC   // Create Spark DB if not exists
-- MAGIC   spark.sql(f"CREATE DATABASE IF NOT EXISTS ${db}")
-- MAGIC   
-- MAGIC   
-- MAGIC   val ddl = f"""
-- MAGIC   CREATE TABLE IF NOT EXISTS ${db}.${table}
-- MAGIC   USING org.apache.spark.sql.jdbc
-- MAGIC   OPTIONS (
-- MAGIC     url "${url}",
-- MAGIC     dbtable "company",
-- MAGIC     user "${user}",
-- MAGIC     password "${pass}")"""
-- MAGIC   spark.sql(ddl)
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC createSQLServerExternalTable("BOBv2", "Lkp_ProductGroup")

-- COMMAND ----------

select * from BOBv2.Lkp_ProductGroup

-- COMMAND ----------

use paulo2;
show tables;

-- COMMAND ----------

select * from bobv2.lkp_productgroup

-- COMMAND ----------


