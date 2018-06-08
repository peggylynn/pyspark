import sys
import os.path

sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))

from pyspark.sql import SparkSession

# Configure the environment                                                     
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/home/ubuntu/spark-1.6.0-bin-hadoop2.6'

spark = SparkSession\
        .builder\
        .appName("ltpc-reset-down")\
        .getOrCreate()



#load the rolling 6 months or 1 year from Redshift dvcstats.devie_page_count_all..this test uses 18 days
theSQL = "(select SUBSTRING(account_code, 1,3) as acct_code,account_code,serial_number,reported_date,lifetime_page_count, color_lifetime_page_count,mono_lifetime_page_count,page_count,color_page_count,mono_page_count,time_dimension_surrogate_key, customer_master_surrogate_key,product_master_surrogate_key,material_master_surrogate_key,account_surrogate_key,asset_surrogate_key,agreement_surrogate_key,create_date_time,create_user from dvcstats.device_page_count_all where trunc(reported_date) >= current_date - INTERVAL '3 d') as myQuery"

#This one is to be run on EMR:
databaseReader = (spark.read
                 .format("jdbc")
                 .option("url","jdbc:postgresql://duck.cff0iaonktcp.us-east-1.redshift.amazonaws.com:5439/idw?tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory")
                 .option("dbtable",theSQL)
                 .option("user","pbishop")
                  .option("password","C0astalL$")
                  .option("driver","com.amazon.redshift.jdbc41.Driver"))
#databaseReader
raw_df = databaseReader.load()
raw_df .cache()

#This gets the serial_numbers with negative page counts and aliases a couple columns

affectedSN = raw_df.select(raw_df.acct_code,raw_df.account_code,raw_df.serial_number,raw_df.reported_date.alias("neg_read_date"),raw_df.lifetime_page_count.alias("LTPC_Y"),raw_df.color_lifetime_page_count,raw_df.mono_lifetime_page_count,raw_df.page_count,raw_df.color_page_count,raw_df.mono_page_count,raw_df.time_dimension_surrogate_key,raw_df. customer_master_surrogate_key,raw_df.product_master_surrogate_key,raw_df.material_master_surrogate_key,raw_df.account_surrogate_key,raw_df.asset_surrogate_key,raw_df.agreement_surrogate_key,raw_df.create_date_time,raw_df.create_user).filter("page_count < 0")
affectedSN.cache()

#Group the affected serial numbers and get a count.
#affectedSNGrp = affectedSN.groupBy(affectedSN.acct_code,affectedSN.serial_number).agg({"serial_number": "count"}).alias("theCount")
affectedSNGrp = affectedSN.groupBy(affectedSN.acct_code,affectedSN.serial_number).agg({"serial_number": "count"}).select(affectedSN.serial_number,affectedSN.acct_code,func.col("count(serial_number)").alias("theCount"))
affectedSNGrp.cache()

oneSN = affectedSN.join(affectedSNGrp, (affectedSNGrp.acct_code == affectedSN.acct_code) & (affectedSNGrp.serial_number == affectedSN.serial_number),'inner').filter(affectedSNGrp.theCount == 1).drop(affectedSNGrp.acct_code).drop(affectedSNGrp.serial_number)

#Find the last ltpc per serial number and the maximum ltpc.  If they are different, then the serial number falls into pattern1.  This needs a couple of routines to get this.
#Join the grouped serial_numbers back to the raw data to get all the history for the time period
affectedSN_all = oneSN.join(raw_df,(oneSN.account_surrogate_key == raw_df.account_surrogate_key) & (oneSN.asset_surrogate_key == raw_df.asset_surrogate_key),'inner') #this one worked, but I don't know if the data is correct

#Find the max ltpc of the affected serial numbers with only 1 negative page count.  This is what I did in the Pig script but didn't filter these out here.  Save for use later in this routine.
#First filter out all data that happened on or after the negative page count date.
#Get the negative page count date. It's in affected_sn1.

pre_ltpc_y_data = affectedSN_all.filter(affectedSN_all.reported_date < affectedSN_all.neg_read_date)

#This gets the data on and before the ltpc reset down
pre_ltpc_y_data.createOrReplaceTempView("pre_ltpc_y_data")
group_for_ltpc_x_y = spark.sql("select acct_code,serial_number, max(lifetime_page_count) ltpc_x, max(reported_date) ltpc_x_date, max(neg_read_date) neg_read_date,max(ltpc_y) ltpc_y from pre_ltpc_y_data group by acct_code,serial_number")
group_for_ltpc_x_y.printSchema()
group_for_ltpc_x_y.createOrReplaceTempView("group_for_ltpc_x_y")

#get the last meter read date by serial_number..this will do it for all serial numbers, affected or not so join to pre_ltpc_y_data to filter out what doesn't match
raw_df.createOrReplaceTempView("raw_df")
last_date = spark.sql("select serial_number, max(reported_date) ltpc_z_date from raw_df group by serial_number")

#Now find the ltpc at the ltpc_z_date
last_date.createOrReplaceTempView("last_date")
last_date_ltpc = spark.sql("select r.lifetime_page_count ltpc_z, r.acct_code,l.ltpc_z_date, l.serial_number,r.account_code, r.color_lifetime_page_count, r.mono_lifetime_page_count,r.page_count, r.color_page_count,r.mono_page_count, r.time_dimension_surrogate_key, r.customer_master_surrogate_key, r.product_master_surrogate_key, r.material_master_surrogate_key,r.account_surrogate_key,r.asset_surrogate_key,r.agreement_surrogate_key,r.create_date_time, r.create_user from raw_df r, last_date l where l.serial_number = r.serial_number and l.ltpc_z_date = r.reported_date")
last_date_ltpc.printSchema()
last_date_ltpc.createOrReplaceTempView("last_date_ltpc")

#join group_for_ltpc_x_y to last_date_ltpc to get all the columns for the affected serial_numbers
final = spark.sql("select g.acct_code, g.serial_number, g.ltpc_x, g.ltpc_x_date, g.neg_read_date, g.ltpc_y, l.ltpc_z_date, l.ltpc_z,l.account_code, l.color_lifetime_page_count, l.mono_lifetime_page_count,l.page_count, l.color_page_count,l.mono_page_count, l.time_dimension_surrogate_key, l.customer_master_surrogate_key, l.product_master_surrogate_key, l.material_master_surrogate_key,l.account_surrogate_key,l.asset_surrogate_key,l.agreement_surrogate_key,l.create_date_time, l.create_user from group_for_ltpc_x_y g, last_date_ltpc l where g.acct_code = l.acct_code and g.serial_number = l.serial_number")

final.coalesce(18).write.json("s3://dev-lxk-home/users/bishopp/ltpc-reset-down-out-pyspark-mar-01")
