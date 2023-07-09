# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_pakc_new.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=70)

# COMMAND ----------

cmp.target_store.display()
cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.params

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

from exposure import exposed

ex_1, ex_2, ex_3 = exposed.get_exposure(cmp)
ex_1.display()
# load_txn.load_txn(cmp, txn_mode="campaign_specific")
# cmp.txn = spark.table("tdm_seg.media_campaign_eval_txn_data_2023_0265_m01e").replace({"cmp":"dur"}).withColumn("unit", F.col("pkg_weight_unit"))
# cmp.matched_store = spark.table("tdm_seg.thanakritboo_th_cust_uplift_sku_matching_tmp")

# COMMAND ----------

from uplift import uplift

uplift.get_cust_uplift_by_mech(cmp, cmp.feat_brand_sku, "brand")

# COMMAND ----------

from activate import activated
from uplift import unexposed
from utils import period_cal
from uplift import uplift

prd_scope_df = cmp.feat_sku
prd_scope_nm = "sku"

# COMMAND ----------

cust_uplift = uplift.get_cust_uplift_by_mech(cmp, prd_scope_df, prd_scope_nm)

# COMMAND ----------

cust_uplift.display()

# COMMAND ----------

# MAGIC %md ##Count check

# COMMAND ----------

cust_all_exposed = activated.get_cust_txn_all_exposed_date_n_mech(cmp)
cust_all_unexposed = unexposed.get_cust_txn_all_unexposed_date_n_mech(cmp)
cust_all_purchased = activated.get_cust_txn_all_prod_purchase_date(cmp, prd_scope_df)

# COMMAND ----------

cust_all_exposed = spark.table("tdm_seg.thanakritBoo_th_cust_all_exposed_tmp")
cust_all_unexposed = spark.table("tdm_seg.thanakritBoo_th_cust_all_unexposed_tmp")
cust_all_purchased = spark.table("tdm_seg.thanakritBoo_th_cust_all_purchase_tmp")
new = spark.table("tdm_seg.thanakritBoo_th_cust_uplift_sku_tmp")

# COMMAND ----------

cust_all_exposed.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

new.where(F.col("exposed").isNotNull()).agg(F.count_distinct("household_id")).display() 

# COMMAND ----------

cust_all_unexposed.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

new.where(F.col("unexposed").isNotNull()).agg(F.count_distinct("household_id")).display()

# COMMAND ----------

# exposed supersede unexposed

cust_all_exposed.join(cust_all_unexposed,"household_id", "inner").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

# MAGIC %md ##purchased

# COMMAND ----------

cust_all_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_all_purchased.join(cust_all_exposed, "household_id", "leftanti").join(cust_all_unexposed, "household_id", "leftanti").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

all_expo_all_unexp = cust_all_exposed.select("household_id").drop_duplicates().unionByName(cust_all_unexposed.select("household_id").drop_duplicates()).drop_duplicates()

# COMMAND ----------

all_expo_all_unexp.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

all_expo_all_unexp.join(cust_all_purchased, "household_id").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

txn_exposed_txn_unexposed = (cust_all_exposed.select("household_id", 
                                                     F.col("exposed_transaction_uid").alias("transaction_uid"),
                                                     F.col("exposed_tran_datetime").alias("tran_datetime")
                                                     ).drop_duplicates().withColumn("txn_type", F.lit("exposed"))
                             .unionByName(cust_all_unexposed.select("household_id", 
                                                     F.col("unexposed_transaction_uid").alias("transaction_uid"),
                                                     F.col("unexposed_tran_datetime").alias("tran_datetime")
                                                     ).drop_duplicates().withColumn("txn_type", F.lit("unexposed")))
)
txn_exposed_txn_unexposed.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

# purchased before exposed
cust_purchased_before = \
(txn_exposed_txn_unexposed
#  .where(F.col("txn_type")=="exposed")
 .join(cust_all_purchased.drop_duplicates(), "household_id", "inner")
 .where(F.col("tran_datetime")>F.col("purchase_tran_datetime"))
)

# COMMAND ----------

cust_purchased_before.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_after = \
(txn_exposed_txn_unexposed
#  .where(F.col("txn_type")=="exposed")
 .join(cust_all_purchased.drop_duplicates(), "household_id", "inner")
 .where(F.col("tran_datetime")<=F.col("purchase_tran_datetime"))
)

# COMMAND ----------

cust_purchased_after.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_before_only = cust_purchased_before.join(cust_purchased_after, "household_id", "leftanti")
cust_purchased_before_only.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_before_and_after =  cust_purchased_before.join(cust_purchased_after, "household_id", "inner")
cust_purchased_before_and_after.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_after.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

from pyspark.sql import Window

(cust_purchased_after
.withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
.withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
.where(F.col('recency_rank') == 1)
).groupBy("txn_type").agg(F.count_distinct("household_id")).display()


# COMMAND ----------

cust_all_exposed.join(cust_all_unexposed, "household_id", "inner").select("household_id").display()

# COMMAND ----------

(cust_purchased_after
.withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
.withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
.where(F.col('recency_rank') > 10)
).display()

# COMMAND ----------

(cust_purchased_after
.withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
.withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
.where(F.col("household_id").isin([102111060000128924]))
).display()

# COMMAND ----------

# MAGIC %md cust unexposed & purchased exclude exposed

# COMMAND ----------

cust_exposed_purchased = \
    (cust_purchased_after
    .where(F.col("txn_type")=="exposed")
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1)
    )

cust_exposed_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_unexposed_purchased = \
    (cust_purchased_after
    .where(F.col("txn_type")=="unexposed")
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1)
    )

cust_unexposed_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_after.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_exposed_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_unexposed_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_unexposed_purchased_excl_exp_purchased = cust_unexposed_purchased.join(cust_all_exposed, "household_id", "leftanti")

# COMMAND ----------

cust_unexposed_purchased_excl_exp_purchased.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_purchased_after.join(cust_exposed_purchased, "household_id", "leftanti").join(cust_unexposed_purchased_excl_exp_purchased, "household_id", "leftanti").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

(cust_purchased_after.join(cust_exposed_purchased, "household_id", "leftanti")
 .join(cust_unexposed_purchased_excl_exp_purchased, "household_id", "leftanti")
 .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('tran_datetime'))
 .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
).display()

# COMMAND ----------

cust_purchased_after.join(cust_exposed_purchased, "household_id", "leftanti").join(cust_unexposed_purchased_excl_exp_purchased, "household_id", "leftanti")

# COMMAND ----------

cust_exposed_purchased.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

cust_all_exposed.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

spark.table("tdm_seg.thanakritBoo_th_cust_uplift_sku_tmp").where(F.col("household_id")==102111060000128924).display()


# COMMAND ----------

cust_all_exposed.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

cust_exposed_purchased.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

cust_all_purchased.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

cust_purchased_after.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

cust_all_unexposed.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

new.where(F.col("household_id")==102111060001360583).display()

# COMMAND ----------

missing = cust_purchased_after.join(cust_exposed_purchased, "household_id", "leftanti").join(cust_unexposed_purchased_excl_exp_purchased, "household_id", "leftanti")

# COMMAND ----------

missing.agg(F.count_distinct("household_id")).display()

# COMMAND ----------

missing.join(cust_all_exposed, "household_id", "leftanti").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

cust_exp_unexp_x_purchased.write.saveAsTable('tdm_seg.thanakritBoo_th_cust_uplift_sku_tmp')

# COMMAND ----------

new = spark.table("tdm_seg.thanakritBoo_th_cust_uplift_sku_tmp")
old = spark.table("tdm_seg.cust_uplift_by_mech_tempthanakritboonquarmdeelotusscom")

# COMMAND ----------

# DBTITLE 1,Dev code v2 - helper v2


# COMMAND ----------

from matching import store_matching

matched_store = store_matching.get_store_matching_across_region(cmp)

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql import Window

txn = spark.createDataFrame([("a", date(2023, 2, 1), date(2023, 2, 10)),
                             ("a", date(2023, 2, 3), date(2023, 2, 10)),
                             ("b", date(2023, 2, 5), date(2023, 2, 10)),
                             ("b", date(2023, 2, 9), date(2023, 2, 10))],
                            ["cust_id", "exposed", "purchase"])


(txn.withColumn('time_diff', F.col('purchase') - F.col('exposed'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('purchase').orderBy(F.col('time_diff'))))
).display()
    

# COMMAND ----------


(txn.withColumn('time_diff', F.col('purchase') - F.col('exposed'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('cust_id','purchase').orderBy(F.col('time_diff'))))
).display()

# COMMAND ----------


