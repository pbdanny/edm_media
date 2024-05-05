# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np

from numpy import ndarray as numpyNDArray
from pandas import DataFrame as PandasDataFrame

from typing import List
from json import dumps

import sys
import os

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

TBL_ITEM = 'tdm.v_transaction_item'
TBL_BASK = 'tdm.v_transaction_head'
TBL_PROD = 'tdm.v_prod_dim_c'
TBL_STORE = 'tdm.v_store_dim'
TBL_DATE = 'tdm.v_th_date_dim'

TBL_STORE_GR = 'tdm.srai_std_rms_store_group_feed'
TBL_SUB_CHANNEL = 'tdm_seg.online_txn_subch_15aug'
TBL_DHB_CUST_SEG = 'tdm.dh_customer_segmentation'
TBL_G_ID = 'tdm.cde_golden_record'
TBL_CUST = 'tdm.v_customer_dim'

TBL_CUST_PROFILE = 'tdm.edm_customer_profile'
TBL_MANUF = 'tdm.v_mfr_dim'

KEY_DIVISION = [1,2,3,4,9,10,13]
KEY_STORE = [1,2,3,4,5]

BASIC_KPI =  [F.countDistinct('store_id').alias('n_store'),
              F.sum('net_spend_amt').alias('sales'),
              F.sum('unit').alias('units'),
              F.countDistinct('transaction_uid').alias('visits'),
              F.countDistinct('household_id').alias('custs'),
              (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
              (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
              (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
              (F.sum('unit')/F.countDistinct('transaction_uid')).alias('upv'),
              (F.sum('net_spend_amt')/F.sum('unit')).alias('ppu')]

# COMMAND ----------

facts_desc = spark.createDataFrame([
    (-1,'Not available'), 
    (1, 'Primary'),
    (2, 'Secondary'),
    (3, 'Tertiary')]
    ,('facts_seg', 'description'))
F.broadcast(facts_desc)

facts_lv2_desc = spark.createDataFrame([
    (-1, 'Not available'),
    (1, 'Primary High'),
    (2, 'Primary Standard'),
    (3, 'Secondary Grow Frequency'),
    (4, 'Secondary Grow Breadth'),
    (5, 'Tertiary Standard'),
    (6, 'Tertiary (OTB)')],
    ('facts_level2_seg', 'description'))
F.broadcast(facts_lv2_desc)

truprice_desc = spark.createDataFrame([
    (1, 'Most Price Driven'),
    (2, 'Price Driven'),
    (3, 'Price Neutral'),
    (4, 'Price Insensitive'),
    (5, 'Most Price Insensitive')], 
    ('truprice_seg', 'description'))
F.broadcast(truprice_desc)

life_cyc_desc = spark.createDataFrame([
    ('newbie', 1),
    ('infrequent', 2),
    ('growing', 3),
    ('stable', 4),
    ('declining', 5),
    ('lapser', 6),
    ('goner', 7)],
    ('lifecycle_name', 'lifecycle_code'))
F.broadcast(life_cyc_desc)

lyt_desc = spark.createDataFrame([
    ('premium', 1),
    ('valuable', 2),
    ('potential', 3),
    ('uncommitted', 4)],
    ('loyalty_name', 'loyalty_code'))
F.broadcast(lyt_desc)

life_cyc_lv2_desc = spark.read.csv('dbfs:/mnt/pvtdmbobazc01/edminput/filestore/user/thanawat_asa/lifecycle_segmentation_prod/lifecycle_detailed_name_def.csv', header=True, inferSchema=True)
F.broadcast(life_cyc_lv2_desc)
