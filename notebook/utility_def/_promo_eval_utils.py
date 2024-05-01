# Databricks notebook source
# MAGIC %md
# MAGIC # Import all library

# COMMAND ----------

## Pat Add import all lib here -- 25 May 2022
## import function
##---------------------------------------------------
## Need to import all function in each notebook
##---------------------------------------------------

## import pyspark sql

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

from pyspark import StorageLevel


## import longging and traceback
import logging
import traceback
import errno

## datetime lib
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time

## pandas and numpy
import pandas as pd
import numpy as np
import math as math

## os path
import os
import sys
import string
import subprocess
import importlib
import shutil
import urllib 
import pathlib

# COMMAND ----------

# MAGIC %md
# MAGIC # Import media utility def

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

def cust_kpi(txn, 
             store_fmt,
             test_store_sf,
             feat_list):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - All Feature subclass
    - Feature Brand in class
    - All Feature class
    """ 
    #---- Features products, brand, classs, subclass
    features_brand = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('brand_name').drop_duplicates()
    features_class = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id').drop_duplicates()
    features_subclass = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    
    txn_test_pre = txn_all.join(test_store_sf.select('store_id').drop_duplicates() , 'store_id').filter(F.col('period').isin(['pre']))
    txn_test_cmp = txn_all.join(test_store_sf.select('store_id').drop_duplicates(), 'store_id').filter(F.col('period').isin(['cmp']))
    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('customer_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )

    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
    
    sku_kpi = \
    (txn_test_combine
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('period')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_sku'))
    )
    
    brand_in_subclass_kpi = \
    (txn_test_combine
     .join(features_brand, 'brand_name')
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('period')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_subclass'))
    )
    
    all_subclass_kpi = \
    (txn_test_combine
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('period')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_subclass'))
    )    
    
    brand_in_class_kpi = \
    (txn_test_combine
     .join(features_brand, 'brand_name')
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('period')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_class'))
    )
    
    all_class_kpi = \
    (txn_test_combine
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('period')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_class'))
    )
    
    combined_kpi = sku_kpi.unionByName(brand_in_subclass_kpi).unionByName(all_subclass_kpi).unionByName(brand_in_class_kpi).unionByName(all_class_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['period', 'kpi_level', 'MyLo_customer']].pivot(index='period', columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_subclass'] = df_pv['feature_sku']/df_pv['feature_subclass']*100
    df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_subclass'] = df_pv['feature_brand_in_class']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv.sort_index(ascending=False, inplace=True)
    
    return combined_kpi, kpi_df, df_pv
