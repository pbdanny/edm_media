# Databricks notebook source
# MAGIC %md ##Import Library

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

import os
from pandas import DataFrame as PandasDataFrame
from typing import List

from json import dumps

from numpy import ndarray as numpyNDArray

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

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', "true")

# spark.conf.set('spark.sql.adaptive.enabled', 'true')
# spark.conf.set('spark.databricks.adaptive.autoBroadcastJoinThreshold', '30MB')

# spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
# spark.conf.set('spark.sql.adaptive.advisoryPartitionSizeInBytes', '64MB')
# spark.conf.set('spark.sql.adaptive.coalescePartitions.minPartitionSize', '1MB')

# spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')
# spark.conf.set('spark.sql.adaptive.skewJoin.skewedPartitionFactor', '5')
# spark.conf.set('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes', '256MB')

# spark.conf.set('spark.databricks.adaptive.emptyRelationPropagation.enabled', 'true')

# spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
# spark.conf.set('spark.databricks.delta.autoCompact.enabled', 'true')

# COMMAND ----------

# TBL_ITEM = 'tdm.v_srai_std_resa_tran_item'
TBL_ITEM = 'tdm.v_transaction_item'
# TBL_BASK = 'tdm.v_srai_std_resa_tran_head'
TBL_BASK = 'tdm.v_transaction_head'
# TBL_PROD = 'tdm.tdm.v_srai_std_rms_product'
TBL_PROD = 'tdm.v_prod_dim'
# TBL_STORE = 'tdm.v_srai_std_rms_store'
TBL_STORE = 'tdm.v_store_dim'
TBL_DATE = 'tdm.v_th_date_dim'

TBL_STORE_GR = 'tdm.srai_std_rms_store_group_feed'
TBL_SUB_CHANNEL = 'tdm_seg.online_txn_subch_15aug'
TBL_DHB_CUST_SEG = 'tdm.dh_customer_segmentation'
TBL_G_ID = 'tdm.cde_golden_record'
# TBL_CUST = 'tdm.v_srai_std_customer'
TBL_CUST = 'tdm.v_customer_dim'

TBL_CUST_PROFILE = 'tdm.edm_customer_profile'
TBL_MANUF = 'tdm.v_mfr_dim'

KEY_DIVISION = [1,2,3,4,9,10,13]
KEY_STORE = [1,2,3,4,5]

# BASIC_KPI =  [F.countDistinct('store_id').alias('n_store'),
#               F.sum('net_spend_amt').alias('sales'),
#               F.sum('pkg_weight_unit').alias('units'),
#               F.countDistinct('transaction_uid').alias('visits'),
#               F.countDistinct('golden_record_external_id_hash').alias('customers'),
#               (F.sum('net_spend_amt')/F.countDistinct('golden_record_external_id_hash')).alias('spc'),
#               (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
#               (F.countDistinct('transaction_uid')/F.countDistinct('golden_record_external_id_hash')).alias('vpc'),
#               (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
#               (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]

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

# COMMAND ----------

# MAGIC %md ##Txn Function

# COMMAND ----------

def _map_format_channel(txn: SparkDataFrame):
    """Helper function for 'get_trans_itm_wkly' and 'get_trans_item_raw'
       use column 'channel', from head, and 'store_format_group', from mapping between head - store
       to create offline_online_other_channel, store_format_online_subchannel_other
    """
#     mapped_chan_txn = \
#     (txn
#      .withColumn('channel', F.trim(F.col('channel')))
#      .withColumn('_channel_group',
#                  F.when(F.col('channel')=='OFFLINE', F.lit('OFFLINE'))
#                   .when(F.col('channel').isin(['Click and Collect','Chat and Collect','Chat and Shop']), F.lit('Click and Collect'))
#                   .when(F.col('channel').isin(['GHS 1','GHS 2']), F.lit('GHS'))
#                   .when(F.col('channel')=='Light Delivery', F.lit('Light Delivery'))
#                   .when(F.col('channel').isin(['Shopee','Lazada','O2O Shopee','O2O Lazada']), F.lit('Market Place'))
#                   .when(F.col('channel').isin(['Ant Delivery ','Food Panda','Go Fresh Delivery','Grabmart',
#                                                  'We Fresh','Happy Fresh','Robinhood']), F.lit('Normal Delivery'))
#                   .otherwise(F.lit('Others/Direct Sales')))
#      .withColumn('offline_online_other_channel', 
#                   F.when(F.col('_channel_group')=='OFFLINE', F.lit('OFFLINE'))
#                    .when(F.col('_channel_group')=='Others/Direct Sales', F.lit('Others/Direct Sales'))
#                    .otherwise(F.lit('ONLINE')))
#      .withColumn('store_format_online_subchannel_other',
#                  F.when(F.col('offline_online_other_channel')=='OFFLINE', F.col('store_format_group'))
#                   .when(F.col('offline_online_other_channel')=='ONLINE', F.col('_channel_group'))
#                   .otherwise(F.col('offline_online_other_channel')))
#     ).drop('_channel_group')
    
    mapped_chan_txn = \
    (txn
     .withColumn("channel", F.trim(F.col("channel")))
     .withColumn("channel_group_forlotuss",
                F.when(F.col("channel")=="OFFLINE", "OFFLINE")
                .when(F.col("channel").isin(["Click and Collect", "HATO", "Scheduled CC"]), "Click and Collect + HATO")
                .when(F.col("channel").isin(["GHS 1", "GHS 2", "GHS APP", "Light Delivery",
                                             "Scheduled HD", "OnDemand HD"]), "GHS")
                .when(F.col("channel").isin(["Shopee", "Lazada", "O2O Lazada", "O2O Shopee"]), "Marketplace")
                .when(F.col("channel").isin(["Ant Delivery", "Food Panda", "Grabmart",
                                        "Happy Fresh", "Robinhood", "We Fresh", "7 MARKET"]), "Aggregator")
                .when(F.col("channel").isin(["HLE", "TRUE SMART QR"]), "Others")
                .otherwise(F.lit("OFFLINE")))
     
     .withColumn("offline_online_other_channel",
                F.when(F.col("channel_group_forlotuss")=="OFFLINE", F.lit("OFFLINE"))
                .otherwise(F.lit("ONLINE")))
     
     .withColumn("store_format_online_subchannel_other",
                        F.when(F.col("offline_online_other_channel")=="OFFLINE", F.col("store_format_group"))
                        .otherwise(F.col("channel_group_forlotuss")))
    )
    
    return mapped_chan_txn

# COMMAND ----------

def get_trans_itm(end_date_txt: str, period_n_week: int, use_business_date: bool = False, 
                  customer_data: str = 'EPOS', manuf_name: bool = False,
                  store_format: List = [1,2,3,4,5], division: List = [1,2,3,4,9,10,13],
                  item_col_select: List = ['transaction_uid', 'store_id', 'date_id', 'upc_id', 
                                          'net_spend_amt', 'pkg_weight_unit', 'customer_id', 'tran_datetime'], 
                  prod_col_select: List = ['upc_id', 'division_name', 'department_name', 
                                          'section_name', 'section_id', 'class_name', 'class_id']) -> SparkDataFrame:
    """Get transaction data with standard criteria

    Parameter
    ---------
    end_date: str
        End date for analysis, with format YYYY-MM-DD ex. 2021-08-12

    period_n_week: int
        Number of period in week of analysis ex. 1 for 1 week, 4 for 4 weeks (1 mth), 13 for 3 mths, 26 for 6 mth and 52 for 12 mth 

    use_business_date: bool , default = False
        To use business_date or date_id for period cut-off & analysis
        
    manuf_name: bool, default = False
        To map the manufacturer code & name
    
    customer_data: str, default = 'EPOS'
        For all transaction data use 'EPOS', for clubcard data only use 'CC'
    
    item_col_select:List , default ['transaction_uid', 'store_id', 'date_id', 'upc_id', 'net_amt', 'pkg_weight_unit', 'customer_id']
        List of additional columns from item table; *store_id , *pkg_weight_unit is derived columns
        
    prod_col_select:List , default ['upc_id', 'division_name', 'department_name', 
                                   'section_name', 'section_id', 'class_name', 'class_id']
        List of additional columns from prod table
    
    Return
    ------
    SparkDataFrame 
    """
    from datetime import datetime, timedelta
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql import DataFrame as SparkDataFrame

    from typing import List
    from copy import deepcopy
    
    BUFFER_END_DATE:int = 7
    ITEM_COL_SELECT = deepcopy(item_col_select)
    PROD_COL_SELECT = deepcopy(prod_col_select)
    
    end_date_id = datetime.strptime(end_date_txt, '%Y-%m-%d')
    str_date_id = end_date_id + timedelta(weeks=-int(period_n_week)) + timedelta(days=1)
    # str_date_txt = str_date_id.strftime('%Y%m%d')

    # Use date start-end for predicate in table loading
    # str_date_id = datetime.strptime(start_date, '%Y%m%d')
    # end_date_id = datetime.strptime(end_date, '%Y%m%d')
    str_dp_data_dt = str_date_id
    end_dp_data_dt = end_date_id + timedelta(days=BUFFER_END_DATE)
    
    print('-'*30)
    print('PARAMETER LOAD')
    print('-'*30)
    print(f'Data end date : {end_date_id.strftime("%Y-%m-%d")}')
    print(f'Period of weeks : {period_n_week} weeks')
    print(f'Data start date : {str_date_id.strftime("%Y-%m-%d")}')

    day_diff = end_date_id - str_date_id
    period_day = day_diff.days + 1  #include start & end date = difference + 1 days
    print(f'Total data period : {period_day} days')
    print('-'*30)
    
    #---- Item 
    item:SparkDataFrame = \
        (spark.table(TBL_ITEM)
         .filter(F.col('country')=='th')
         .filter(F.col('source')=='resa')
         .filter(F.col('dp_data_dt').between(str_dp_data_dt, end_dp_data_dt))
         
         .filter(F.col('net_spend_amt')>0)
         .filter(F.col('product_qty')>0)
         
         .withColumnRenamed('counted_qty', 'count_qty')
         .withColumnRenamed('measured_qty', 'measure_qty')
         
         .withColumn('pkg_weight_unit', F.when(F.col('count_qty').isNotNull(), F.col('product_qty')).otherwise(F.col('measure_qty')))

#         .select(['transaction_uid', 'store_id', 'date_id', 'upc_id', 'net_amt', 'pkg_weight_unit', 'customer_id', 'business_date'])
    )
    
    #--- if use business_date as date scope
    if use_business_date:
        item:SparkDataFrame = \
            (item
             .withColumn('business_date', F.coalesce(F.to_date('business_date'), F.col('date_id')))
             .filter(F.col('business_date').between(str_date_id, end_date_id))
            )
        # If use business date, add businee_date in output columns
        ITEM_COL_SELECT.append('business_date')
    
    #---- if not use business date    
    else:
        item:SparkDataFrame = \
            (item
             .filter(F.col('date_id').between(str_date_id, end_date_id))
            )
        
    #---- Basket
    # Get store_id and date_id for new joining logic - Dec 2022 - Ta
    bask:SparkDataFrame = (
        spark.table(TBL_BASK)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='resa')
        .filter(F.col('dp_data_dt').between(str_dp_data_dt, end_dp_data_dt))
        .filter(F.col('date_id').between(str_date_id, end_date_id))
        
        .filter(F.col('net_spend_amt')>0)
        .filter(F.col('total_qty')>0)
        
#         .withColumn('offline_online', F.when(F.col('channel')=='OFFLINE', 'OFFLINE').otherwise('ONLINE'))
        
        .select('transaction_uid', 'channel', 'store_id', 'date_id')
        .drop_duplicates()
    )
    
    #---- Store
    store:SparkDataFrame = (
        spark.table(TBL_STORE)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('format_id').isin(store_format))
        
        .filter(~F.col('store_id').like('8%')) #
        
        .withColumn('store_format_group', F.when(F.col('format_id').isin([1,2,3]),'HDE')
                                           .when(F.col('format_id')==4,'Talad')
                                           .when(F.col('format_id')==5,'GoFresh'))
        .withColumnRenamed('region', 'store_region')
        .select('store_id', 'store_format_group', 'store_region')
        .drop_duplicates()
    )
        
    #---- Product
    prod:SparkDataFrame = (
        spark.table(TBL_PROD)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('division_id').isin(division))
        .withColumn('department_name')
#         .select('upc_id', 'division_name', 'department_name', 'section_name', 
#         'class_name', 'subclass_name', 'product_desc', 'product_en_desc', 'brand_name', 'mfr_id')
        .drop_duplicates()
    )
    filter_division_prod_id = prod.select('upc_id').drop_duplicates() 
    
    #---- Manufacturer
    mfr:SparkDataFrame = (
        spark.table(TBL_MANUF)
        .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
        .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name-7)'))
        .select('mfr_id', 'mfr_only_name')
        .withColumnRenamed('mfr_only_name', 'manuf_name')
        .drop_duplicates()
    )
    #---- If need manufacturer name, add output columns, add manuf name in prod sparkDataFrame
    if manuf_name:
        with_manuf_col_list = PROD_COL_SELECT + ['mfr_id', 'manuf_name']
        PROD_COL_SELECT = list(set(with_manuf_col_list)) # Dedup add columns
        prod = prod.join(mfr, 'mfr_id', 'left')
        
    #---- Result spark dataframe
    # Revised basket joining logic - Dec 2022 - Ta
    sf = \
    (item
     .select(ITEM_COL_SELECT)
     .join(F.broadcast(filter_division_prod_id), 'upc_id', 'inner')
     .join(F.broadcast(store), 'store_id')
     .join(bask, on=['transaction_uid', 'date_id', 'store_id'], how='left')
     .join(prod.select(PROD_COL_SELECT), 'upc_id')
    )
    
    #---- mapping offline-online-other channel
    sf = _map_format_channel(sf)
    
    #---- Mapping household_id
    party_mapping = \
    (spark.table(TBL_CUST)
     .select('customer_id', 'household_id')
     .drop_duplicates()
    )
    
    #---- Filter out only Clubcard data
    if customer_data == 'CC':
        sf = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        
    else:
        cc = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        non_cc = sf.filter(F.col('customer_id').isNull())
        
        sf = cc.unionByName(non_cc, allowMissingColumns=True)
    
    # Dec 2022 - Add transaction_uid made of concatenated data from transaction_uid, date_id and store_id. Keep original transaction_uid as separate column
    sf = sf.withColumn('transaction_uid_orig', F.col('transaction_uid')) \
           .withColumn('transaction_uid', F.concat_ws('_', F.col('transaction_uid'), F.col('date_id'), F.col('store_id')))
    
    #---- print result column
    txn_cols = sf.columns
    print(f'Output columns : {txn_cols}')
    print('-'*30)
    
    return sf

# COMMAND ----------

def get_trans_itm_wkly_promo(start_week_id: int, 
                             end_week_id: int, 
                             customer_data: str = 'EPOS', 
                             manuf_name: bool = False,
                             store_format: List = [1,2,3,4,5], 
                             division: List = [1,2,3,4,9,10,13],
                             item_col_select: List = ['transaction_uid', 'store_id', 'date_id', 'week_id', 'upc_id', 
                                                      'net_spend_amt', 'pkg_weight_unit', 'customer_id', 'promoweek_id', 'tran_datetime'], 
                             prod_col_select: List = ['upc_id', 'division_name', 'department_name', 
                                                'section_name', 'section_id', 'class_name', 'class_id']) -> SparkDataFrame:
    """Get transaction data with standard criteria, from weely data feed
    If defined end_week_id, will ignore end_date_txt and pull data based on week
    If defined end_date_txt, will pull the week_id and filter only date id needed

    Parameter
    ---------
    start_wk_id: int
        Start fisweek id
    
    end_week_id: int
        End fisweek id
        
    use_business_date: bool , default = False
        To use business_date or date_id for period cut-off & analysis
        
    manuf_name: bool, default = False
        To map the manufacturer code & name
    
    customer_data: str, default = 'EPOS'
        For all transaction data use 'EPOS', for clubcard data only use 'CC'
    
    item_col_select:List , default ['transaction_uid', 'store_id', 'date_id', 'upc_id', 'net_amt', 'pkg_weight_unit', 'customer_id']
        List of additional columns from item table; *store_id , *pkg_weight_unit is derived columns
        
    prod_col_select:List , default ['upc_id', 'division_name', 'department_name', 
                                   'section_name', 'section_id', 'class_name', 'class_id']
        List of additional columns from prod table
    
    Return
    ------
    SparkDataFrame 
    """
    
    from datetime import datetime, timedelta
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql import DataFrame as SparkDataFrame

    from typing import List
    from copy import deepcopy
    
    ITEM_COL_SELECT = deepcopy(item_col_select)
    PROD_COL_SELECT = deepcopy(prod_col_select)
    
    ## Get promoweek_id to txn table (no filter on date table)
    data_period = \
    (spark.table(TBL_DATE)
          .where(F.col('promoweek_id').between(start_week_id, end_week_id))
          .select('date_id', 'promoweek_id')
          .drop_duplicates()
    )
    print('-'*80)
    print('TRANSACTION CREATTION')
    print('-'*80)
    print(f'Data start Promo week : {start_week_id}')
    print(f'Data end Promo week : {end_week_id}')
    print('-'*80)
    
    ## get transaction filter using promo week id
    
    item:SparkDataFrame = \
        (spark.table(TBL_ITEM)
              .join (data_period, 'date_id', 'inner')
        )
    
    ## add fitler basket using promoweek_id
    
    bask:SparkDataFrame = \
        (spark.table(TBL_BASK)
              .join (data_period, 'date_id', 'inner')
        )
        
    #---- Item, Head : filter by partition
    if customer_data == 'CC':
        item = item.filter(F.col('cc_flag').isin(['cc']))
        bask = bask.filter(F.col('cc_flag').isin(['cc']))

    #---- filter net_spend_amt > 0 , product_qty > 0 , calculate units
    item = (item
            .filter(F.col('net_spend_amt')>0)
            .filter(F.col('product_qty')>0)

            .withColumnRenamed('counted_qty', 'count_qty')
            .withColumnRenamed('measured_qty', 'measure_qty')

            .withColumn('pkg_weight_unit', F.when(F.col('count_qty').isNotNull(), F.col('product_qty')).otherwise(F.col('measure_qty')))
           )
    
    #---- Basket, filter net_spend_amt > 0 , total_qty > 0 , get channel
    # Also select store_id and date_id for new joining logic - Dec 2022 - Ta
    bask = \
    (bask     
     .filter(F.col('net_spend_amt')>0)
     .filter(F.col('total_qty')>0)     
     .select('transaction_uid', 'channel', 'store_id', 'date_id')
     .drop_duplicates()
    )
    
    #---- Store
    store:SparkDataFrame = (
        spark.table(TBL_STORE)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('format_id').isin(store_format))
        
        .filter(~F.col('store_id').like('8%'))
        
        .withColumn('store_format_group', F.when(F.col('format_id').isin([1,2,3]),'HDE')
                                             .when(F.col('format_id')==4,'Talad')
                                             .when(F.col('format_id')==5,'GoFresh')
                                             .when(F.col('format_id')==6,'B2B')
                                             .when(F.col('format_id')==7,'Cafe')
                                             .when(F.col('format_id')==8,'Wholesale')

                   )
        .withColumnRenamed('region', 'store_region')
        .select('store_id', 'store_format_group', 'store_region')
        .drop_duplicates()
    )
        
    #---- Product
    prod:SparkDataFrame = (
        spark.table('tdm.v_prod_dim_c')
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('division_id').isin(division))
        .withColumn('department_name', F.trim(F.col('department_name')))
        .withColumn('section_name', F.trim(F.col('section_name')))
        .withColumn('class_name', F.trim(F.col('class_name')))
        .withColumn('subclass_name', F.trim(F.col('subclass_name')))
        .withColumn('brand_name', F.trim(F.col('brand_name')))
#         .select('upc_id', 'division_name', 'department_name', 'section_name', 
#         'class_name', 'subclass_name', 'product_desc', 'product_en_desc', 'brand_name', 'mfr_id')
        .drop_duplicates()
    )
    filter_division_prod_id = prod.select('upc_id').drop_duplicates() 
    
    #---- Manufacturer
    mfr:SparkDataFrame = (
        spark.table(TBL_MANUF)
        .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
        .withColumn('len_mfr_id', F.length(F.regexp_extract('mfr_name', r'(-\d+)', 1)))
        .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name - len_mfr_id)'))
        .select('mfr_id', 'mfr_only_name')
        .withColumnRenamed('mfr_only_name', 'manuf_name')
        .drop_duplicates()
    )
    #---- If need manufacturer name, add output columns, add manuf name in prod sparkDataFrame
    if manuf_name:
        with_manuf_col_list = PROD_COL_SELECT + ['mfr_id', 'manuf_name']
        PROD_COL_SELECT = list(set(with_manuf_col_list)) # Dedup add columns
        prod = prod.join(mfr, 'mfr_id', 'left')
        
    #---- Result spark dataframe
    # Revise joining logic - Dec 2022 - Ta
    sf = \
    (item
     .select(ITEM_COL_SELECT)
     .join(F.broadcast(filter_division_prod_id), 'upc_id', 'inner')
     .join(F.broadcast(store), 'store_id')
     .join(bask, on=['transaction_uid', 'date_id', 'store_id'], how='left')
     .join(prod.select(PROD_COL_SELECT), 'upc_id')
    )
    #---- mapping channel
    sf = _map_format_channel(sf)
    
    #---- Mapping household_id
    party_mapping = \
    (spark.table(TBL_CUST)
     .select('customer_id', 'household_id')
     .drop_duplicates()
    )
    
    #---- Filter out only Clubcard data
    if customer_data == 'CC':
        sf = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        
    else:
        cc = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        non_cc = sf.filter(F.col('customer_id').isNull())
        
        sf = cc.unionByName(non_cc, allowMissingColumns=True)
    
    #---- print result column
    txn_cols = sf.columns
    print(f'Output columns : {txn_cols}')
    print('-'*30)
    
    # Dec 2022 - Add transaction_uid made of concatenated data from transaction_uid, date_id and store_id. Keep original transaction_uid as separate column
    sf = sf.withColumn('transaction_uid_orig', F.col('transaction_uid')) \
           .withColumn('transaction_uid', F.concat_ws('_', F.col('transaction_uid'), F.col('date_id'), F.col('store_id')))
    
    return sf

# COMMAND ----------

import functools

def adjust_gofresh_region(func):
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # print('Before main function')
        txn = func(*args, **kwargs)
        print('Mapping new region')
        adjusted_store_region =  \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .drop_duplicates()
        )
        txn = txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left')
        return txn
    
    return wrapper

# COMMAND ----------

def get_trans_itm_wkly(start_week_id: int, 
                       end_week_id: int, 
                       customer_data: str = 'EPOS', 
                       manuf_name: bool = False,
                       store_format: List = [1,2,3,4,5], 
                       division: List = [1,2,3,4,9,10,13],
                       item_col_select: List = ['transaction_uid', 'store_id', 'date_id', 'week_id', 'upc_id', 
                                                'net_spend_amt', 'pkg_weight_unit', 'customer_id', 'promoweek_id', 'tran_datetime'], 
                       prod_col_select: List = ['upc_id', 'division_name', 'department_name', 
                                                'section_name', 'section_id', 'class_name', 'class_id']) -> SparkDataFrame:
    """Get transaction data with standard criteria, from weely data feed
    If defined end_week_id, will ignore end_date_txt and pull data based on week
    If defined end_date_txt, will pull the week_id and filter only date id needed

    Parameter
    ---------
    start_wk_id: int
        Start fisweek id
    
    end_week_id: int
        End fisweek id
        
    use_business_date: bool , default = False
        To use business_date or date_id for period cut-off & analysis
        
    manuf_name: bool, default = False
        To map the manufacturer code & name
    
    customer_data: str, default = 'EPOS'
        For all transaction data use 'EPOS', for clubcard data only use 'CC'
    
    item_col_select:List , default ['transaction_uid', 'store_id', 'date_id', 'upc_id', 'net_amt', 'pkg_weight_unit', 'customer_id']
        List of additional columns from item table; *store_id , *pkg_weight_unit is derived columns
        
    prod_col_select:List , default ['upc_id', 'division_name', 'department_name', 
                                   'section_name', 'section_id', 'class_name', 'class_id']
        List of additional columns from prod table
    
    Return
    ------
    SparkDataFrame 
    """
    
    from datetime import datetime, timedelta
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql import DataFrame as SparkDataFrame

    from typing import List
    from copy import deepcopy
    
    ITEM_COL_SELECT = deepcopy(item_col_select)
    PROD_COL_SELECT = deepcopy(prod_col_select)
    
    data_period = \
    (spark.table(TBL_DATE)
        .filter(F.col('week_id').between(start_week_id, end_week_id))
        .select('date_id', 'promoweek_id')
        .drop_duplicates()
    )
    print('-'*80)
    print('TRANSACTION CREATTION')
    print('-'*80)
    print(f'Data start fis week : {start_week_id}')
    print(f'Data end fis week : {end_week_id}')
    print('-'*80)
        
    item:SparkDataFrame = \
        (spark.table(TBL_ITEM)
         .filter(F.col('week_id').between(start_week_id, end_week_id))
         .join(data_period, 'date_id')
        )
        
    bask:SparkDataFrame = \
        (spark.table(TBL_BASK)
         .filter(F.col('week_id').between(start_week_id, end_week_id))
        )
        
    #---- Item, Head : filter by partition
    if customer_data == 'CC':
        item = item.filter(F.col('cc_flag').isin(['cc']))
        bask = bask.filter(F.col('cc_flag').isin(['cc']))

    #---- filter net_spend_amt > 0 , product_qty > 0 , calculate units
    item = (item
            .filter(F.col('net_spend_amt')>0)
            .filter(F.col('product_qty')>0)

            .withColumnRenamed('counted_qty', 'count_qty')
            .withColumnRenamed('measured_qty', 'measure_qty')

            .withColumn('pkg_weight_unit', F.when(F.col('count_qty').isNotNull(), F.col('product_qty')).otherwise(F.col('measure_qty')))
           )
    
    #---- Basket, filter net_spend_amt > 0 , total_qty > 0 , get channel
    # Add store_id and date_id for new joining logic - Dec 2022 - Ta
    bask = \
    (bask     
     .filter(F.col('net_spend_amt')>0)
     .filter(F.col('total_qty')>0)     
     .select('transaction_uid', 'channel', 'store_id', 'date_id')
     .drop_duplicates()
    )
    
    #---- Store
    store:SparkDataFrame = (
        spark.table(TBL_STORE)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('format_id').isin(store_format))
        
        .filter(~F.col('store_id').like('8%'))
        
        .withColumn('store_format_group', F.when(F.col('format_id').isin([1,2,3]),'HDE')
                                             .when(F.col('format_id')==4,'Talad')
                                             .when(F.col('format_id')==5,'GoFresh')
                                             .when(F.col('format_id')==6,'B2B')
                                             .when(F.col('format_id')==7,'Cafe')
                                             .when(F.col('format_id')==8,'Wholesale')

                   )
        .withColumnRenamed('region', 'store_region')
        .select('store_id', 'store_format_group', 'store_region')
        .drop_duplicates()
    )
        
    #---- Product
    prod:SparkDataFrame = (
        spark.table('tdm.v_prod_dim_c')
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('division_id').isin(division))
        .withColumn('department_name', F.trim(F.col('department_name')))
        .withColumn('section_name', F.trim(F.col('section_name')))
        .withColumn('class_name', F.trim(F.col('class_name')))
        .withColumn('subclass_name', F.trim(F.col('subclass_name')))
        .withColumn('brand_name', F.trim(F.col('brand_name')))
#         .select('upc_id', 'division_name', 'department_name', 'section_name', 
#         'class_name', 'subclass_name', 'product_desc', 'product_en_desc', 'brand_name', 'mfr_id')
        .drop_duplicates()
    )
    filter_division_prod_id = prod.select('upc_id').drop_duplicates() 
    
    #---- Manufacturer
    mfr:SparkDataFrame = (
        spark.table(TBL_MANUF)
        .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
        .withColumn('len_mfr_id', F.length(F.regexp_extract('mfr_name', r'(-\d+)', 1)))
        .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name - len_mfr_id)'))
        .select('mfr_id', 'mfr_only_name')
        .withColumnRenamed('mfr_only_name', 'manuf_name')
        .drop_duplicates()
    )
    #---- If need manufacturer name, add output columns, add manuf name in prod sparkDataFrame
    if manuf_name:
        with_manuf_col_list = PROD_COL_SELECT + ['mfr_id', 'manuf_name']
        PROD_COL_SELECT = list(set(with_manuf_col_list)) # Dedup add columns
        prod = prod.join(mfr, 'mfr_id', 'left')
        
    #---- Result spark dataframe
    # New joining logic - Dec 2022 - Ta
    sf = \
    (item
     .select(ITEM_COL_SELECT)
     .join(F.broadcast(filter_division_prod_id), 'upc_id', 'inner')
     .join(F.broadcast(store), 'store_id')
     .join(bask, on=['transaction_uid', 'store_id', 'date_id'], how='left')
     .join(prod.select(PROD_COL_SELECT), 'upc_id')
    )
    #---- mapping channel
    sf = _map_format_channel(sf)
    
    #---- Mapping household_id
    party_mapping = \
    (spark.table(TBL_CUST)
     .select('customer_id', 'household_id')
     .drop_duplicates()
    )
    
    #---- Filter out only Clubcard data
    if customer_data == 'CC':
        sf = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        
    else:
        cc = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        non_cc = sf.filter(F.col('customer_id').isNull())
        
        sf = cc.unionByName(non_cc, allowMissingColumns=True)
    
    #---- print result column
    txn_cols = sf.columns
    print(f'Output columns : {txn_cols}')
    print('-'*30)
    
    # Dec 2022 - Add transaction_uid made of concatenated data from transaction_uid, date_id and store_id. Keep original transaction_uid as separate column
    sf = sf.withColumn('transaction_uid_orig', F.col('transaction_uid')) \
           .withColumn('transaction_uid', F.concat_ws('_', F.col('transaction_uid'), F.col('date_id'), F.col('store_id')))
    
    return sf

# COMMAND ----------

def get_trans_itm_raw(end_date_txt: str, period_n_week: int, use_business_date: bool = False, 
                  customer_data: str = 'EPOS', manuf_name: bool = False,
                  store_format: List = [1,2,3,4,5], division: List = [1,2,3,4,9,10,13],
                  item_col_select: List = ['transaction_uid', 'store_id', 'date_id', 'week_id', 'upc_id', 
                                          'net_spend_amt', 'pkg_weight_unit', 'customer_id'], 
                  prod_col_select: List = ['upc_id', 'division_name', 'department_name', 
                                          'section_name', 'section_id', 'class_name', 'class_id']) -> SparkDataFrame:
    """Get transaction data from daily updated feed, with standard criteria

    Parameter
    ---------
    end_date: str
        End date for analysis, with format YYYY-MM-DD ex. 2021-08-12

    period_n_week: int
        Number of period in week of analysis ex. 1 for 1 week, 4 for 4 weeks (1 mth), 13 for 3 mths, 26 for 6 mth and 52 for 12 mth 

    use_business_date: bool , default = False
        To use business_date or date_id for period cut-off & analysis
        
    manuf_name: bool, default = False
        To map the manufacturer code & name
    
    customer_data: str, default = 'EPOS'
        For all transaction data use 'EPOS', for clubcard data only use 'CC'
    
    item_col_select:List , default ['transaction_uid', 'store_id', 'date_id', 'upc_id', 'net_amt', 'pkg_weight_unit', 'customer_id']
        List of additional columns from item table; *store_id , *pkg_weight_unit is derived columns
        
    prod_col_select:List , default ['upc_id', 'division_name', 'department_name', 
                                   'section_name', 'section_id', 'class_name', 'class_id']
        List of additional columns from prod table
    
    Return
    ------
    SparkDataFrame 
    """
    
    from datetime import datetime, timedelta
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql import DataFrame as SparkDataFrame

    from typing import List
    from copy import deepcopy
    
    BUFFER_END_DATE:int = 7
    ITEM_COL_SELECT = deepcopy(item_col_select)
    PROD_COL_SELECT = deepcopy(prod_col_select)
    
    end_date_id = datetime.strptime(end_date_txt, '%Y-%m-%d')
    str_date_id = end_date_id + timedelta(weeks=-int(period_n_week)) + timedelta(days=1)
    
    str_dp_data_dt = str_date_id
    end_dp_data_dt = end_date_id + timedelta(days=BUFFER_END_DATE)
    
    # From end_date_id, str_date_id find week_id
    week_id = \
    (spark.table(TBL_DATE)
     .filter(F.col('date_id').between(str_date_id, end_date_id))
     .select('date_id', 'week_id')
     .drop_duplicates()
    )
    start_week_id = week_id.agg(F.min('week_id')).collect()[0][0]
    end_week_id = week_id.agg(F.max('week_id')).collect()[0][0]
    
    print('-'*30)
    print('Get DAILY transaction feed from tdm.v_transaction_item_raw')
    print('Get DAILY transaction feed from tdm.v_transaction_head_raw')
    print('-'*30)
    print('PARAMETER LOAD')
    print('-'*30)
    print(f'Data end date : {end_date_id.strftime("%Y-%m-%d")}')
    print(f'Period of weeks : {period_n_week} weeks')
    print(f'Data start date : {str_date_id.strftime("%Y-%m-%d")}')
    
    day_diff = end_date_id - str_date_id
    period_day = day_diff.days + 1  #include start & end date = difference + 1 days
    print(f'Total data period : {period_day} days')
    print(f'Data end week id : {end_week_id}')
    print(f'Data start week id : {start_week_id}')
    print('-'*30)
    
    #---- Item 
    item:SparkDataFrame = \
        (spark.table('tdm.v_transaction_item_raw')
         .filter(F.col('country')=='th')
         .filter(F.col('source')=='resa')
         .filter(F.col('dp_data_dt').between(str_dp_data_dt, end_dp_data_dt))
         
         .filter(F.col('net_spend_amt')>0)
         .filter(F.col('product_qty')>0)
         
         .withColumnRenamed('counted_qty', 'count_qty')
         .withColumnRenamed('measured_qty', 'measure_qty')
         
         .withColumn('pkg_weight_unit', F.when(F.col('count_qty').isNotNull(), F.col('product_qty')).otherwise(F.col('measure_qty')))
        )
         
    #--- if use business_date as date scope
    if use_business_date:
        item:SparkDataFrame = \
            (item
             .withColumn('business_date', F.coalesce(F.to_date('business_date'), F.col('date_id')))
             .filter(F.col('business_date').between(str_date_id, end_date_id))
            )
        # If use business date, add businee_date in output columns
        ITEM_COL_SELECT.append('business_date')
    
    #---- if not use business date    
    else:
        item:SparkDataFrame = \
            (item
             .filter(F.col('date_id').between(str_date_id, end_date_id))
            )
    #---- Basket
    # Add store_id and date_id for new joining logic - Dec 2022 - Ta
    bask:SparkDataFrame = (
        spark.table('tdm.v_transaction_head_raw')
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='resa')
        .filter(F.col('dp_data_dt').between(str_dp_data_dt, end_dp_data_dt))
        .filter(F.col('date_id').between(str_date_id, end_date_id))
        
        .filter(F.col('net_spend_amt')>0)
        .filter(F.col('total_qty')>0)
        
#         .withColumn('offline_online', F.when(F.col('channel')=='OFFLINE', 'OFFLINE').otherwise('ONLINE'))
        .select('transaction_uid', 'channel', 'store_id', 'date_id')
        .drop_duplicates()
    )
    
    #---- Store
    store:SparkDataFrame = (
        spark.table(TBL_STORE)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('format_id').isin(store_format))
        
        .filter(~F.col('store_id').like('8%'))
        
        .withColumn('store_format_group', F.when(F.col('format_id').isin([1,2,3]),'HDE')
                                             .when(F.col('format_id')==4,'Talad')
                                             .when(F.col('format_id')==5,'GoFresh')
                                             .when(F.col('format_id')==6,'B2B')
                                             .when(F.col('format_id')==7,'Cafe')
                                             .when(F.col('format_id')==8,'Wholesale')

                   )
        .withColumnRenamed('region', 'store_region')
        .select('store_id', 'store_format_group', 'store_region')
        .drop_duplicates()
    )
        
    #---- Product
    prod:SparkDataFrame = (
        spark.table(TBL_PROD)
        .filter(F.col('country')=='th')
        .filter(F.col('source')=='rms')
        .filter(F.col('division_id').isin(division))
#         .select('upc_id', 'division_name', 'department_name', 'section_name', 
#         'class_name', 'subclass_name', 'product_desc', 'product_en_desc', 'brand_name', 'mfr_id')
        .drop_duplicates()
    )
    filter_division_prod_id = prod.select('upc_id').drop_duplicates() 
    
    #---- Manufacturer
    mfr:SparkDataFrame = (
        spark.table(TBL_MANUF)
        .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
        .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name-6)'))
        .select('mfr_id', 'mfr_only_name')
        .withColumnRenamed('mfr_only_name', 'manuf_name')
        .drop_duplicates()
    )
    #---- If need manufacturer name, add output columns, add manuf name in prod sparkDataFrame
    if manuf_name:
        with_manuf_col_list = PROD_COL_SELECT + ['mfr_id', 'manuf_name']
        PROD_COL_SELECT = list(set(with_manuf_col_list)) # Dedup add columns
        prod = prod.join(mfr, 'mfr_id', 'left')
        
    #---- Result spark dataframe
    # New joining logic - Dec 2022 - Ta
    sf = \
    (item
     .select(ITEM_COL_SELECT)
     .join(F.broadcast(filter_division_prod_id), 'upc_id', 'inner')
     .join(F.broadcast(store), 'store_id')
     .join(bask, on=['transaction_uid', 'store_id', 'date_id'], how='left')
     .join(prod.select(PROD_COL_SELECT), 'upc_id')
    )
    
    #---- mapping offline-online-other
    sf = _map_format_channel(sf)
    
    #---- Mapping household_id
    party_mapping = \
    (spark.table(TBL_CUST)
     .select('customer_id', 'household_id')
     .drop_duplicates()
    )
    
    #---- Filter out only Clubcard data
    if customer_data == 'CC':
        sf = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        
    else:
        cc = sf.filter(F.col('customer_id').isNotNull()).join(F.broadcast(party_mapping), 'customer_id')
        non_cc = sf.filter(F.col('customer_id').isNull())
        
        sf = cc.unionByName(non_cc, allowMissingColumns=True)
    
    #---- print result column
    txn_cols = sf.columns
    print(f'Output columns : {txn_cols}')
    print('-'*30)
    
    # Dec 2022 - Add transaction_uid made of concatenated data from transaction_uid, date_id and store_id. Keep original transaction_uid as separate column
    sf = sf.withColumn('transaction_uid_orig', F.col('transaction_uid')) \
           .withColumn('transaction_uid', F.concat_ws('_', F.col('transaction_uid'), F.col('date_id'), F.col('store_id')))
    
    #---- Opmized partition size in memory, for 128MB per partition, 1 wk data = 1722mb / 128 ~ 15 partition per week
    NUM_PARTITION = period_n_week * 15
    sf = sf.coalesce(NUM_PARTITION)
    
    return sf

# COMMAND ----------

# MAGIC %md ##Other Function

# COMMAND ----------

def sparkDataFrame_to_csv_filestore(sf: SparkDataFrame, csv_folder_name: str, prefix: str) -> bool:
    """Save spark DataFrame in DBFS Filestores path, under specifided as single CSV file
    and return HTML for download file to local.
    
    Parameter
    ---------
    sf: pyspark.sql.DataFrame
        spark DataFrame to save
        
    csv_folder_name: str
        name of 'csv folder', result 1 single file will be under those filder
        
    prefix: str , 
        Spark API full path under FileStore, for deep hierarchy, use without '/' at back
        ex. 'dbfs:/FileStore/thanakrit/trader' , 'dbfs:/FileStore/thanakrit/trader/cutoff'
        
    Return
    ------
    :bool
        If success True, unless False
    """
    import os
    from pathlib import Path
    
    # auto convert all prefix to Spark API
    spark_prefix = os.path.join('dbfs:', prefix[6:])
    spark_file_path = os.path.join(spark_prefix, csv_folder_name)
    
    # save csv
    (sf
     .coalesce(1)
     .write
     .format("com.databricks.spark.csv")
     .option("header", "true")
     .save(spark_file_path)
    )
    
    # get first file name endswith .csv, under in result path
    files = dbutils.fs.ls(spark_file_path)
    csv_file_key = [file.path for file in files if file.path.endswith(".csv")][0]
    csv_file_name = csv_file_key.split('/')[-1]
    
    # rename spark auto created csv file to target csv filename
    dbutils.fs.mv(csv_file_key, os.path.join(spark_file_path, csv_folder_name))
    
    # HTML direct download link
    filestore_path_to_url = str(Path(*Path(spark_file_path).parts[2:]))
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url, csv_folder_name + '?o=2606104010931262')
    print(f'HTML download link :\n')
    
    return html_url

# COMMAND ----------

def to_pandas(sf: SparkDataFrame) -> PandasDataFrame:
    """Solve DecimalType conversion to PandasDataFrame as string
    with pre-covert DecimalType to DoubleType then toPandas()
    automatically convert to float64
    
    Parameter
    ---------
    sf: pyspark.sql.DataFrame
        spark DataFrame to save
        
    Return
    ------
    :pandas.DataFrame
    """
    from pyspark.sql import types as T
    from pyspark.sql import functions as F
    
    col_name_type = sf.dtypes
    select_cast_exp = [F.col(c[0]).cast(T.DoubleType()) if c[1].startswith('decimal') else F.col(c[0]) for c in col_name_type]
    conv_sf = sf.select(*select_cast_exp)
    
    return conv_sf.toPandas()

# COMMAND ----------

def pandas_to_csv_filestore(df: PandasDataFrame, csv_file_name: str, prefix: str) -> str:
    """Save PandasDataFrame in DBFS Filestores path as csv, under specifided path
    and return HTML for download file to local.
    
    Parameter
    ---------
    df: pandas.DataFrame
         pandas DataFrame to save
        
    csv_folder_name: str
        name of 'csv folder', result 1 single file will be under those filder
        
    prefix: str ,
        Full file path FileStore, for deep hierarchy, use without '/' at back
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    Return
    ------
    : str
        If success return htlm download location, unless None
    """
    import os
    from pathlib import Path
    
    # auto convert all prefix to File API
    prefix = os.path.join('/dbfs', prefix[6:])
    
    # if not existed prefix, create prefix
    try:
        os.listdir(prefix)
    except:
        os.makedirs(prefix)
    
    # Write PandasDataFrame as CSV to folder
    filestore_path = os.path.join(prefix, csv_file_name)

    try:
        df.to_csv(filestore_path, index=False)
        
        #---- HTML direct download link
        filestore_path_to_url = str(Path(*Path(filestore_path).parts[3:]))
        html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url + '?o=2606104010931262')
        print(f'HTML download link :\n')
        return html_url
    
    except:
        return None

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_helper import pandas_to_csv_filestore

# COMMAND ----------

def get_vector_numpy(sf: SparkDataFrame, vec_col:str) -> numpyNDArray:
    """
    Collect & convert mllip.Vector to numpy NDArray in local driver
    
    Parameter
    ---------
    sf: SparkDataFrame
        source data
    vec_col: str
        name of column with data type as spark vector
    
    Return
    ------
    numpyNDArray
        retun to local driver
    
    """
    from pyspark.ml.functions import vector_to_array
    
    # Size of vector
    v_size = sf[[vec_col]].take(1)[0][0].toArray().shape[0]
    
    # exported vector name
    vec_export_name = vec_col + '_'
    out_np = \
    (sf
     .withColumn(vec_export_name, vector_to_array(vec_col))
     .select([F.col(vec_export_name)[i] for i in range(v_size)])
    ).toPandas().to_numpy()
    
    return out_np

# COMMAND ----------

def bucket_bin(sf: SparkDataFrame, splits: List, split_col: str) -> SparkDataFrame:
        """
        Bin data with definded splits, if data contain NULL will skip from bining
        Result bin description in column name 'bin'
        
        Parameter
        ---------
        sf: SparkDataFrame
            source data for binning
            
        splits: List
            list for use as splits point
        
        split_col: str
            name of column to be used as binning
            
        Return
        ------
        SparkDataFrame
            
        """
        from pyspark.ml.feature import Bucketizer
        
        import pandas as pd
        from copy import deepcopy
        
        #---- Create bin desc
        up_bound = deepcopy(splits)
        up_bound.pop(0)
        low_bound = deepcopy(splits)
        low_bound.pop(-1)
        
        #---- Creae SparkDataFram for mapping split_id -> bin
        bin_desc_list = [[i, '['+str(l)+' - '+str(w)+')'] for i,(l,w) in enumerate(zip(low_bound, up_bound))]
        
        bin_desc_df = pd.DataFrame(data=bin_desc_list, columns=['bin_id', 'bin'])
        bin_desc_sf = spark.createDataFrame(bin_desc_df)
        
        # here I created a dictionary with {index: name of split}
        # splits_dict = {i:splits[i] for i in range(len(splits))}

        #---- create bucketizer
        bucketizer = Bucketizer(splits=splits, inputCol=split_col, outputCol="bin_id")

        #---- bucketed dataframe
        bucketed = bucketizer.setHandleInvalid('skip').transform(sf)
        
        #---- map bin id to bin desc
        bucketed_desc = bucketed.join(F.broadcast(bin_desc_sf), 'bin_id') #.drop('bin_id')
                
        return bucketed_desc

# COMMAND ----------

def create_zip_from_dbsf_prefix(dbfs_prefix_to_zip: str, output_zipfile_name: str):
    """
    Create zip from all file in dbfs prefix and show download link
    The zip file will be written to driver then copy back to dbsf for download url generation
    Also the output zip file will be in the parent folder of source zip file

    Parameter
    ---------
    dbfs_prefix_to_zip: str
        path under DBFS FileStore, for deep hierarchy, use **without** '/' in front & back
        use full file API path style , always start with '/dbfs/FileStore'
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    output_zipfile_name: str
        name of zip file with suffix '.zip' ex. 'output.zip'
        
    """
    import os
    import zipfile
    import shutil
    from pathlib import Path

    #----- Mount a container of Azure Blob Storage to dbfs
    # storage_account_name='<your storage account name>'
    # storage_account_access_key='<your storage account key>'
    # container_name = '<your container name>'

    # dbutils.fs.mount(
    #   source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
    #   mount_point = "/mnt/<a mount directory name under /mnt, such as `test`>",
    #   extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":storage_account_access_key})

    #---- List all files which need to be compressed, from dbfs
    source_dbfs_prefix  = dbfs_prefix_to_zip
    source_filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=source_dbfs_prefix , topdown=False) for name in files]
    print(source_filenames)
    
    #---- Convert to Path object
#     source_dbfs_prefix = Path(os.path.join('/dbfs', 'FileStore', dbfs_prefix_to_zip))

#     with ZipFile(filename, "w", ZIP_DEFLATED) as zip_file:
#         for entry in source_dbfs_prefix.rglob("*"):
#             zip_file.write(entry, entry.relative_to(dir))
    
    #---- Directly zip files to driver path '/databricks/driver'
    #---- For Azure Blob Storage use blob path on the mount point, such as '/dbfs/mnt/test/demo.zip'
    # target_zipfile = 'demo.zip'
    
    with zipfile.ZipFile(output_zipfile_name, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        for filename in source_filenames:
            zip_f.write(filename)
    
    #---- Copy zip file from driver path back to dbfs path
    
    source_driver_zipfile = os.path.join('/databricks', 'driver', output_zipfile_name)
    root_dbfs_prefix_to_zip = str(Path(dbfs_prefix_to_zip).parent)
    target_dbfs_zipfile = os.path.join(root_dbfs_prefix_to_zip, output_zipfile_name)
    shutil.copyfile(source_driver_zipfile, target_dbfs_zipfile)
    
    #---- HTML direct download link
    root_dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(root_dbfs_prefix_to_zip).parts[3:]))
    print(root_dbfs_prefix_to_zip)
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', root_dbfs_prefix_to_zip_remove_first_2_parts, output_zipfile_name + '?o=2606104010931262')
    print(f'HTML download link :\n{html_url}')
    
    return html_url

# COMMAND ----------

def create_zip_from_dbsf_prefix_indir(dbfs_prefix_to_zip: str, output_zipfile_name: str):
    """
    Create zip from all file in dbfs prefix and show download link
    The zip file will be written to driver then copy back to dbsf for download url generation
    Also the output zip file will be in the parent folder of source zip file

    Parameter
    ---------
    dbfs_prefix_to_zip: str
        path under DBFS FileStore, for deep hierarchy, use **without** '/' in front & back
        use full file API path style , always start with '/dbfs/FileStore'
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    output_zipfile_name: str
        name of zip file with suffix '.zip' ex. 'output.zip'
        
    """
    import os
    import zipfile
    import shutil
    from pathlib import Path

    #----- Mount a container of Azure Blob Storage to dbfs
    # storage_account_name='<your storage account name>'
    # storage_account_access_key='<your storage account key>'
    # container_name = '<your container name>'

    # dbutils.fs.mount(
    #   source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
    #   mount_point = "/mnt/<a mount directory name under /mnt, such as `test`>",
    #   extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":storage_account_access_key})

    #---- List all files which need to be compressed, from dbfs
    source_dbfs_prefix  = dbfs_prefix_to_zip
    source_filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=source_dbfs_prefix , topdown=False) for name in files]
    print(source_filenames)
    
    #---- Convert to Path object
#     source_dbfs_prefix = Path(os.path.join('/dbfs', 'FileStore', dbfs_prefix_to_zip))

#     with ZipFile(filename, "w", ZIP_DEFLATED) as zip_file:
#         for entry in source_dbfs_prefix.rglob("*"):
#             zip_file.write(entry, entry.relative_to(dir))
    
    #---- Directly zip files to driver path '/databricks/driver'
    #---- For Azure Blob Storage use blob path on the mount point, such as '/dbfs/mnt/test/demo.zip'
    # target_zipfile = 'demo.zip'
    
    with zipfile.ZipFile(output_zipfile_name, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        for filename in source_filenames:
            zip_f.write(filename)
    
    #---- Copy zip file from driver path back to dbfs path
    
    source_driver_zipfile = os.path.join('/databricks', 'driver', output_zipfile_name)
    #root_dbfs_prefix_to_zip = str(Path(dbfs_prefix_to_zip).parent)
    #target_dbfs_zipfile = os.path.join(root_dbfs_prefix_to_zip, output_zipfile_name)
    target_dbfs_zipfile = os.path.join(dbfs_prefix_to_zip, output_zipfile_name)
    
    print('Check path')
    print('-'*80)
    print('source_driver_zipfile : ' + str( source_driver_zipfile))
    print('target_dbfs_zipfile : ' + str( target_dbfs_zipfile))
    
    
    shutil.copyfile(source_driver_zipfile, target_dbfs_zipfile)
    
    #---- HTML direct download link
    #root_dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(root_dbfs_prefix_to_zip).parts[3:]))
    
    dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(dbfs_prefix_to_zip).parts[3:]))
   
    print('Target path with out pre-fix')
    print(dbfs_prefix_to_zip_remove_first_2_parts)
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', dbfs_prefix_to_zip_remove_first_2_parts, output_zipfile_name + '?o=2606104010931262')
    print(f'HTML download link :\n{html_url}')
    
    return html_url
## end def

# COMMAND ----------

def create_zip_blob_prefix_indir(blob_prefix_to_zip: str, output_zipfile_name: str):
    """
    Create zip from all file in dbfs prefix and show download link
    The zip file will be written to driver then copy back to dbsf for download url generation
    Also the output zip file will be in the parent folder of source zip file

    Parameter
    ---------
    dbfs_prefix_to_zip: str
        path under DBFS FileStore, for deep hierarchy, use **without** '/' in front & back
        use full file API path style , always start with '/dbfs/FileStore'
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    output_zipfile_name: str
        name of zip file with suffix '.zip' ex. 'output.zip'
        
    """
    import os
    import zipfile
    import shutil
    from pathlib import Path

    #----- Mount a container of Azure Blob Storage to dbfs
    # storage_account_name='<your storage account name>'
    # storage_account_access_key='<your storage account key>'
    # container_name = '<your container name>'

    # dbutils.fs.mount(
    #   source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
    #   mount_point = "/mnt/<a mount directory name under /mnt, such as `test`>",
    #   extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":storage_account_access_key})

    #---- List all files which need to be compressed, from dbfs
    source_dbfs_prefix  = dbfs_prefix_to_zip
    source_filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=source_dbfs_prefix , topdown=False) for name in files]
    print(source_filenames)
    
    #---- Convert to Path object
#     source_dbfs_prefix = Path(os.path.join('/dbfs', 'FileStore', dbfs_prefix_to_zip))

#     with ZipFile(filename, "w", ZIP_DEFLATED) as zip_file:
#         for entry in source_dbfs_prefix.rglob("*"):
#             zip_file.write(entry, entry.relative_to(dir))
    
    #---- Directly zip files to driver path '/databricks/driver'
    #---- For Azure Blob Storage use blob path on the mount point, such as '/dbfs/mnt/test/demo.zip'
    # target_zipfile = 'demo.zip'
    
    with zipfile.ZipFile(output_zipfile_name, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        for filename in source_filenames:
            zip_f.write(filename)
    
    #---- Copy zip file from driver path back to dbfs path
    
    source_driver_zipfile = os.path.join('/databricks', 'driver', output_zipfile_name)
    #root_dbfs_prefix_to_zip = str(Path(dbfs_prefix_to_zip).parent)
    #target_dbfs_zipfile = os.path.join(root_dbfs_prefix_to_zip, output_zipfile_name)
    target_dbfs_zipfile = os.path.join(dbfs_prefix_to_zip, output_zipfile_name)
    
    print('Check path')
    print('-'*80)
    print('source_driver_zipfile : ' + str( source_driver_zipfile))
    print('target_dbfs_zipfile : ' + str( target_dbfs_zipfile))
    
    
    shutil.copyfile(source_driver_zipfile, target_dbfs_zipfile)
    
    #---- HTML direct download link
    #root_dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(root_dbfs_prefix_to_zip).parts[3:]))
    
    dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(dbfs_prefix_to_zip).parts[3:]))
   
    print('Target path with out pre-fix')
    print(dbfs_prefix_to_zip_remove_first_2_parts)
    #html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', dbfs_prefix_to_zip_remove_first_2_parts, output_zipfile_name + '?o=2606104010931262')
    #print(f'HTML download link :\n{html_url}')
    
    return html_url
## end def

# COMMAND ----------

def notify_gchat(msg):
    """ Notify job status to Google Chat, pre-defined chat space 'Databricks' 
    Parameter
    ---------
    msg: str
        message to sent in G chat
    """
    from json import dumps
    from httplib2 import Http
    
    url = 'https://chat.googleapis.com/v1/spaces/AAAAW6qavWw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=3ObbM8j9XztSrMjf-BxJ84GebTP-eoASw_xalgi6GbY%3D'
    bot_message = {
        'text' : msg}

    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )
    # print(response)
    
    return None

# COMMAND ----------

# MAGIC %md ## date, week function

# COMMAND ----------

def first_date_of_wk (in_dt):
    """
    Description: To get the first date of week from input date
	Input parameter: in_dt as string in format yyyy-mm-dd
	ReTurn: First Date of week of the provided date
	
    """
    ## convert input string to date    
    in_date   = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get week day number
    in_wk_day = in_date.strftime('%w')
    
    ## Get diff from current date to first day of week.
    ## Sunday is "0" reset value to 7 (last day of week for Lotus)    
    if int(in_wk_day) == 0:
        diff_day = (int(in_wk_day) + 7)-1
    else:
        diff_day = int(in_wk_day) - 1
    ## end if
    
    fst_dt_wk = in_date - timedelta(days = diff_day)
    
    return fst_dt_wk
## End def


# COMMAND ----------

def wk_of_year_ls (in_dt):
    """
    Description: Get week of specific year (Lotus Week) from specific date
    Input Parameter :
        in_dt: string , string of date in format yyyy-mm-dd --> '%Y-%m-%d'
    Prerequsit function: first_date_of_wk(in_dt)
    """
    in_date = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get year, weekday number
    in_yr   = int(in_date.strftime('%y'))  ## return last 2 digit of year ex: 22
    in_year = int(in_date.strftime('%Y'))
    
    ## create next year value    
    in_nxyr   = int(in_yr) + 1
    in_nxyear = int(in_year) + 1
    
    ## Step 0: Prepre first date of this year and nextyear
    ## For Lotus first week of year is week with 30th Dec except year 2024, first date of year = 2024-01-01 ** special request from Lotuss
    
    if in_year == 2023 :
        
        inyr_1date = first_date_of_wk(str(in_year -1) + '-' + '12-30')
        nxyr_1date = first_date_of_wk(str(in_nxyear) + '-' + '01-01')
    elif in_year == 2024:
        inyr_1date = first_date_of_wk(str(in_year) + '-' + '01-01')
        nxyr_1date = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    else:
        inyr_1date = first_date_of_wk(str(in_year -1) + '-' + '12-30')
        nxyr_1date = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    
    ## end if
    
    ## Step 1 : Check which year to get first date of year
    ## Step 2 : Get first date of year and diff between current date  THen get week
    
    if in_date < nxyr_1date:
        fdate_yr = inyr_1date
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date        
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk    = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        fiswk = (in_year * 100)+ wk
        
    else:
        fdate_yr = nxyr_1date
        ## return first week of year
        fiswk    = (int(in_nxyear) * 100) + 1 
    ## end if
    
    return fiswk

## end def

# COMMAND ----------

def f_date_of_wk(in_wk):
    """
    input parameter = in_wk is week_id eg: 202203
    prerequsit Function :
    first_date_of_wk(in_date)
    """
    # Step : get year, week of year using mod
    wk = in_wk % 100
    
    ## 4 digit year
    in_yr     = (in_wk - wk)/100
    
    diff_days = (wk-1) * 7  ## diff date 
    
    ## step2 get date from first date of year (in first week)
    
    if in_yr == 2024:
        fdate_yr = first_date_of_wk(str(int(in_yr)) + '-01-01')
    else :
        fdate_yr = first_date_of_wk(str(int(in_yr)-1) + '-12-31')
    ## end if
    
    dt_wk    = fdate_yr + timedelta(days=diff_days)
    
    return dt_wk

## End def

# COMMAND ----------

def week_cal (in_wk, n) :

    """
    Description : Get week apart from "in_wk" by "n" (weeks)
    Input parameter : 2
    in_wk = week number format yyyywk
    n     = number of weeks apart from "in_wk' - exclusive in_wk (if minus then week before)
    Prerequsit function:
    first_date_of_wk, wkOfYearLs, fDateofWk
    
    """
    
    in_dt   = f_date_of_wk(in_wk)
    diff_dy = n * 7
    
    out_dt  = in_dt + timedelta(days=diff_dy)
    out_wk = wk_of_year_ls(out_dt.strftime('%Y-%m-%d'))
    
    return out_wk

## End def

# COMMAND ----------

def first_date_of_promo_wk (in_dt):
    """
    Description: To get the first date of promo week from input date
    Input parameter: in_dt as string in format yyyy-mm-dd
    ReTurn: First Date of week of the provided date
    
    """
    ## convert input string to date    
    in_date   = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get week day number
    in_wk_day = in_date.strftime('%w')
    
    ## Get diff from current date to first day of week.
    ## Sunday is "0" reset value to 4 (promo week start from Thursday = 4)    
    if int(in_wk_day) == 4:
        diff_day = 0
    else:
        diff_day = ((int(in_wk_day) + 4) -1 ) % 7  ## mod
    ## end if
    
    fst_dt_wk = in_date - timedelta(days = diff_day)
    
    return fst_dt_wk

# COMMAND ----------

def wk_of_year_promo_ls (in_dt):
    """
    Description: Get week of specific year (Lotus Week) from specific date
    Input Parameter :
        in_dt: string , string of date in format yyyy-mm-dd --> '%Y-%m-%d'
    Prerequsit function: first_date_of_wk, first_date_of_promo_wk(in_dt)
    """
    in_date = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get year, weekday number
    in_yr   = int(in_date.strftime('%y'))  ## return last 2 digit of year ex: 22
    in_year = int(in_date.strftime('%Y'))
    
    ## create next year value    
    in_nxyr   = int(in_yr) + 1
    in_nxyear = int(in_year) + 1
    
    ## Step 0: Prepre first date of this year and nextyear
    ## For Lotus first week of year is week with 30th Dec 
    ## First date promo week - first Thursday of normal fis week.
    
    bkyr_1date  = first_date_of_wk(str(in_year -2) + '-' + '12-30')
    bkyr_1promo = bkyr_1date + timedelta(days = 3)  ## + 3 days from Monday = Thursday
        
    inyr_1date  = first_date_of_wk(str(in_year -1) + '-' + '12-30')
    inyr_1promo = inyr_1date + timedelta(days = 3)  ## + 3 days from Monday = Thursday
    
    nxyr_1date  = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    nxyr_1promo = nxyr_1date + timedelta(days = 3)
        
    ## Step 1 : Check which year to get first date of year
    ## Step 2 : Get first date of year and diff between current date  THen get week
    
    if in_date < inyr_1promo:  ## use back year week
    
        fdate_yr = bkyr_1promo
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date        
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk    = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        fiswk = ( (in_year-1) * 100)+ wk
        
    elif in_date < nxyr_1promo :
    
        fdate_yr = inyr_1promo
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk       = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        promo_wk = (in_year * 100)+ wk
        
    else:   ## in date is in next year promo_week, set as first week
        fdate_yr = nxyr_1promo
        ## return first week of year
        promo_wk    = (int(in_nxyear) * 100) + 1 
    ## end if
    
    return promo_wk

## end def

# COMMAND ----------

def f_date_of_promo_wk(in_promo_wk):
    """
    input parameter = in_wk is week_id eg: 202203
    prerequsit Function :
    first_date_of_wk, first_date_of_promo_wk(in_date)
    return Date value
    """
    # Step : get year, week of year using mod
    wk = in_promo_wk % 100
    
    ## 4 digit year
    in_yr     = (in_promo_wk - wk)/100
    
    diff_days = (wk-1) * 7  ## diff date 
    
    ## step2 get date from first date of year (in first week)
    
    fdate_year = first_date_of_wk(str(int(in_yr)-1) + '-12-31')
    
    fdate_promo = fdate_year + timedelta(days=3)  ## Plus 3 to be Thursday
    
    dt_wk    = fdate_promo + timedelta(days=diff_days)
    
    return dt_wk

## End def

# COMMAND ----------

def promo_week_cal (in_promo_wk, n) :

    """
    Description : Get week apart from "in_wk" by "n" (weeks)
    Input parameter : 2
    in_wk = week number format yyyywk
    n     = number of weeks apart from "in_wk' - exclusive in_wk (if minus then week before)
    Prerequsit function:
    first_date_of_wk, wk_of_year_promo_ls, f_date_of_promo_wk
    
    """
    
    in_dt   = f_date_of_promo_wk(in_promo_wk)
    
    diff_dy = n * 7
    
    out_dt  = in_dt + timedelta(days=diff_dy)
    out_wk  = wk_of_year_promo_ls(out_dt.strftime('%Y-%m-%d'))
    
    return out_wk

## End def

# COMMAND ----------



# COMMAND ----------

# wk = promo_week_cal(202002, -2)
# print(wk)

# COMMAND ----------

# MAGIC %md #Text, string Function

# COMMAND ----------

def list2string(inlist, delim = ' '):
    """ This function use to convert from list variable to concatenate string
    having 2 input parameters
    inlist : list , that need to conver to string
    delim  : text , is a charactor use to be delimeter between each value in list
    """
    
    outtxt = ''
    n = 0
    
    for itm in inlist:
        
        if n == 0:
            outtxt = outtxt + str(itm)
            n      = n + 1
        else:
            outtxt = outtxt + str(delim) + str(itm)
            n      = n + 1
        ## end if
    ## end for
    
    return outtxt

## end def

# COMMAND ----------

# tlist = ['aa', 'bb']

# test  = list2string(tlist, ' , ')
# print(test)

# COMMAND ----------

def div_zero_chk(x, y):
    if (y == 0) | (x==0) | (x is None) | (y is None):
        return 0
    else:
        return x/y
## end def

# COMMAND ----------

# MAGIC %md # List directory recursive

# COMMAND ----------

## Copy from : https://stackoverflow.com/questions/63955823/list-the-files-of-a-directory-and-subdirectory-recursively-in-databricksdbfs

def get_dir_content(ls_path):

    dir_paths = dbutils.fs.ls(ls_path)
    
    subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
    
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
    
    
    return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

## end def 

# COMMAND ----------


