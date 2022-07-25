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

# MAGIC %md ##Calculate total number of week of data to retrived

# COMMAND ----------

def get_total_data_num_week(c_st_date_txt: str,  c_en_date_txt: str, gap_week_txt: str):
    """Get number of weeks to snap data
    |-----|---|---|------|
    |prior|pre|gap|during|
    |-----|---|---|------|
    | 13  |13 | ? |   ?  |
    |-----|---|---|------|
    |  Y  | Y | Y |   Y  |
    |-----|---|---|------|
                         ^-- campaing end date
    """
    import math
    from datetime import date, datetime, timedelta
    
    c_st_date = datetime.strptime(c_st_date_txt, '%Y-%m-%d')
    c_en_date = datetime.strptime(c_en_date_txt, '%Y-%m-%d')

    camp_days  = c_en_date - c_st_date
    camp_weeks = math.ceil(camp_days.days/7)
    
    #---- Find start-end gap date
    gap_week = int(gap_week_txt)
    if gap_week == 0:
        gap_en_date = c_st_date
        gap_st_date = c_st_date
    else:
        # Date of Gap start - end
        gap_en_date = c_st_date + pd.DateOffset(days=-1)
        gap_st_date = gap_en_date + pd.DateOffset(weeks=-gap_week, days=1)
    
    #---- Find start-end pre, prior
    pre_weeks  = 13
    prior_weeks = 13
    
    # Date of Pre (13 wk)
    pre_en_date = gap_st_date + pd.DateOffset(days=-1)
    pre_st_date = pre_en_date + pd.DateOffset(weeks= -pre_weeks, days=1)
    
    # Date of Prio (13 wk)
    prior_en_date = pre_st_date + pd.DateOffset(days=-1)
    prior_st_date = prior_en_date + pd.DateOffset(weeks= -prior_weeks, days=1)

    tot_data_week_num = prior_weeks + pre_weeks + gap_week + camp_weeks

    print('-'*80)
    print(f'Total weeks for data retrival : {tot_data_week_num}')    
    print(f'Campaign period {camp_weeks} weeks : {c_st_date:%Y-%m-%d} - {c_en_date:%Y-%m-%d}')
    print(f'Gap period {gap_week} weeks : {gap_st_date:%Y-%m-%d} - {gap_en_date:%Y-%m-%d}')
    print(f'Pre period {pre_weeks} weeks : {pre_st_date:%Y-%m-%d} - {pre_en_date:%Y-%m-%d}')
    print(f'Prior period {prior_weeks} weeks : {prior_st_date:%Y-%m-%d} - {prior_en_date:%Y-%m-%d}')
    print('-'*80)

    c_st_date_txt = c_st_date.strftime('%Y-%m-%d')
    c_en_date_txt = c_en_date.strftime('%Y-%m-%d')
    
    return tot_data_week_num, c_st_date, c_en_date, gap_st_date, gap_en_date, pre_st_date, pre_en_date, prior_st_date, prior_en_date

# COMMAND ----------

# MAGIC %md ##Check & combine test store region

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
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .drop_duplicates()
        )
        txn = txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left')
        return txn
    return wrapper

# COMMAND ----------

from typing import List
from pyspark.sql import DataFrame as sparkDataFrame
from pandas import DataFrame as pandasDataFrame

def check_combine_region(store_format_group: str, test_store_sf: sparkDataFrame, txn: sparkDataFrame) -> (sparkDataFrame, sparkDataFrame):
    """Base on store group name, 
    - if HDE / Talad -> count check test vs total store
    - if GoFresh -> adjust 'store_region' in txn, count check
    """
    print('-'*80)
    print('Count check store region & Combine store region for GoFresh')
    print(f'Store format defined : {store_format_group}')
        
    if store_format_group == 'hde':
        format_id_list = [1,2,3]
        
        all_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id', 'store_name', F.col('region').alias('store_region')).drop_duplicates()
         .dropna('all', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'total_{store_format_group}'))
        )
        test_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .join(test_store_sf, 'store_id', 'left_semi')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'test_store_count'))
        )
    
    elif store_format_group == 'talad':
        format_id_list = [4]
        
        all_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .dropna('all', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'total_{store_format_group}'))
        )
        test_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .join(test_store_sf, 'store_id', 'left_semi')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'test_store_count'))
        )
        
    elif store_format_group == 'gofresh':
        format_id_list = [5]
        
        #---- Adjust Transaction
        print('Combine store_region West + Central in variable "txn_all"')
        adjusted_store_region =  \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .drop_duplicates()
        )
        txn = txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left')
        #---- Count Region
        all_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .dropna('all', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'total_{store_format_group}'))
        )
        
        test_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .join(test_store_sf, 'store_id', 'left_semi')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'test_store_count'))
        )

    else:
        print(f'Unknow format group name : {format_group_name}')

    # Combine count store
    test_vs_all_store_count = all_store_count_region.join(test_store_count_region, 'store_region', 'left').orderBy('store_region')
    #     test_vs_all_store_count.display()
    #     test_vs_all_store_count_df = to_pandas(test_va_all_store_count)
    #     pandas_to_csv_filestore(test_vs_all_store_count_df, f'test_vs_all_store_count.csv', prefix=os.path.join(dbfs_project_path, 'result'))

    return test_vs_all_store_count, txn

# COMMAND ----------

#@adjust_gofresh_region
#def 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Adjacency product

# COMMAND ----------

from typing import List
from pyspark.sql import DataFrame as sparkDataFrame
from pandas import DataFrame as pandasDataFrame

#----------
# Get list of adjacency ucp_id from feature upc_id 
#----------

def _get_from_feature_upc_id(promoted_upc_id, exposure_group_file):
    """From promoted product, find exposure group and exposure ucp id
    """
    from pyspark.sql import functions as F
    
    exposure_group = spark.read.csv(exposure_group_file, header="true", inferSchema="true")
    
    exposure_group_with_sec = \
    (exposure_group
     .select('group', 'section_name')
     .drop_duplicates()
     .groupBy('group')
     .agg(F.collect_set(F.col('section_name')).alias('set_section_name'))
     .withColumn('group_section_name', F.sort_array(F.col('set_section_name')))
     .select('group', 'group_section_name')
    )
    #---- To display features SKUs & related exposure group
    #---- Details of features product + exposure group
    promoted_product_exposure_detail = \
    (spark
     .table('tdm.v_prod_dim_c').alias('prod_dim')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(promoted_upc_id))
     .drop_duplicates()
     .join(exposure_group, ['section_id', 'class_id'])
     .select('group', 'prod_dim.division_name', 'prod_dim.department_name', 'prod_dim.section_name', 'prod_dim.class_name', 
             'prod_dim.subclass_name', 'prod_dim.brand_name', 'prod_dim.upc_id', 'prod_dim.mfr_id', 'prod_dim.product_en_desc')
     .drop_duplicates()
     .join(exposure_group_with_sec, 'group', 'left')
    )
    
    mfr = \
    (spark.table('tdm.v_mfr_dim')
     .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
     .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name-6)'))
     .select('mfr_id', 'mfr_only_name')
     .withColumnRenamed('mfr_only_name', 'manuf_name')
     .drop_duplicates()
    )
    
    promoted_product_exposure_detail = promoted_product_exposure_detail.join(mfr, 'mfr_id', 'left').drop('mfr_id')
    
    mfr_promoted_product = promoted_product_exposure_detail.select('manuf_name').collect()[0][0]
    
    promoted_product_exposure_detail.display()
    
    #---- To generate upc_id of exposure product & generate exposure group name
    #---- Find section_id, class_id of promoted products
    sec_id_class_id_promoted_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(promoted_upc_id))
     .select('section_id', 'section_name', 'class_id', 'class_name')
     .drop_duplicates()
    )
    
    #---- Create upc_id under exposure group
    #---- Find exposure group based on section_id, class_id of promoted products
    exposure_group_promoted_product = \
    (exposure_group
     .join(sec_id_class_id_promoted_product, ['section_id', 'class_id'])
     .select('group')
     .drop_duplicates()
    )
    
    print('Exposure group of promoted products :')
    exposure_group_promoted_product.show(truncate=False)
    
    #---- Create upc_id under exposure group
    sec_id_class_id_exposure_group = \
    (exposure_group
     .join(exposure_group_promoted_product, 'group')
     .select('section_id', 'class_id', 'group')
     .drop_duplicates()
    )
    
    prod_exposure_group = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .join(sec_id_class_id_exposure_group, ['section_id', 'class_id'])
     .select('upc_id', 'section_id', 'class_id', 'section_name', 'class_name', 'group')
     .drop_duplicates()
    )
    
    print('Section, Class and number of sku under exposure group :')
    (prod_exposure_group
     .groupBy('group', 'section_id', 'section_name', 'class_id', 'class_name')
     .agg(F.count('upc_id').alias('n_skus'))
     .orderBy('group', 'section_name', 'class_name')
    ).show(truncate=False)
    
    upc_id_exposure_group = \
    (prod_exposure_group
     .select('upc_id')
     .drop_duplicates()
    )

    return upc_id_exposure_group, exposure_group_promoted_product, promoted_product_exposure_detail, mfr_promoted_product

#----
# For Cross Category, Use Exposure group to generate
#----

def _get_group_product_id(cross_adjacency_group, exposure_group_file):
    """Idenfity exposure group from group name
    """
    from pyspark.sql import functions as F
    
    exposure_group = spark.read.csv(exposure_group_file, header="true", inferSchema="true")

    #---- To display features SKUs & related exposure group
    #---- Details of features product + exposure group
    promoted_product_exposure_detail = \
    (spark
     .table('tdm.v_prod_dim_c').alias('prod_dim')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(promoted_upc_id))
     .withColumn('group', F.lit(f'Cross Category - {cross_adjacency_group}'))
     .drop_duplicates()
     .select('group', 'prod_dim.division_name', 'prod_dim.department_name', 'prod_dim.section_name', 'prod_dim.class_name', 
             'prod_dim.subclass_name', 'prod_dim.upc_id', 'prod_dim.mfr_id', 'prod_dim.product_en_desc')
     .drop_duplicates()
    )
    
    mfr = \
    (spark.table('tdm.v_mfr_dim')
     .withColumn('len_mfr_name', F.length(F.col('mfr_name')))
     .withColumn('mfr_only_name', F.expr('substring(mfr_name, 1, len_mfr_name-6)'))
     .select('mfr_id', 'mfr_only_name')
     .withColumnRenamed('mfr_only_name', 'manuf_name')
     .drop_duplicates()
    )
    
    promoted_product_exposure_detail = promoted_product_exposure_detail.join(mfr, 'mfr_id', 'left').drop('mfr_id')
    
    mfr_promoted_product = promoted_product_exposure_detail.select('manuf_name').collect()[0][0]
    
    promoted_product_exposure_detail.display()
     
    #---- Create upc_id under exposure group
    sec_id_class_id_exposure_group = \
    (exposure_group
     .filter(F.col('group') == cross_adjacency_group)
     .select('section_id', 'class_id', 'group')
     .drop_duplicates()
    )
    
    prod_exposure_group = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .join(sec_id_class_id_exposure_group, ['section_id', 'class_id'])
     .select('upc_id', 'section_id', 'class_id', 'section_name', 'class_name', 'group')
     .drop_duplicates()
    )
    
    print('Section, Class and number of sku under exposure group :')
    (prod_exposure_group
     .groupBy('group', 'section_id', 'section_name', 'class_id', 'class_name')
     .agg(F.count('upc_id').alias('n_skus'))
     .orderBy('group', 'section_name', 'class_name')
    ).show(truncate=False)
    
    upc_id_exposure_group = \
    (prod_exposure_group
     .select('upc_id')
     .drop_duplicates()
    )
    
    return upc_id_exposure_group

#---- Main Function 

def get_adjacency_product_id(promoted_upc_id: List, adjacecy_file_path: str, 
                             sel_sec: str = None, sel_class: str = None, sel_subcl: str = None,
                             adjacency_type: str = 'FEATURE_UPC_ID', cross_adjacency_group: str = None) -> (sparkDataFrame, sparkDataFrame):
    """If adjacency type = 'PARENT_CATEGORY', will use select section, class, subclass to find adjacency product group and product id
    Otherwise will use cross_adjacency_group to find adjacency product id
    """
    print('='*80)
    print('Check Feature SKU Details -> Check Aisle definition -> Create adjacency product list')
    print(f'Adjacency type : {adjacency_type}')
    if adjacency_type == 'FEATURE_UPC_ID':
        out, exposure_gr, feature_prod_exposure, mfr_promoted_product = _get_from_feature_upc_id(promoted_upc_id, adjacecy_file_path)
        
    elif cross_adjacency_group == 'CROSS_CATEGORY':
        print(f'Cross category type to group : {cross_adjacency_group}')
        out = _get_group_product_id(cross_adjacency_group, adjacecy_file_path)
        exposure_gr = spark.createDataFrame([(cross_adjacency_group,)], ['group',])
        
    else:
        print(f'Could not find adjacency type {adjacency_type} (with {cross_adjacency_group}')
        
    return out, exposure_gr, feature_prod_exposure, mfr_promoted_product

# COMMAND ----------

# MAGIC %md ##Get Awareness

# COMMAND ----------

def get_awareness(txn, cp_start_date, cp_end_date, store_fmt, test_store_sf, adj_prod_sf,
                  media_spend):
    """For Awareness of HDE, Talad
    
    """
    #get only txn in exposure area in test store
    print('='*80)
    print('Exposure v.3 - add media mechanics multiplyer by store & period by store')
    print('Exposed customer (test store) from "OFFLINE" channel only')
    
    print('Check test store input column')
    test_store_sf.display()
    
    # Family size for HDE, Talad
    if store_fmt == 'hde':
        family_size = 2.2
    elif store_fmt == 'talad':
        family_size = 1.5
    else:
        family_size = 1.0
    print(f'For store format "{store_fmt}" family size : {family_size:.2f}')
    
    # Join txn with test store details & adjacency product
    # If not defined c_start, c_end will use cp_start_date, cp_end_date
    # Filter period in each store by period defined in test store 
    
    txn_exposed = \
    (txn
     .filter(F.col('channel')=='OFFLINE')
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .fillna('NA', subset='store_region')
    )
    
    #---- Overall Exposure
    by_store_impression = \
    (txn_exposed
     .groupBy('store_id', 'mech_count')
     .agg(
       F.countDistinct('transaction_uid').alias('epos_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNotNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('carded_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('non_carded_visits')
       )
     .withColumn('epos_impression', F.col('epos_visits')*family_size*F.col('mech_count')) 
     .withColumn('carded_impression', F.col('carded_visits')*family_size*F.col('mech_count')) 
     .withColumn('non_carded_impression', F.col('non_carded_visits')*family_size*F.col('mech_count'))
     )
    
    all_impression = \
    (by_store_impression
     .agg(F.sum('epos_visits').alias('epos_visits'),
          F.sum('carded_visits').alias('carded_visits'),
          F.sum('non_carded_visits').alias('non_carded_visits'),
          F.sum('epos_impression').alias('epos_impression'),
          F.sum('carded_impression').alias('carded_impression'),
          F.sum('non_carded_impression').alias('non_carded_impression')
         )
    )
    
    all_store_customer = txn_exposed.agg(F.countDistinct( F.col('household_id') ).alias('carded_customers') ).collect()[0][0]
    
    exposure_all = \
    (all_impression
     .withColumn('carded_reach', F.lit(all_store_customer))
     .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach') )
     .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
     .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
     .withColumn('media_spend', F.lit(media_spend))
     .withColumn('CPM', F.lit(media_spend) / ( F.col('epos_impression') / 1000 ) )
    )         
    
    #---- By Region Exposure
    
    by_store_region_impression = \
    (txn_exposed
     .groupBy('store_id', 'store_region', 'mech_count')
     .agg(
       F.countDistinct('transaction_uid').alias('epos_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNotNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('carded_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('non_carded_visits')
       )
     .withColumn('epos_impression', F.col('epos_visits')*family_size*F.col('mech_count')) 
     .withColumn('carded_impression', F.col('carded_visits')*family_size*F.col('mech_count')) 
     .withColumn('non_carded_impression', F.col('non_carded_visits')*family_size*F.col('mech_count'))
     )
    
    region_impression = \
    (by_store_region_impression
     .groupBy('store_region')
     .agg(F.sum('epos_visits').alias('epos_visits'),
          F.sum('carded_visits').alias('carded_visits'),
          F.sum('non_carded_visits').alias('non_carded_visits'),
          F.sum('epos_impression').alias('epos_impression'),
          F.sum('carded_impression').alias('carded_impression'),
          F.sum('non_carded_impression').alias('non_carded_impression')
         )
    )
    
    customer_by_region = txn_exposed.groupBy('store_region').agg(F.countDistinct( F.col('household_id') ).alias('carded_customers') )

    # Allocate media by region
    count_test_store_all =  test_store_sf.select('store_id').drop_duplicates().count()
    
    # combine region for gofresh
    if store_fmt == 'gofresh':
        print('Combine store_region West + Central for GoFresh Evaluation')
        count_test_store_region =  \
        (spark
         .table('tdm.v_store_dim')
         .join(test_store_sf.select('store_id').drop_duplicates() , 'store_id', 'inner')
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .fillna('NA', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias('num_test_store'))
        )
    else:    
        count_test_store_region = \
        (spark
         .table('tdm.v_store_dim')
         .select('store_id', F.col('region').alias('store_region'))
         .drop_duplicates()
         .join(test_store_sf, 'store_id', 'inner')
         .fillna('NA', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias('num_test_store'))
        )
    
    media_by_region = \
    (count_test_store_region
     .withColumn('num_all_test_stores', F.lit(count_test_store_all))
     .withColumn('all_media_spend', F.lit(media_spend))
     .withColumn('region_media_spend', F.col('all_media_spend')/F.col('num_all_test_stores')*F.col('num_test_store'))
    )
    
    exposure_region = \
    (region_impression
     .join(customer_by_region, 'store_region', 'left')
     .join(media_by_region, 'store_region', 'left')
     .withColumn('carded_reach', F.col('carded_customers'))
     .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach') )
     .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
     .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
     .withColumn('CPM', F.col('region_media_spend') / ( F.col('epos_impression') / 1000 ) )
    )
     
    return exposure_all, exposure_region

# COMMAND ----------

# MAGIC %md ##GoFresh : Awareness-PromoWeek

# COMMAND ----------

def get_awareness_promo_wk(txn, 
                          cp_start_date, 
                          cp_end_date, 
                          store_fmt, 
                          test_store_sf, 
                          media_spend):
    """Awareness for gofresh, use exposure at store level not need aisle definition & adjacency product
    
    """
    #get only txn in exposure area in test store
    print('='*80)
    print('Exposure GoFresh - Promo week - media mechanics multiplyer by store & period by store')
    print('Exposed customer (test store) from "OFFLINE" channel only')
    
    print('Check test store input column')
    test_store_sf.display()
    
    # Family size for HDE, Talad
    if store_fmt == 'hde':
        FAMILY_SIZE = 2.2
    elif store_fmt == 'talad':
        FAMILY_SIZE = 1.5
    else:
        FAMILY_SIZE = 1.0
    print(f'For store format "{store_fmt}" family size : {FAMILY_SIZE:.2f}')
    
    # Join txn with test store details 
    # If not defined c_start, c_end will use cp_start_date, cp_end_date
    # Filter period in each store by period defined in test store 
    
    txn_exposed = \
    (txn
     .filter(F.col('channel')=='OFFLINE')
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
   # .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .fillna('NA', subset='store_region')
    )
    
    #---- Overall Exposure
    by_store_impression = \
    (txn_exposed
     .groupBy('store_id', 'mech_count')
     .agg(
       F.countDistinct('transaction_uid').alias('epos_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNotNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('carded_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('non_carded_visits')
       )
     .withColumn('epos_impression', F.col('epos_visits')*FAMILY_SIZE*F.col('mech_count')) 
     .withColumn('carded_impression', F.col('carded_visits')*FAMILY_SIZE*F.col('mech_count')) 
     .withColumn('non_carded_impression', F.col('non_carded_visits')*FAMILY_SIZE*F.col('mech_count'))
     )
    
    all_impression = \
    (by_store_impression
     .agg(F.sum('epos_visits').alias('epos_visits'),
          F.sum('carded_visits').alias('carded_visits'),
          F.sum('non_carded_visits').alias('non_carded_visits'),
          F.sum('epos_impression').alias('epos_impression'),
          F.sum('carded_impression').alias('carded_impression'),
          F.sum('non_carded_impression').alias('non_carded_impression')
         )
    )
    
    all_store_customer = txn_exposed.agg(F.countDistinct( F.col('household_id') ).alias('carded_customers') ).collect()[0][0]
    
    exposure_all = \
    (all_impression
     .withColumn('carded_reach', F.lit(all_store_customer))
     .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach') )
     .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
     .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
     .withColumn('media_spend', F.lit(media_spend))
     .withColumn('CPM', F.lit(media_spend) / ( F.col('epos_impression') / 1000 ) )
    )         
    
    #---- By Region Exposure
    
    by_store_region_impression = \
    (txn_exposed
     .groupBy('store_id', 'store_region', 'mech_count')
     .agg(
       F.countDistinct('transaction_uid').alias('epos_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNotNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('carded_visits'),
       F.countDistinct( (F.when(F.col('customer_id').isNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('non_carded_visits')
       )
     .withColumn('epos_impression', F.col('epos_visits')*FAMILY_SIZE*F.col('mech_count')) 
     .withColumn('carded_impression', F.col('carded_visits')*FAMILY_SIZE*F.col('mech_count')) 
     .withColumn('non_carded_impression', F.col('non_carded_visits')*FAMILY_SIZE*F.col('mech_count'))
     )
    
    region_impression = \
    (by_store_region_impression
     .groupBy('store_region')
     .agg(F.sum('epos_visits').alias('epos_visits'),
          F.sum('carded_visits').alias('carded_visits'),
          F.sum('non_carded_visits').alias('non_carded_visits'),
          F.sum('epos_impression').alias('epos_impression'),
          F.sum('carded_impression').alias('carded_impression'),
          F.sum('non_carded_impression').alias('non_carded_impression')
         )
    )
    
    customer_by_region = txn_exposed.groupBy('store_region').agg(F.countDistinct( F.col('household_id') ).alias('carded_customers') )
    
    # Allocate media by region
    count_test_store_all =  test_store_sf.select('store_id').drop_duplicates().count()
    
    # combine region for gofresh
    if store_fmt == 'gofresh':
        print('Combine store_region West + Central for GoFresh Evaluation')
        count_test_store_region =  \
        (spark
         .table('tdm.v_store_dim')
         .join(test_store_sf.select('store_id').drop_duplicates() , 'store_id', 'inner')
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .fillna('NA', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias('num_test_store'))
        )
    else:    
        count_test_store_region = \
        (spark
         .table('tdm.v_store_dim')
         .select('store_id', F.col('region').alias('store_region'))
         .drop_duplicates()
         .join(test_store_sf, 'store_id', 'inner')
         .fillna('NA', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias('num_test_store'))
        )
    
    media_by_region = \
    (count_test_store_region
     .withColumn('num_all_test_stores', F.lit(count_test_store_all))
     .withColumn('all_media_spend', F.lit(media_spend))
     .withColumn('region_media_spend', F.col('all_media_spend')/F.col('num_all_test_stores')*F.col('num_test_store'))
    )
    
    exposure_region = \
    (region_impression
     .join(customer_by_region, 'store_region', 'left')
     .join(media_by_region, 'store_region', 'left')
     .withColumn('carded_reach', F.col('carded_customers'))
     .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach') )
     .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
     .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
     .withColumn('CPM', F.col('region_media_spend') / ( F.col('epos_impression') / 1000 ) )
    )
     
    return exposure_all, exposure_region

# COMMAND ----------

# MAGIC %md ##Customer movement & swithcing

# COMMAND ----------

def cust_movement(switching_lv, 
                  txn, 
                  cp_start_date, 
                  cp_end_date, 
                  brand_df,
                  test_store_sf, 
                  adj_prod_sf, 
                  feat_list):
    """Media evaluation solution, Customer movement and switching v2
    - Exposure based on each store media period
    
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    
    print('Customer movement for "OFFLINE" + "ONLINE"')
    
    #---- Get scope for brand in class / brand in subclass
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Get list of feature brand name
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
        
    #---- During camapign - exposed customer, 
    dur_campaign_exposed_cust = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media     
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_exposed_date'))
    )
    
    #---- During campaign - Exposed & Feature Brand buyer
    dur_campaign_brand_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .join(brand_df, 'upc_id')
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_brand_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_brand_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_brand_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_brand_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_brand_buy_date'))
     .select('household_id')
    )
    
    activated_brand = dur_campaign_exposed_cust_and_brand_shopper.count()
    print(f'Total exposed and brand shopper (Activated Brand) : {activated_brand}')    
    
    #---- During campaign - Exposed & Features SKU shopper
    dur_campaign_sku_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_sku_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_sku_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_sku_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
     .select('household_id')
    )
    
    activated_sku = dur_campaign_exposed_cust_and_sku_shopper.count()
    print(f'Total exposed and sku shopper (Activated SKU) : {activated_sku}')    
    
    activated_df = pd.DataFrame({'customer_exposed_brand_activated':[activated_brand], 'customer_exposed_sku_activated':[activated_sku]})
    
    #---- Find Customer movement from (PPP+PRE) -> CMP period
    
    # Existing and New SKU buyer (movement at micro level)
    prior_pre_sku_shopper = \
    (txn
     .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )
    
    existing_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
    )
    
    existing_exposed_cust_and_sku_shopper = existing_exposed_cust_and_sku_shopper.checkpoint()
    
    new_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
    )
    new_exposed_cust_and_sku_shopper = new_exposed_cust_and_sku_shopper.checkpoint()
        
    #---- Movement macro level for New to SKU
    prior_pre_cc_txn = \
    (txn
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .select('household_id')
    ).drop_duplicates()
    
    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
     .select('household_id')
    ).drop_duplicates()
        
    #---- Grouping, flag customer macro flag
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )
    
    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )
    
    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )
        
        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()
        
        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    elif switching_lv == 'class':
        
        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    else:
        print('Not recognized Movement and Switching level param')
        return None

# COMMAND ----------

def cust_switching(switching_lv, cust_movement_sf,
                   txn, cp_start_date, cp_end_date, 
#                    prior_start_date, prior_end_date, pre_start_date, pre_end_date,
                   feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame
    
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    print('Customer switching for "OFFLINE" + "ONLINE"')
    #---- switching fn from Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: sparkDataFrame, prod_trans: SparkDataFrame, grp: List, 
                   prod_lev: str, full_prod_lev: str , col_rename: str, period: str) -> SparkDataFrame:
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
#         print('cust_micro_df')
#         cust_micro_df.display()
        # Transaction of focus customer group
    # prod_trans = pri_pre_feature_subclass transaction
    # cust_micro_df = exposed & actiaved & new_to_brand
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )
#         print('cust_mirco_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        # total of other brand/subclass/class spending
#         total_oth = \
#         (cust_micro_kpi_prod_lv
#          .select(F.sum('oth_'+prod_lev+'_spend').alias('total_oth_'+prod_lev+'_spend'))
#          .toPandas().fillna(0)
#         )['total_oth_'+prod_lev+'_spend'][0]
        
        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]
        
        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))
        
#         print('cust_micro_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        
        print("\t\t\t\t\t\t**Running micro df2")

#         print('cust_micro_df2')
#         cust_micro_df2.display()

        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
#                       'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
#             micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='subclass_name', how='inner')
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )            
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif prod_lev == 'class':
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        
        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name','class_name','original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
         .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(), F.col('pct_spend_oth_'+full_prod_lev).desc())
        )
        # display(micro_df_summ)
        switching_result = switching_result.checkpoint()
        
        return switching_result
    
    #---- Main
    # Section id, class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Brand name of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement
        prior_pre_cc_txn_subcl = \
        (txn
         .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        prior_pre_cc_txn_subcl_sel_brand = prior_pre_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_subcl_sel_brand_kpi = \
        (prior_pre_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_subcl = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_fis_wk').isin(['cmp']))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        dur_cc_txn_subcl_sel_brand = dur_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        dur_subcl_sel_brand_kpi = \
        (dur_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name', 'household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_subcl_sel_brand_kpi
         .join(dur_subcl_sel_brand_kpi, 
               ['division_name','department_name','section_name', 'class_name',
#                 'subclass_name',
                'brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_subcl, 
#                                                  ['subclass_name', 'brand_name'], 
                                                 ["class_name", 'brand_name'], 
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        subcl_cust = dur_cc_txn_subcl.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_subcl
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(subcl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )
        
        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    
    elif switching_lv == 'class':
        #---- Create class-brand spend by cust movment
        prior_pre_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('period_fis_wk').isin(['pre', 'ppp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        prior_pre_cc_txn_class_sel_brand = prior_pre_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_class_sel_brand_kpi = \
        (prior_pre_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_fis_wk').isin(['cmp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        dur_cc_txn_class_sel_brand = dur_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        dur_class_sel_brand_kpi = \
        (dur_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_class_sel_brand_kpi
         .join(dur_class_sel_brand_kpi, ['division_name','department_name','section_name',
                  'class_name','brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_class,
                                                 ['class_name', 'brand_name'], 
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within class
        cl_cust = dur_cc_txn_class.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_class
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(cl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    else:
        
        print('Unrecognized switching level')
        return None

# COMMAND ----------

def cust_sku_switching(switching_lv,
                       txn, cp_start_date, cp_end_date, 
#                        prior_start_date, prior_end_date, 
#                        pre_start_date, pre_end_date,
                       test_store_sf, 
                       adj_prod_sf, 
                       feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame
    
    """Media evaluation solution, customer sku switching
    """
    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    
    prod_desc = spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()
    
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    print('Customer switching SKU for "OFFLINE" + "ONLINE"')
    
    #---- From Exposed customer, find Existing and New SKU buyer (movement at micro level)
    dur_campaign_exposed_cust = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media          
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_exposed_date'))
    )
    
    dur_campaign_sku_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_sku_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_sku_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_sku_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
     .select('household_id')
    )    
    #----
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_subcl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
          .filter(F.col('period_fis_wk').isin(['pre', 'ppp', 'cmp']))

#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])

         .withColumn('pre_subcl_sales', F.when( F.col('period_fis_wk').isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_subcl_sales', F.when( F.col('period_fis_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_subcl_sales', F.sum(F.col('pre_subcl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_subcl_sales', F.sum(F.col('dur_subcl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_subcl = txn_sel_subcl_cc.filter( (F.col('cust_tt_pre_subcl_sales')>0) & (F.col('cust_tt_dur_subcl_sales')>0) )
        
        txn_cust_both_period_subcl_not_pre_but_dur_sku = \
        (txn_cust_both_period_subcl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_fis_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_fis_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        
        n_cust_both_subcl_switch_sku = \
        (txn_cust_both_period_subcl_not_pre_but_dur_sku
         .filter(F.col('pre_subcl_sales')>0) # only other products
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')
         .orderBy('custs', ascending=False)
        )
        
        return n_cust_both_subcl_switch_sku
        
    elif switching_lv == 'class':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_cl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('period_fis_wk').isin(['pre', 'ppp', 'cmp']))

#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])

         .withColumn('pre_cl_sales', F.when( F.col('period_fis_wk').isin(['ppp', 'pre']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_cl_sales', F.when( F.col('period_fis_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_cl_sales', F.sum(F.col('pre_cl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_cl_sales', F.sum(F.col('dur_cl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_cl = txn_sel_cl_cc.filter( (F.col('cust_tt_pre_cl_sales')>0) & (F.col('cust_tt_dur_cl_sales')>0) )
        
        txn_cust_both_period_cl_not_pre_but_dur_sku = \
        (txn_cust_both_period_cl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_fis_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_fis_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        n_cust_both_cl_switch_sku = \
        (txn_cust_both_period_cl_not_pre_but_dur_sku
         .filter(F.col('pre_cl_sales')>0)
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')         
         .orderBy('custs', ascending=False)
        )
        return n_cust_both_cl_switch_sku
        
    else:
        print('Unrecognized switching level')
        return None

# COMMAND ----------

# MAGIC %md ## GoFresh : Customer Movement and Switching-PromoWk

# COMMAND ----------

def cust_movement_promo_wk(switching_lv, 
                  txn, 
                  cp_start_date, 
                  cp_end_date, 
                  brand_df,
                  test_store_sf, 
#                   adj_prod_sf, 
                  feat_list):
    """Media evaluation solution, Customer movement and switching v2 - GoFresh
    - Exposure based on each store media period
    
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    print('Customer movement Promo week')
    print('Customer movement for "OFFLINE" + "ONLINE"')
    
    #---- Get scope for brand in class / brand in subclass
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Get list of feature brand name
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
        
    #---- During camapign - exposed customer, 
    dur_campaign_exposed_cust = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media          
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_exposed_date'))
    )
    
    #---- During campaign - Exposed & Feature Brand buyer
    dur_campaign_brand_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .join(brand_df, 'upc_id')
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_brand_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_brand_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_brand_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_brand_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_brand_buy_date'))
     .select('household_id')
    )
    
    activated_brand = dur_campaign_exposed_cust_and_brand_shopper.count()
    print(f'Total exposed and brand shopper (Activated Brand) : {activated_brand}')    
    
    #---- During campaign - Exposed & Features SKU shopper
    dur_campaign_sku_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_sku_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_sku_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_sku_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
     .select('household_id')
    )
    
    activated_sku = dur_campaign_exposed_cust_and_sku_shopper.count()
    print(f'Total exposed and sku shopper (Activated SKU) : {activated_sku}')    
    
    activated_df = pd.DataFrame({'customer_exposed_brand_activated':[activated_brand], 'customer_exposed_sku_activated':[activated_sku]})
    
    #---- Find Customer movement from (PPP+PRE) -> CMP period
    
    # Existing and New SKU buyer (movement at micro level)
    prior_pre_sku_shopper = \
    (txn
     .filter(F.col('period_promo_wk').isin(['pre', 'ppp']))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )
    
    existing_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
    )
    
    existing_exposed_cust_and_sku_shopper = existing_exposed_cust_and_sku_shopper.checkpoint()
    
    new_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
    )
    new_exposed_cust_and_sku_shopper = new_exposed_cust_and_sku_shopper.checkpoint()
        
    #---- Movement macro level for New to SKU
    prior_pre_cc_txn = \
    (txn
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('period_promo_wk').isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .select('household_id')
    ).drop_duplicates()
    
    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
     .select('household_id')
    ).drop_duplicates()
        
    #---- Grouping, flag customer macro flag
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )
    
    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )
    
    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )
        
        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()
        
        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    elif switching_lv == 'class':
        
        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    else:
        print('Not recognized Movement and Switching level param')
        return None

# COMMAND ----------

def cust_switching_promo_wk(switching_lv, cust_movement_sf,
                   txn, cp_start_date, cp_end_date, 
#                    prior_start_date, prior_end_date, pre_start_date, pre_end_date,
                   feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame
    
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    print('Customer switching for "OFFLINE" + "ONLINE"')
    #---- switching fn from Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: sparkDataFrame, prod_trans: SparkDataFrame, grp: List, 
                   prod_lev: str, full_prod_lev: str , col_rename: str, period: str) -> SparkDataFrame:
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
#         print('cust_micro_df')
#         cust_micro_df.display()
        # Transaction of focus customer group
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )
#         print('cust_mirco_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        # total of other brand/subclass/class spending
        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]
        
        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))
        
#         print('cust_micro_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        
        print("\t\t\t\t\t\t**Running micro df2")

#         print('cust_micro_df2')
#         cust_micro_df2.display()

        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
#                       'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )            
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif prod_lev == 'class':
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        
        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name','class_name','original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
         .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(), F.col('pct_spend_oth_'+full_prod_lev).desc())
        )
        # display(micro_df_summ)
        switching_result = switching_result.checkpoint()
        
        return switching_result
    
    #---- Main
    # Section id, class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Brand name of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement
        prior_pre_cc_txn_subcl = \
        (txn
         .filter(F.col('period_promo_wk').isin(['pre', 'ppp']))
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        prior_pre_cc_txn_subcl_sel_brand = prior_pre_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_subcl_sel_brand_kpi = \
        (prior_pre_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_subcl = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_promo_wk').isin(['cmp']))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        dur_cc_txn_subcl_sel_brand = dur_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        dur_subcl_sel_brand_kpi = \
        (dur_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name', 'household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_subcl_sel_brand_kpi
         .join(dur_subcl_sel_brand_kpi, 
               ['division_name','department_name','section_name','class_name',
#                 'subclass_name',
                'brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', 
                                                 cust_movement_pre_dur_spend, prior_pre_cc_txn_subcl, 
                                                 ["class_name", 'brand_name'],
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        subcl_cust = dur_cc_txn_subcl.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_subcl
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(subcl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )
        
        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    
    elif switching_lv == 'class':
        #---- Create class-brand spend by cust movment
        prior_pre_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('period_promo_wk').isin(['pre', 'ppp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        prior_pre_cc_txn_class_sel_brand = prior_pre_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_class_sel_brand_kpi = \
        (prior_pre_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_promo_wk').isin(['cmp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        dur_cc_txn_class_sel_brand = dur_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        dur_class_sel_brand_kpi = \
        (dur_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_class_sel_brand_kpi
         .join(dur_class_sel_brand_kpi, ['division_name','department_name','section_name',
                  'class_name','brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_class,
                                                 ['class_name', 'brand_name'], 
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within class
        cl_cust = dur_cc_txn_class.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_class
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(cl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    else:
        
        print('Unrecognized switching level')
        return None

# COMMAND ----------

def cust_sku_switching_promo_wk(switching_lv,
                       txn, cp_start_date, cp_end_date, 
#                        prior_start_date, prior_end_date, 
#                        pre_start_date, pre_end_date,
                       test_store_sf, 
#                        adj_prod_sf, 
                       feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame
    
    """Media evaluation solution, customer sku switching
    """
    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    
    prod_desc = spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()
    
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    print('Customer switching SKU for "OFFLINE" + "ONLINE"')
    
    #---- From Exposed customer, find Existing and New SKU buyer (movement at micro level)
    dur_campaign_exposed_cust = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media          
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
#      .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_exposed_date'))
    )
    
    dur_campaign_sku_shopper = \
    (txn
     .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('household_id')
     .agg(F.min('date_id').alias('first_sku_buy_date'))
     .drop_duplicates()
    )
    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust
     .join(dur_campaign_sku_shopper, 'household_id', 'inner')
     .filter(F.col('first_exposed_date').isNotNull())
     .filter(F.col('first_sku_buy_date').isNotNull())
     .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
     .select('household_id')
    )    
    #----
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_subcl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
          .filter(F.col('period_promo_wk').isin(['pre', 'ppp', 'cmp']))

#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])

         .withColumn('pre_subcl_sales', F.when( F.col('period_promo_wk').isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_subcl_sales', F.when( F.col('period_promo_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_subcl_sales', F.sum(F.col('pre_subcl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_subcl_sales', F.sum(F.col('dur_subcl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_subcl = txn_sel_subcl_cc.filter( (F.col('cust_tt_pre_subcl_sales')>0) & (F.col('cust_tt_dur_subcl_sales')>0) )
        
        txn_cust_both_period_subcl_not_pre_but_dur_sku = \
        (txn_cust_both_period_subcl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_promo_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_promo_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        
        n_cust_both_subcl_switch_sku = \
        (txn_cust_both_period_subcl_not_pre_but_dur_sku
         .filter(F.col('pre_subcl_sales')>0) # only other products
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')
         .orderBy('custs', ascending=False)
        )
        
        return n_cust_both_subcl_switch_sku
        
    elif switching_lv == 'class':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_cl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('period_promo_wk').isin(['pre', 'ppp', 'cmp']))

#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])

         .withColumn('pre_cl_sales', F.when( F.col('period_promo_wk').isin(['ppp', 'pre']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_cl_sales', F.when( F.col('period_promo_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_cl_sales', F.sum(F.col('pre_cl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_cl_sales', F.sum(F.col('dur_cl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_cl = txn_sel_cl_cc.filter( (F.col('cust_tt_pre_cl_sales')>0) & (F.col('cust_tt_dur_cl_sales')>0) )
        
        txn_cust_both_period_cl_not_pre_but_dur_sku = \
        (txn_cust_both_period_cl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_promo_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_promo_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        n_cust_both_cl_switch_sku = \
        (txn_cust_both_period_cl_not_pre_but_dur_sku
         .filter(F.col('pre_cl_sales')>0)
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')         
         .orderBy('custs', ascending=False)
        )
        return n_cust_both_cl_switch_sku
        
    else:
        print('Unrecognized switching level')
        return None

# COMMAND ----------

# MAGIC %md ##Create Control Store

# COMMAND ----------

def get_rest_control_store(format_group_name: str, test_store_sf: sparkDataFrame, 
                           GoFresh_mediable_store_path: str = None) -> pandasDataFrame:
    """
    Parameters
    ----------
    format_group_name:
        'HDE' / 'Talad' / 'GoFresh'.
        
    test_store_sf: sparkDataFrame
        Media features store list.
    
    store_universe: str, default = None
        Store universe/network store that could install media material
    """
    from pyspark.sql import functions as F
    
    all_store_id = spark.table(TBL_STORE).select('format_id', 'store_id').filter(~F.col('store_id').like('8%')).drop_duplicates()
    
    if format_group_name == 'HDE':
        universe_store_id = all_store_id.filter(F.col('format_id').isin([1,2,3])).select('store_id')
        rest_control_store_sf = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'Talad':
        universe_store_id = all_store_id.filter(F.col('format_id').isin([4])).select('store_id')
        rest_control_store_sf = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'GoFresh':
        if not GoFresh_mediable_store_path:
            store_universe = 'dbfs:/FileStore/media/reserved_store/20211226/media_store_universe_20220309_GoFresh.csv'
        print(f'For GoFresh store use network stores (1200 branched) universe \n Loading from {store_universe}')
        network_gofresh = spark.read.csv(store_universe, header=True, inferSchema=True)
        rest_control_store_sf = network_gofresh.join(test_store_sf, 'store_id', 'leftanti')
        
    else:
        print('Count not create dummy reserved store')
        
    return rest_control_store_sf

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Store matching

# COMMAND ----------

def get_store_matching(txn, prior_st_date, pre_end_date, store_matching_lv: str, sel_brand:str, sel_sku: List, switching_lv: str,
                       test_store_sf: sparkDataFrame, reserved_store_sf: sparkDataFrame, matching_methodology: str = 'varience') -> List:
    """
    Parameters
    ----------
    txn: sparkDataFrame
        
    prior_st_date: date_id
        Prior start date, 26 week before campaign start date
    
    pre_end_date: date_id
        Pre end date, 1 day before start campaing start date
    
    store_matching_lv: str
        'brand' or 'sku'
    
    sel_brand: str
        Selected brand for analysis
    
    sel_sku: List
        List of promoted upc_id
        
    switching_lv: str
        Use switching level to define 'brand' scope for store matching.
        This apply only store_matching_lv = 'brand'. If switching_lv = 'class' then
        store matching by KPI of Focus brand in features class. When swithcing_lv = 'subclass'
        then matching by KPI of Focus brand in features subclass
        
    test_store_sf: sparkDataFrame
        Media features store list
        
    reserved_store_sf: sparkDataFrame
        Customer picked reserved store list, for finding store matching -> control store
        
    matching_methodology: str, default 'varience'
        'variance', 'euclidean', 'cosine_similarity'
    """
    from pyspark.sql import functions as F
    
    from sklearn.metrics import auc
    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity
    
    #---- filter txn at matching level
    print('-'*50)
    print('Store matching v.2')
    print('Multi-Section / Class / Subclass')
    print(f'Store matching at : {store_matching_lv} level')
    
    brand_scope_columns = ['section_id', 'class_id']
    if store_matching_lv.lower() == 'brand':
        print(f'Switching level at : {switching_lv} level, will use to define scope of brand for store mathching kpi')
        if switching_lv.lower() == 'subclass':
            brand_scope_columns = ['section_id', 'class_id', 'subclass_id']
        else:
            brand_scope_columns = ['section_id', 'class_id']

    features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    features_sku_sf = spark.createDataFrame(features_sku_df)
    features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select(brand_scope_columns).drop_duplicates()
    (spark
     .table(TBL_PROD)
     .join(features_sec_class_subcl, brand_scope_columns)
     .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
     .drop_duplicates()
     .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    ).show(truncate=False)
    print('-'*50)
    
    if store_matching_lv.lower() == 'brand':
        print('Matching performance only "OFFLINE"')
        print('Store matching at BRAND lv')
        txn_matching = \
        (txn
         .join(features_sec_class_subcl, brand_scope_columns)
         .filter(F.col('date_id').between(prior_st_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('brand_name')==sel_brand)
        )
    else:
        print('Matching performance only "OFFLINE"')
        print('Store matching at SKU lv')
        txn_matching = \
        (txn
         .filter(F.col('date_id').between(prior_st_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('upc_id').isin(sel_sku))
        )
    #---- Map weekid
#     date_id_week_id_mapping = \
#     (spark
#      .table('tdm.v_date_dim')
#      .select('date_id', 'week_id')
#      .drop_duplicates()
#     )
    
#     txn_matching = txn_matching.join(date_id_week_id_mapping, 'date_id')
    
    #----- create test / resereved txn 
    test_txn_matching = txn_matching.join(test_store_list,'store_id','inner')
    rs_txn_matching = txn_matching.join(rs_store_list,'store_id','inner')

    #---- get weekly feature by store
    test_wk_matching = test_txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
    rs_wk_matching = rs_txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
    all_store_wk_matching = txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))

    #convert to pandas
    test_df = to_pandas(test_wk_matching).fillna(0)
    rs_df = to_pandas(rs_wk_matching).fillna(0)
    all_store_df = to_pandas(all_store_wk_matching).fillna(0)
    
    #get matching features
    f_matching = all_store_df.columns[3:]

    #using Standardscaler to scale features first
    ss = StandardScaler()
    ss.fit(all_store_df[f_matching])
    test_ss = ss.transform(test_df[f_matching])
    reserved_ss = ss.transform(rs_df[f_matching])

    #setup dict to collect info
    dist_dict = {}
    var_dict = {}
    cos_dict = {}

    #for each test store
    for i in range(len(test_ss)):

        #set standard euc_distance
        dist0 = 10**9
        #set standard var
        var0 = 100
        #set standard cosine
        cos0 = -1

        #finding its region & store_id
        test_region = test_df.iloc[i].store_region
        test_store_id = test_df.iloc[i].store_id
        #get value from that test store
        test = test_ss[i]

        #get index for reserved store
        ctr_index = rs_df[rs_df['store_region'] == test_region].index

        #loop in that region
        for j in ctr_index:
            #get value of res store & its store_id
            res = reserved_ss[j]
            res_store_id = rs_df.iloc[j].store_id

    #-----------------------------------------------------------------------------
            #finding min distance
            dist = distance.euclidean(test,res)
            if dist < dist0:
                dist0 = dist
                dist_dict[test_store_id] = [res_store_id,dist]

    #-----------------------------------------------------------------------------            
            #finding min var
            var = stats.variance(np.abs(test-res))
            if var < var0:
                var0 = var
                var_dict[test_store_id] = [res_store_id,var]

    #-----------------------------------------------------------------------------  
            #finding highest cos
            cos = cosine_similarity(test.reshape(1,-1),res.reshape(1,-1))[0][0]
            if cos > cos0:
                cos0 = cos
                cos_dict[test_store_id] = [res_store_id,cos]
    
    #---- create dataframe            
    dist_df = pd.DataFrame(dist_dict,index=['ctr_store_dist','euc_dist']).T.reset_index().rename(columns={'index':'store_id'})
    var_df = pd.DataFrame(var_dict,index=['ctr_store_var','var']).T.reset_index().rename(columns={'index':'store_id'})
    cos_df = pd.DataFrame(cos_dict,index=['ctr_store_cos','cos']).T.reset_index().rename(columns={'index':'store_id'})
    
    #join to have ctr store by each method
    matching_df = test_df[['store_id','store_region']].merge(dist_df[['store_id','ctr_store_dist']],on='store_id',how='left')\
                                                  .merge(var_df[['store_id','ctr_store_var']],on='store_id',how='left')\
                                                  .merge(cos_df[['store_id','ctr_store_cos']],on='store_id',how='left')

    #change data type to int
    matching_df.ctr_store_dist = matching_df.ctr_store_dist.astype('int')
    matching_df.ctr_store_var = matching_df.ctr_store_var.astype('int')
    matching_df.ctr_store_cos = matching_df.ctr_store_cos.astype('int')
    
    #----select control store using var method
    if matching_methodology == 'varience':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_var]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'euclidean':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_dist]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'cosine_similarity':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_cos]))
        return ctr_store_list, matching_df
    else:
        print('Matching metodology not in scope list : varience, euclidean, cosine_similarity')
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matching fisweek auto select match level

# COMMAND ----------

def get_store_matching_at( txn
                          , pre_en_wk
                          , brand_df
                          , sel_sku: List
                          , test_store_sf
                          , reserved_store_sf
                          , matching_methodology: str = 'varience') -> List:
    """
    Parameters
    ----------
    txn: sparkDataFrame
        
    pre_en_wk: End pre_period week --> yyyymm
        Pre period end week
    
    store_matching_lv: str
        'brand' or 'sku'
    
    brand_df: sparkDataFrame
        sparkDataFrame contain all analysis SKUs of brand (at switching level)
    
    sclass_df: sparkDataFrame
         sparkDataFrame contain all analysis SKUs of subclass of feature product
        
    switching_lv: str
        Use switching level to define 'brand' scope for store matching.
        This apply only store_matching_lv = 'brand'. If switching_lv = 'class' then
        store matching by KPI of Focus brand in features class. When swithcing_lv = 'subclass'
        then matching by KPI of Focus brand in features subclass
        
    test_store_sf: sparkDataFrame
        Media features store list
        
    reserved_store_sf: sparkDataFrame
        Customer picked reserved store list, for finding store matching -> control store
        
    matching_methodology: str, default 'varience'
        'variance', 'euclidean', 'cosine_similarity'
    """
    from pyspark.sql import functions as F
    
    from sklearn.metrics import auc
    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity
    
    ###================================================================ ## Pat Add check for matching Level
    ## Add check trans week to select matching level -- Pat May 2022
    ## ---------------------------------------------------
    ## add initial value
    
    match_lvl  = 'na'
    
    #pre_en_wk = wk_of_year_ls(pre_end_date)
    pre_st_wk  = week_cal(pre_en_wk, -12)  ## go to 12 week before given week, when inclusive end week = 13 week
    
    
    ##-------------------------------------------
    ## need to count sales week of each store - Feature
    ##-------------------------------------------
    ## Target store check
    txn_match_trg  = txn.join  (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                        .where  ((txn.upc_id.isin(sel_sku)) & 
                                 (txn.week_id.between(pre_st_wk, pre_en_wk)) &
                                 (txn.channel == 'OFFLINE'))\
                        .select ( txn['*']
                                 ,test_store_sf.store_region.alias('store_region_new')
                                 ,lit('test').alias('store_type')
                                )\
                        .persist()

    trg_wk_cnt_df = txn_match_trg.groupBy(txn_match_trg.store_id)\
                                 .agg    (countDistinct(txn_match_trg.week_id).alias('wk_sales'))
                                
    trg_min_wk    = trg_wk_cnt_df.agg(min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
    
    print(' Feature product - target stores min week sales = ' + str(trg_min_wk) )
    #print(trg_min_wk)
    
    del trg_wk_cnt_df
    ### Control store check
    txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                        .where  ((txn.upc_id.isin(sel_sku))  & 
                                 (txn.week_id.between(pre_st_wk, pre_en_wk))&
                                 (txn.channel == 'OFFLINE'))\
                        .select ( txn['*']
                                 ,reserved_store_sf.store_region.alias('store_region_new')
                                 ,lit('ctrl').alias('store_type')
                                )\
                        .persist()

    ctl_wk_cnt_df = txn_match_ctl.groupBy(txn_match_ctl.store_id)\
                                 .agg    (countDistinct(txn_match_ctl.week_id).alias('wk_sales'))
                                
    ctl_min_wk    = ctl_wk_cnt_df.agg(min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
    
    print('\n Feature control stores min week sales = ' + str(ctl_min_wk))
    
    del ctl_wk_cnt_df
    
    ## check if can match at feature : need to have sales >= 3 weeks
    if (trg_min_wk >= 3) & (ctl_min_wk >= 3) :
        match_lvl = 'feat'
        print('-'*80 + ' \n This campaign will do matching at "Feature" product \n' )
        print(' Matching performance only "OFFLINE" \n ' + '-'*80 + '\n')
        ## Combine transaction that use for matching
        
        txn_matching = txn_match_trg.union(txn_match_ctl)
    else:
        ## delete feature dataframe and will use at other level
        txn_match_trg.unpersist()
        txn_match_ctl.unpersist()
        del txn_match_trg
        del txn_match_ctl
        #match_lvl = 'na'  ## match level still be = 'na'
        ##-------------------------------------------
        ## need to count sales week of each store - Brand
        ##-------------------------------------------
        txn_match_trg  = txn.join   (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                            .join   (brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                            .where  ((txn.week_id.between(pre_st_wk, pre_en_wk))&
                                    (txn.channel == 'OFFLINE'))\
                            .select ( txn['*']
                                     ,test_store_sf.store_region.alias('store_region_new')
                                     ,lit('test').alias('store_type')
                                    )\
                            .persist()
                            
        txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                            .join   (brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                            .where  ((txn.week_id.between(pre_st_wk, pre_en_wk)) &
                                    (txn.channel == 'OFFLINE'))\
                            .select ( txn['*']
                                     ,reserved_store_sf.store_region.alias('store_region_new')
                                     ,lit('ctrl').alias('store_type')
                                     )\
                            .persist()
        
        ## check count week
        ## Target
        trg_wk_cnt_df = txn_match_trg.groupBy(txn_match_trg.store_id)\
                                     .agg    (countDistinct(txn_match_trg.week_id).alias('wk_sales'))
        trg_min_wk    = trg_wk_cnt_df.agg(min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
        
        print('\n Brand Target stores min week sales = ' + str(trg_min_wk) )
        
        del trg_wk_cnt_df
        
        ## control
        ctl_wk_cnt_df = txn_match_ctl.groupBy(txn_match_ctl.store_id)\
                                     .agg    (countDistinct(txn_match_ctl.week_id).alias('wk_sales'))
                                
        ctl_min_wk    = ctl_wk_cnt_df.agg(min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
        
        print('\n Brand control stores min week sales = ' + str(ctl_min_wk) )
         
        del ctl_wk_cnt_df
        
        if (trg_min_wk >= 3) & (ctl_min_wk >= 3) :
            match_lvl = 'brand'
            print('-'*80 + ' \n This campaign will do matching at "Brand" Level \n' )
            print(' Matching performance only "OFFLINE" \n' + '-'*80 + '\n')
            
            ## Combine transaction that use for matching
            txn_matching = txn_match_trg.union(txn_match_ctl)
        else:
            match_lvl = 'subclass'
            print('-'*80 + ' \n This campaign will do matching at "Subclass" Level \n'  )
            print(' Matching performance only "OFFLINE" \n' + '-'*80 + '\n')
            txn_match_trg.unpersist()
            txn_match_ctl.unpersist()
            del txn_match_trg
            del txn_match_ctl
            
            ## create trans match
            txn_match_trg  = txn.join   (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                                .join   (sclass_df, [txn.upc_id == sclass_df.upc_id], 'left_semi')\
                                .where  ((txn.week_id.between(pre_st_wk, pre_en_wk))&
                                        (txn.channel == 'OFFLINE'))\
                                .select ( txn['*']
                                         ,test_store_sf.store_region.alias('store_region_new')
                                         ,lit('test').alias('store_type')
                                        )\
                                .persist()
                            
            txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                                .join   (sclass_df, [txn.upc_id == sclass_df.upc_id], 'left_semi')\
                                .where  ((txn.week_id.between(pre_st_wk, pre_en_wk)) &
                                        (txn.channel == 'OFFLINE'))\
                                .select ( txn['*']
                                         ,reserved_store_sf.store_region.alias('store_region_new')
                                         ,lit('ctrl').alias('store_type')
                                        )\
                                .persist() 
            
            ## Combine transaction that use for matching
            txn_matching = txn_match_trg.union(txn_match_ctl)
            
        ## end if
        
    ## end if
    
    ###================================================================ ## End -- Pat Add check for matching Level
        
    #---- filter txn at matching level
    #print('-'*50)
    #print('Store matching v.3')
    #print('Multi-Section / Class / Subclass')
    #print(f'Store matching at : {match_lvl} level')
    
    #brand_scope_columns = ['section_id', 'class_id']
    #if store_matching_lv.lower() == 'brand':
    #    print(f'Switching level at : {switching_lv} level, will use to define scope of brand for store mathching kpi')
    #    if switching_lv.lower() == 'subclass':
    #        brand_scope_columns = ['section_id', 'class_id', 'subclass_id']
    #    else:
    #        brand_scope_columns = ['section_id', 'class_id']
    #
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select(brand_scope_columns).drop_duplicates()
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, brand_scope_columns)
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*50)
    #
    #if store_matching_lv.lower() == 'brand':
    #    print('Matching performance only "OFFLINE"')
    #    print('Store matching at BRAND lv')
    #    txn_matching = \
    #    (txn
    #     .join(features_sec_class_subcl, brand_scope_columns)
    #     .filter(F.col('date_id').between(prior_st_date, pre_end_date))
    #     .filter(F.col('channel')=='OFFLINE')
    #     .filter(F.col('brand_name')==sel_brand)
    #    )
    #else:
    #    print('Matching performance only "OFFLINE"')
    #    print('Store matching at SKU lv')
    #    txn_matching = \
    #    (txn
    #     .filter(F.col('date_id').between(prior_st_date, pre_end_date))
    #     .filter(F.col('channel')=='OFFLINE')
    #     .filter(F.col('upc_id').isin(sel_sku))
    #    )
    #---- Map weekid
#     date_id_week_id_mapping = \
#     (spark
#      .table('tdm.v_date_dim')
#      .select('date_id', 'week_id')
#      .drop_duplicates()
#     )
    
#     txn_matching = txn_matching.join(date_id_week_id_mapping, 'date_id')
    
    #----- create test / resereved txn 
    
    #txn_match_trg = txn_matching.join(test_store_list,'store_id','inner')
    #txn_match_ctl = txn_matching.join(rs_store_list,'store_id','inner')

    #---- get weekly sales by store,region
    test_wk_matching      = txn_match_trg.groupBy('store_id','store_region_new').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
    rs_wk_matching        = txn_match_ctl.groupBy('store_id','store_region_new').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
    all_store_wk_matching = txn_matching.groupBy('store_id','store_region_new').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))

    #convert to pandas
    test_df = to_pandas(test_wk_matching).fillna(0)
    rs_df   = to_pandas(rs_wk_matching).fillna(0)
    all_store_df = to_pandas(all_store_wk_matching).fillna(0)
    
    #get matching features
    f_matching = all_store_df.columns[3:]

    #using Standardscaler to scale features first
    ss = StandardScaler()
    ss.fit(all_store_df[f_matching])
    test_ss = ss.transform(test_df[f_matching])
    reserved_ss = ss.transform(rs_df[f_matching])

    #setup dict to collect info
    dist_dict = {}
    var_dict = {}
    cos_dict = {}

    #for each test store
    for i in range(len(test_ss)):

        #set standard euc_distance
        dist0 = 10**9
        #set standard var
        var0 = 100
        #set standard cosine
        cos0 = -1

        #finding its region & store_id
        test_region = test_df.iloc[i].store_region_new
        test_store_id = test_df.iloc[i].store_id
        #get value from that test store
        test = test_ss[i]

        #get index for reserved store
        ctr_index = rs_df[rs_df['store_region_new'] == test_region].index

        #loop in that region
        for j in ctr_index:
            #get value of res store & its store_id
            res = reserved_ss[j]
            res_store_id = rs_df.iloc[j].store_id

    #-----------------------------------------------------------------------------
            #finding min distance
            dist = distance.euclidean(test,res)
            if dist < dist0:
                dist0 = dist
                dist_dict[test_store_id] = [res_store_id,dist]

    #-----------------------------------------------------------------------------            
            #finding min var
            var = stats.variance(np.abs(test-res))
            if var < var0:
                var0 = var
                var_dict[test_store_id] = [res_store_id,var]

    #-----------------------------------------------------------------------------  
            #finding highest cos
            cos = cosine_similarity(test.reshape(1,-1),res.reshape(1,-1))[0][0]
            if cos > cos0:
                cos0 = cos
                cos_dict[test_store_id] = [res_store_id,cos]
    
    #---- create dataframe            
    dist_df = pd.DataFrame(dist_dict,index=['ctr_store_dist','euc_dist']).T.reset_index().rename(columns={'index':'store_id'})
    var_df = pd.DataFrame(var_dict,index=['ctr_store_var','var']).T.reset_index().rename(columns={'index':'store_id'})
    cos_df = pd.DataFrame(cos_dict,index=['ctr_store_cos','cos']).T.reset_index().rename(columns={'index':'store_id'})
    
    #join to have ctr store by each method
    matching_df = test_df[['store_id','store_region_new']].merge(dist_df[['store_id','ctr_store_dist']],on='store_id',how='left')\
                                                      .merge(var_df[['store_id','ctr_store_var']],on='store_id',how='left')\
                                                      .merge(cos_df[['store_id','ctr_store_cos']],on='store_id',how='left')

    #change data type to int
    matching_df.ctr_store_dist = matching_df.ctr_store_dist.astype('int')
    matching_df.ctr_store_var = matching_df.ctr_store_var.astype('int')
    matching_df.ctr_store_cos = matching_df.ctr_store_cos.astype('int')
    
    matching_df.rename(columns = {'store_region_new' : 'store_region'}, inplace = True)
    
    print(' \n Result matching table show below \n')
    matching_df.display()
    
    #----select control store using var method
    if matching_methodology == 'varience':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_var]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'euclidean':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_dist]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'cosine_similarity':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_cos]))
        return ctr_store_list, matching_df
    else:
        print('Matching metodology not in scope list : varience, euclidean, cosine_similarity')
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matching promoweek auto select match level

# COMMAND ----------

def get_store_matching_promo_at( txn
                               , pre_en_promowk
                               , brand_df
                               , sel_sku: List
                               , test_store_sf
                               , reserved_store_sf
                               , matching_methodology: str = 'varience') -> List:
    """
    Use matching at Promo week instead of fis week
    
    Parameters
    ----------
    txn: sparkDataFrame
        
    pre_end_wk: Promo_week_id  yyyymm
        Pre period end promo_week
    
    store_matching_lv: str
        'brand' or 'sku'
    
    brand_df: sparkDataFrame
        sparkDataFrame contain all analysis SKUs of brand (at switching level)
    
    sclass_df: sparkDataFrame
         sparkDataFrame contain all analysis SKUs of subclass of feature product
        
    switching_lv: str
        Use switching level to define 'brand' scope for store matching.
        This apply only store_matching_lv = 'brand'. If switching_lv = 'class' then
        store matching by KPI of Focus brand in features class. When swithcing_lv = 'subclass'
        then matching by KPI of Focus brand in features subclass
        
    test_store_sf: sparkDataFrame
        Media features store list
        
    reserved_store_sf: sparkDataFrame
        Customer picked reserved store list, for finding store matching -> control store
        
    matching_methodology: str, default 'varience'
        'variance', 'euclidean', 'cosine_similarity'
    """
    from pyspark.sql import functions as F
    
    from sklearn.metrics import auc
    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity
    
    ###================================================================ ## Pat Add check for matching Level
    ## Add check trans week to select matching level -- Pat May 2022
    ## ---------------------------------------------------
    ## add initial value
    
    match_lvl  = 'na'
    
    #pre_en_wk = wk_of_year_ls(pre_end_date)
    pre_st_promowk  = promo_week_cal(pre_en_promowk, -12)  ## go to 12 week before given week, when inclusive end week = 13 week
    
    
    ##-------------------------------------------
    ## need to count sales week of each store - Feature
    ##-------------------------------------------
    ## Target store check
    txn_match_trg  = txn.join  (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                        .where  ((txn.upc_id.isin(sel_sku)) & 
                                 (txn.promoweek_id.between(pre_st_promowk, pre_en_promowk)) &
                                 (txn.channel == 'OFFLINE'))\
                        .select ( txn['*']
                                 ,test_store_sf.store_region.alias('store_region_new')
                                 ,lit('test').alias('store_type')
                                )\
                        .persist()

    trg_wk_cnt_df = txn_match_trg.groupBy(txn_match_trg.store_id)\
                                 .agg    (countDistinct(txn_match_trg.promoweek_id).alias('wk_sales'))
                                
    trg_min_wk    = trg_wk_cnt_df.agg(min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
    
    print(' Feature product - target stores min week sales = ' + str(trg_min_wk) )
    #print(trg_min_wk)
    
    del trg_wk_cnt_df
    ### Control store check
    txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                        .where  ((txn.upc_id.isin(sel_sku))  & 
                                 (txn.promoweek_id.between(pre_st_promowk, pre_en_promowk))&
                                 (txn.channel == 'OFFLINE'))\
                        .select ( txn['*']
                                 ,reserved_store_sf.store_region.alias('store_region_new')
                                 ,lit('ctrl').alias('store_type')
                                )\
                        .persist()

    ctl_wk_cnt_df = txn_match_ctl.groupBy(txn_match_ctl.store_id)\
                                 .agg    (countDistinct(txn_match_ctl.promoweek_id).alias('wk_sales'))
                                
    ctl_min_wk    = ctl_wk_cnt_df.agg(min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
    
    print('\n Feature control stores min week sales = ' + str(ctl_min_wk))
    
    del ctl_wk_cnt_df
    
    ## check if can match at feature : need to have sales >= 3 weeks
    if (trg_min_wk >= 3) & (ctl_min_wk >= 3) :
        match_lvl = 'feat'
        print('-'*80 + ' \n This campaign will do matching at "Feature" product \n' )
        print(' Matching performance only "OFFLINE" \n ' + '-'*80 + '\n')
        ## Combine transaction that use for matching
        
        txn_matching = txn_match_trg.union(txn_match_ctl)
    else:
        ## delete feature dataframe and will use at other level
        txn_match_trg.unpersist()
        txn_match_ctl.unpersist()
        del txn_match_trg
        del txn_match_ctl
        #match_lvl = 'na'  ## match level still be = 'na'
        ##-------------------------------------------
        ## need to count sales week of each store - Brand
        ##-------------------------------------------
        txn_match_trg  = txn.join   (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                            .join   (brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                            .where  ((txn.promoweek_id.between(pre_st_wk, pre_en_wk)) &
                                     (txn.channel == 'OFFLINE')
                                    )\
                            .select ( txn['*']
                                     ,test_store_sf.store_region.alias('store_region_new')
                                     ,lit('test').alias('store_type')
                                    )\
                            .persist()
                            
        txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                            .join   (brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                            .where  ( (txn.promoweek_id.between(pre_st_wk, pre_en_wk)) &
                                      (txn.channel == 'OFFLINE')
                                    )\
                            .select ( txn['*']
                                     ,reserved_store_sf.store_region.alias('store_region_new')
                                     ,lit('ctrl').alias('store_type')
                                     )\
                            .persist()
        
        ## check count week
        ## Target
        trg_wk_cnt_df = txn_match_trg.groupBy(txn_match_trg.store_id)\
                                     .agg    (countDistinct(txn_match_trg.promoweek_id).alias('wk_sales'))
        trg_min_wk    = trg_wk_cnt_df.agg(min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
        
        print('\n Brand Target stores min week sales = ' + str(trg_min_wk) )
        
        del trg_wk_cnt_df
        
        ## control
        ctl_wk_cnt_df = txn_match_ctl.groupBy(txn_match_ctl.store_id)\
                                     .agg    (countDistinct(txn_match_ctl.promoweek_id).alias('wk_sales'))
                                
        ctl_min_wk    = ctl_wk_cnt_df.agg(min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0].min_wk_sales
        
        print('\n Brand control stores min week sales = ' + str(ctl_min_wk) )
         
        del ctl_wk_cnt_df
        
        if (trg_min_wk >= 3) & (ctl_min_wk >= 3) :
            match_lvl = 'brand'
            print('-'*80 + ' \n This campaign will do matching at "Brand" Level \n' )
            print(' Matching performance only "OFFLINE" \n' + '-'*80 + '\n')
            
            ## Combine transaction that use for matching
            txn_matching = txn_match_trg.union(txn_match_ctl)
        else:
            match_lvl = 'subclass'
            print('-'*80 + ' \n This campaign will do matching at "Subclass" Level \n'  )
            print(' Matching performance only "OFFLINE" \n' + '-'*80 + '\n')
            txn_match_trg.unpersist()
            txn_match_ctl.unpersist()
            del txn_match_trg
            del txn_match_ctl
            
            ## create trans match
            txn_match_trg  = txn.join   (test_store_sf, [txn.store_id == test_store_sf.store_id], 'inner')\
                                .join   (sclass_df, [txn.upc_id == sclass_df.upc_id], 'left_semi')\
                                .where  ( (txn.promoweek_id.between(pre_st_wk, pre_en_wk)) &
                                          (txn.channel == 'OFFLINE')
                                         )\
                                .select ( txn['*']
                                         ,test_store_sf.store_region.alias('store_region_new')
                                         ,lit('test').alias('store_type')
                                        )\
                                .persist()
                            
            txn_match_ctl  = txn.join   (reserved_store_sf, [txn.store_id == reserved_store_sf.store_id], 'inner')\
                                .join   (sclass_df, [txn.upc_id == sclass_df.upc_id], 'left_semi')\
                                .where  ( (txn.promoweek_id.between(pre_st_wk, pre_en_wk)) &
                                          (txn.channel == 'OFFLINE')
                                        )\
                                .select ( txn['*']
                                         ,reserved_store_sf.store_region.alias('store_region_new')
                                         ,lit('ctrl').alias('store_type')
                                        )\
                                .persist() 
            
            ## Combine transaction that use for matching
            txn_matching = txn_match_trg.union(txn_match_ctl)
            
        ## end if
        
    ## end if
    
    ###================================================================ ## End -- Pat Add check for matching Level
        
    #---- filter txn at matching level
    #print('-'*50)
    #print('Store matching v.3')
    #print('Multi-Section / Class / Subclass')
    #print(f'Store matching at : {match_lvl} level')
    
    #brand_scope_columns = ['section_id', 'class_id']
    #if store_matching_lv.lower() == 'brand':
    #    print(f'Switching level at : {switching_lv} level, will use to define scope of brand for store mathching kpi')
    #    if switching_lv.lower() == 'subclass':
    #        brand_scope_columns = ['section_id', 'class_id', 'subclass_id']
    #    else:
    #        brand_scope_columns = ['section_id', 'class_id']
    #
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select(brand_scope_columns).drop_duplicates()
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, brand_scope_columns)
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*50)
    #
    #if store_matching_lv.lower() == 'brand':
    #    print('Matching performance only "OFFLINE"')
    #    print('Store matching at BRAND lv')
    #    txn_matching = \
    #    (txn
    #     .join(features_sec_class_subcl, brand_scope_columns)
    #     .filter(F.col('date_id').between(prior_st_date, pre_end_date))
    #     .filter(F.col('channel')=='OFFLINE')
    #     .filter(F.col('brand_name')==sel_brand)
    #    )
    #else:
    #    print('Matching performance only "OFFLINE"')
    #    print('Store matching at SKU lv')
    #    txn_matching = \
    #    (txn
    #     .filter(F.col('date_id').between(prior_st_date, pre_end_date))
    #     .filter(F.col('channel')=='OFFLINE')
    #     .filter(F.col('upc_id').isin(sel_sku))
    #    )
    #---- Map weekid
#     date_id_week_id_mapping = \
#     (spark
#      .table('tdm.v_date_dim')
#      .select('date_id', 'week_id')
#      .drop_duplicates()
#     )
    
#     txn_matching = txn_matching.join(date_id_week_id_mapping, 'date_id')
    
    #----- create test / resereved txn 
    
    #txn_match_trg = txn_matching.join(test_store_list,'store_id','inner')
    #txn_match_ctl = txn_matching.join(rs_store_list,'store_id','inner')

    #---- get weekly sales by store,region
    test_wk_matching      = txn_match_trg.groupBy('store_id','store_region_new').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales'))
    rs_wk_matching        = txn_match_ctl.groupBy('store_id','store_region_new').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales'))
    all_store_wk_matching = txn_matching.groupBy('store_id','store_region_new').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales'))

    #convert to pandas
    test_df = to_pandas(test_wk_matching).fillna(0)
    rs_df   = to_pandas(rs_wk_matching).fillna(0)
    all_store_df = to_pandas(all_store_wk_matching).fillna(0)
    
    #get matching features
    f_matching = all_store_df.columns[3:]

    #using Standardscaler to scale features first
    ss = StandardScaler()
    ss.fit(all_store_df[f_matching])
    test_ss = ss.transform(test_df[f_matching])
    reserved_ss = ss.transform(rs_df[f_matching])

    #setup dict to collect info
    dist_dict = {}
    var_dict = {}
    cos_dict = {}

    #for each test store
    for i in range(len(test_ss)):

        #set standard euc_distance
        dist0 = 10**9
        #set standard var
        var0 = 100
        #set standard cosine
        cos0 = -1

        #finding its region & store_id
        test_region = test_df.iloc[i].store_region_new
        test_store_id = test_df.iloc[i].store_id
        #get value from that test store
        test = test_ss[i]

        #get index for reserved store
        ctr_index = rs_df[rs_df['store_region_new'] == test_region].index

        #loop in that region
        for j in ctr_index:
            #get value of res store & its store_id
            res = reserved_ss[j]
            res_store_id = rs_df.iloc[j].store_id

    #-----------------------------------------------------------------------------
            #finding min distance
            dist = distance.euclidean(test,res)
            if dist < dist0:
                dist0 = dist
                dist_dict[test_store_id] = [res_store_id,dist]

    #-----------------------------------------------------------------------------            
            #finding min var
            var = stats.variance(np.abs(test-res))
            if var < var0:
                var0 = var
                var_dict[test_store_id] = [res_store_id,var]

    #-----------------------------------------------------------------------------  
            #finding highest cos
            cos = cosine_similarity(test.reshape(1,-1),res.reshape(1,-1))[0][0]
            if cos > cos0:
                cos0 = cos
                cos_dict[test_store_id] = [res_store_id,cos]
    
    #---- create dataframe            
    dist_df = pd.DataFrame(dist_dict,index=['ctr_store_dist','euc_dist']).T.reset_index().rename(columns={'index':'store_id'})
    var_df = pd.DataFrame(var_dict,index=['ctr_store_var','var']).T.reset_index().rename(columns={'index':'store_id'})
    cos_df = pd.DataFrame(cos_dict,index=['ctr_store_cos','cos']).T.reset_index().rename(columns={'index':'store_id'})
    
    #join to have ctr store by each method
    matching_df = test_df[['store_id','store_region_new']].merge(dist_df[['store_id','ctr_store_dist']],on='store_id',how='left')\
                                                      .merge(var_df[['store_id','ctr_store_var']],on='store_id',how='left')\
                                                      .merge(cos_df[['store_id','ctr_store_cos']],on='store_id',how='left')

    #change data type to int
    matching_df.ctr_store_dist = matching_df.ctr_store_dist.astype('int')
    matching_df.ctr_store_var = matching_df.ctr_store_var.astype('int')
    matching_df.ctr_store_cos = matching_df.ctr_store_cos.astype('int')
    
    matching_df.rename(columns = {'store_region_new' : 'store_region'}, inplace = True)
    
    print(' \n Result matching table show below \n')
    matching_df.display()
    
    #----select control store using var method
    if matching_methodology == 'varience':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_var]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'euclidean':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_dist]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'cosine_similarity':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_cos]))
        return ctr_store_list, matching_df
    else:
        print('Matching metodology not in scope list : varience, euclidean, cosine_similarity')
        return None

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Customer Share

# COMMAND ----------

def cust_kpi(txn, 
             store_fmt,
             test_store_sf,
             ctr_store_list,
             feat_list):
    """Full eval : customer KPIs Pre-Dur for test store vs control store
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
    
    txn_test_pre = txn_all.join(test_store_sf.select('store_id').drop_duplicates() , 'store_id').filter(F.col('period_fis_wk').isin(['pre'])).withColumn('test_ctrl_store', F.lit('test'))
    txn_test_cmp = txn_all.join(test_store_sf.select('store_id').drop_duplicates(), 'store_id').filter(F.col('period_fis_wk').isin(['cmp'])).withColumn('test_ctrl_store', F.lit('test'))
    
    txn_ctrl_pre = txn_all.filter(F.col('store_id').isin(ctr_store_list)).filter(F.col('period_fis_wk').isin(['pre'])).withColumn('test_ctrl_store', F.lit('ctrl'))
    txn_ctrl_cmp = txn_all.filter(F.col('store_id').isin(ctr_store_list)).filter(F.col('period_fis_wk').isin(['cmp'])).withColumn('test_ctrl_store', F.lit('ctrl'))
    
    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('customer_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    txn_ctrl_combine = \
    (txn_ctrl_pre
     .unionByName(txn_ctrl_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('customer_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    txn_all_combine = txn_test_combine.unionByName(txn_ctrl_combine)    
    
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
    (txn_all_combine
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('test_ctrl_store','period_fis_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_sku'))
    )
    
    brand_in_subclass_kpi = \
    (txn_all_combine
     .join(features_brand, 'brand_name')
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('test_ctrl_store','period_fis_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_subclass'))
    )
    
    all_subclass_kpi = \
    (txn_all_combine
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('test_ctrl_store','period_fis_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_subclass'))
    )    
    
    brand_in_class_kpi = \
    (txn_all_combine
     .join(features_brand, 'brand_name')
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('test_ctrl_store','period_fis_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_class'))
    )
    
    all_class_kpi = \
    (txn_all_combine
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('test_ctrl_store','period_fis_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_class'))
    )
    
    combined_kpi = sku_kpi.unionByName(brand_in_subclass_kpi).unionByName(all_subclass_kpi).unionByName(brand_in_class_kpi).unionByName(all_class_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['test_ctrl_store', 'period_fis_wk', 'kpi_level', 'MyLo_customer']].pivot(index=['test_ctrl_store','period_fis_wk'], columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_subclass'] = df_pv['feature_sku']/df_pv['feature_subclass']*100
    df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_subclass'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_class']/df_pv['feature_class']*100
    df_pv.sort_index(axis=0, ascending=False, inplace=True)
    df_pv_out = df_pv.reset_index()
    
    return combined_kpi, kpi_df, df_pv_out

# COMMAND ----------

# MAGIC %md ## customer KPI with no control (pre-during, test vs control)

# COMMAND ----------

def cust_kpi_noctrl(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 
    #---- Features products, brand, classs, subclass
    #features_brand = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('brand_name').drop_duplicates()
    #features_class = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id').drop_duplicates()
    #features_subclass = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    
    features_brand    = brand_df
    features_category = cate_df
    
    trg_str_df   = test_store_sf.select('store_id').dropDuplicates().persist()
    
    txn_test_pre = txn_all.filter(txn_all.period_promo_wk.isin(['pre']))\
                          .join  (trg_str_df , 'store_id', 'left_semi')
                          
    txn_test_cmp = txn_all.filter(txn_all.period_promo_wk.isin(['cmp']))\
                          .join(trg_str_df, 'store_id', 'left_semi')
    
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
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_promo_wk')\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))

    
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                            .groupBy('period_promo_wk')\
                                            .pivot('carded_nonCarded')\
                                            .agg(*kpis)\
                                            .withColumn('kpi_level', F.lit('feature_brand'))
    
    
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_promo_wk')\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))
                                       
   ##------------------------------------------------------------------------------
   ## Pat comment out, will do only 1 level at category (either class or subclass)
   ##------------------------------------------------------------------------------
   
   # brand_in_class_kpi = \
   # (txn_test_combine
   #  .join(features_brand, 'brand_name')
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_brand_in_class'))
   # )
   # 
   # all_class_kpi = \
   # (txn_test_combine
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_class'))
   # )
    
    combined_kpi = sku_kpi.unionByName(brand_in_cate_kpi).unionByName(all_category_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['period_promo_wk', 'kpi_level', 'MyLo_customer']].pivot(index='period_promo_wk', columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi, kpi_df, cust_share_pd

# COMMAND ----------

# MAGIC %md ##GoFresh : Customer Share - PromoWk

# COMMAND ----------

def cust_kpi_promo_wk(txn, 
             store_fmt,
             test_store_sf,
             ctr_store_list,
             feat_list):
    """Full eval : customer KPIs Pre-Dur for test store vs control store
    - Features SKUs
    - Feature Brand in subclass
    - All Feature subclass
    - Feature Brand in class
    - All Feature class
    """ 
    print('Customer KPI - Promo week')
    #---- Features products, brand, classs, subclass
    features_brand = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('brand_name').drop_duplicates()
    features_class = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id').drop_duplicates()
    features_subclass = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    
    txn_test_pre = txn_all.join(test_store_sf.select('store_id').drop_duplicates() , 'store_id').filter(F.col('period_promo_wk').isin(['pre'])).withColumn('test_ctrl_store', F.lit('test'))
    txn_test_cmp = txn_all.join(test_store_sf.select('store_id').drop_duplicates(), 'store_id').filter(F.col('period_promo_wk').isin(['cmp'])).withColumn('test_ctrl_store', F.lit('test'))
    
    txn_ctrl_pre = txn_all.filter(F.col('store_id').isin(ctr_store_list)).filter(F.col('period_promo_wk').isin(['pre'])).withColumn('test_ctrl_store', F.lit('ctrl'))
    txn_ctrl_cmp = txn_all.filter(F.col('store_id').isin(ctr_store_list)).filter(F.col('period_promo_wk').isin(['cmp'])).withColumn('test_ctrl_store', F.lit('ctrl'))
    
    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('customer_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    txn_ctrl_combine = \
    (txn_ctrl_pre
     .unionByName(txn_ctrl_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('customer_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    txn_all_combine = txn_test_combine.unionByName(txn_ctrl_combine)    
    
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
    (txn_all_combine
     .filter(F.col('upc_id').isin(feat_list))
     .groupBy('test_ctrl_store','period_promo_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_sku'))
    )
    
    brand_in_subclass_kpi = \
    (txn_all_combine
     .join(features_brand, 'brand_name')
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('test_ctrl_store','period_promo_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_subclass'))
    )
    
    all_subclass_kpi = \
    (txn_all_combine
     .join(features_subclass, ['section_id', 'class_id', 'subclass_id'])
     .groupBy('test_ctrl_store','period_promo_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_subclass'))
    )    
    
    brand_in_class_kpi = \
    (txn_all_combine
     .join(features_brand, 'brand_name')
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('test_ctrl_store','period_promo_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_brand_in_class'))
    )
    
    all_class_kpi = \
    (txn_all_combine
     .join(features_class, ['section_id', 'class_id'])
     .groupBy('test_ctrl_store','period_promo_wk')
     .pivot('carded_nonCarded')
     .agg(*kpis)
     .withColumn('kpi_level', F.lit('feature_class'))
    )
    
    combined_kpi = sku_kpi.unionByName(brand_in_subclass_kpi).unionByName(all_subclass_kpi).unionByName(brand_in_class_kpi).unionByName(all_class_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['test_ctrl_store', 'period_promo_wk', 'kpi_level', 'MyLo_customer']].pivot(index=['test_ctrl_store','period_promo_wk'], columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_subclass'] = df_pv['feature_sku']/df_pv['feature_subclass']*100
    df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_subclass'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_class']/df_pv['feature_class']*100
    df_pv.sort_index(axis=0, ascending=False, inplace=True)
    df_pv_out = df_pv.reset_index()
    
    return combined_kpi, kpi_df, df_pv_out

# COMMAND ----------

# MAGIC %md ##Customer Uplift

# COMMAND ----------

def get_customer_uplift(txn,
                        ctr_store_list, 
                        test_store_sf, 
                        cp_start_date,
                        cp_end_date,
                        adj_prod_sf, 
                        cust_uplift_lv,
                        switching_lv, 
                        feat_list):
    """customer id that 
    Expose (shop adjacency product during campaing in test store)
    Unexpose (shop adjacency product during campaing in control store)
    expose superseded unexpose
    """
    from pyspark.sql import functions as F

    print('Customer uplift with exposed/unexposed media at "OFFLINE"')

    #---- Get scope for brand in class / brand in subclass
    # ---- Section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    
    # brand of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
    txn_exposed = \
    (txn
      .filter(F.col('channel')=='OFFLINE') # for offline media     
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .join(adj_prod_sf, 'upc_id', 'inner')
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
   )
    print('Exposed customer (test store) from "OFFLINE" channel only')
    
    txn_unexposed = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .filter(F.col('period_fis_wk').isin('cmp'))
     .filter(F.col('store_id').isin(ctr_store_list))
     .join(adj_prod_sf, 'upc_id', 'inner')
    )
    print('Unexposed customer (control store) from "OFFLINE" channel only')
    
    exposed_cust = \
    (txn_exposed
     .filter(F.col('household_id').isNotNull())
     .groupBy('household_id')
     .agg(F.min('date_id').alias('1st_exposed_date'))
     .withColumn('exposed_flag', F.lit(1))
    )

    unexposed_cust = \
    (txn_unexposed
     .filter(F.col('household_id').isNotNull())
     .groupBy('household_id')
     .agg(F.min('date_id').alias('1st_unexposed_date'))
     .withColumn('unexposed_flag', F.lit(1))
    )

    exposure_cust_table = exposed_cust.join(unexposed_cust, 'household_id', 'outer').fillna(0)
    
    #----------------------
    #---- uplift
    # brand buyer, during campaign
    print('-'*30)
    print(f'Customer uplift at {cust_uplift_lv.upper()} under {switching_lv.upper()} level')
    
    if cust_uplift_lv == 'brand':
        
        if switching_lv == "subclass":
            
            buyer_dur_campaign = \
            (txn
            .filter(F.col('period_fis_wk').isin(['cmp']))
            .filter(F.col('household_id').isNotNull())
            .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .groupBy('household_id')
            .agg(F.sum('net_spend_amt').alias('spending'),
                F.min('date_id').alias('1st_buy_date'))
            )
            
            # customer movement brand : prior - pre
            cust_prior = \
            (txn
            .filter(F.col('period_fis_wk').isin(['ppp']))
            .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
            )

            cust_pre = \
            (txn
            .filter(F.col('period_fis_wk').isin(['pre']))
            .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
            )
            
        elif switching_lv == "class":
            buyer_dur_campaign = \
            (txn
            .filter(F.col('period_fis_wk').isin(['cmp']))
            .filter(F.col('household_id').isNotNull())
            .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .groupBy('household_id')
            .agg(F.sum('net_spend_amt').alias('spending'),
                F.min('date_id').alias('1st_buy_date'))
            )
            
            # customer movement brand : prior - pre
            cust_prior = \
            (txn
            .filter(F.col('period_fis_wk').isin(['ppp']))
            .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
            )

            cust_pre = \
            (txn
            .filter(F.col('period_fis_wk').isin(['pre']))
            .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
            )
        else:
            print(f"Switching parameter : {switching_lv} unknown")
        
    elif cust_uplift_lv == 'sku':
        
        buyer_dur_campaign = \
        (txn
         .filter(F.col('period_fis_wk').isin('cmp'))
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id')
         .agg(F.sum('net_spend_amt').alias('spending'),
              F.min('date_id').alias('1st_buy_date'))
        )
        # customer movement brand : prior - pre
        cust_prior = \
        (txn
         .filter(F.col('period_fis_wk').isin(['ppp']))
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
        )

        cust_pre = \
        (txn
         .filter(F.col('period_fis_wk').isin(['pre']))         
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
        )
        
    # brand buyer, exposed-unexposed cust
    dur_camp_exposure_cust_and_buyer = \
    (exposure_cust_table
     .join(buyer_dur_campaign, 'household_id', 'left')
     .withColumn('exposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNotNull() ) & \
                                                 (F.col('1st_buy_date').isNotNull() ) & \
                                                 (F.col('1st_exposed_date') <= F.col('1st_buy_date')), '1').otherwise(0))
     .withColumn('unexposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNull()) & \
                                                   (F.col('1st_unexposed_date').isNotNull()) & \
                                                   (F.col('1st_buy_date').isNotNull()) & \
                                                   (F.col('1st_unexposed_date') <= F.col('1st_buy_date')), '1').otherwise(0)) 
    )
    dur_camp_exposure_cust_and_buyer.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().show()
    
    prior_pre_cust_buyer = cust_prior.join(cust_pre, 'household_id', 'outer').fillna(0)
    
    # brand buyer, exposed-unexposed cust x customer movement
    movement_and_exposure = \
    (dur_camp_exposure_cust_and_buyer
     .join(prior_pre_cust_buyer,'household_id', 'left')
     .withColumn('customer_group', F.when(F.col('pre_spending')>0,'existing')
                 .when(F.col('prior_spending')>0,'lapse').otherwise('new'))
    )
    
    movement_and_exposure.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.countDistinct('household_id')).show()
    
    # BRAND : uplift
    cust_uplift = \
    (movement_and_exposure
     .groupby('customer_group','exposed_flag','unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag')
     .agg(F.countDistinct('household_id').alias('customers'))
    )
    # find customer by group
    group_expose = cust_uplift.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_customers'))
    group_expose_buy = \
    (cust_uplift.filter(F.col('exposed_and_buy_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_shoppers')))

    group_unexpose = \
    (cust_uplift
     .filter( (F.col('exposed_flag')==0) & (F.col('unexposed_flag')==1) )
     .groupBy('customer_group').agg(F.sum('customers').alias('unexposed_customers'))
    )
    group_unexpose_buy = \
    (cust_uplift
     .filter(F.col('unexposed_and_buy_flag')==1)
     .groupBy('customer_group')
     .agg(F.sum('customers').alias('unexposed_shoppers'))
    )

    uplift = group_expose.join(group_expose_buy,'customer_group') \
                               .join(group_unexpose,'customer_group') \
                               .join(group_unexpose_buy,'customer_group')

    total_cust_uplift = (uplift
                         .agg(F.sum("exposed_customers").alias("exposed_customers"),
                              F.sum("exposed_shoppers").alias("exposed_shoppers"),
                              F.sum("unexposed_customers").alias("unexposed_customers"),
                              F.sum("unexposed_shoppers").alias("unexposed_shoppers"))
                         .withColumn("customer_group", F.lit("Total"))
                        )

    uplift_result = uplift.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
                          .withColumn('cvs_rate_test', F.col('exposed_shoppers')/F.col('exposed_customers'))\
                          .withColumn('cvs_rate_ctr', F.col('unexposed_shoppers')/F.col('unexposed_customers'))\
                          .withColumn('pct_uplift', F.col('cvs_rate_test')/F.col('cvs_rate_ctr') - 1 )\
                          .withColumn('uplift_cust',(F.col('cvs_rate_test')-F.col('cvs_rate_ctr'))*F.col('exposed_customers'))    
                          
    uplift_out = uplift_result.unionByName(total_cust_uplift, allowMissingColumns=True)
    
    return uplift_out

# COMMAND ----------

# MAGIC %md ##GoFresh : Customer Uplift - PromoWk

# COMMAND ----------

def get_customer_uplift_promo_wk(txn,
                        ctr_store_list, 
                        test_store_sf, 
                        cp_start_date,
                        cp_end_date,
                        adj_prod_sf, 
                        cust_uplift_lv,
                        switching_lv, 
                        feat_list):
    """customer id that 
    Expose (shop adjacency product during campaing in test store)
    Unexpose (shop adjacency product during campaing in control store)
    expose superseded unexpose
    """
    from pyspark.sql import functions as F
    print('Customer uplift with exposed/unexposed media at "OFFLINE"')
    print('Customer Uplift - Promo week')
    # ---- Section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    
    # brand of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
    txn_exposed = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .filter(F.col('period_promo_wk').isin('cmp'))
     .join(test_store_sf, 'store_id', 'inner')
    )
    print('Exposed customer (test store) from "OFFLINE" channel only')
    
    txn_unexposed = \
    (txn
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .filter(F.col('period_promo_wk').isin('cmp'))
     .filter(F.col('store_id').isin(ctr_store_list))
    )
    print('Unexposed customer (control store) from "OFFLINE" channel only')
    
    exposed_cust = \
    (txn_exposed
     .filter(F.col('household_id').isNotNull())
     .groupBy('household_id')
     .agg(F.min('date_id').alias('1st_exposed_date'))
     .withColumn('exposed_flag', F.lit(1))
    )

    unexposed_cust = \
    (txn_unexposed
     .filter(F.col('household_id').isNotNull())
     .groupBy('household_id')
     .agg(F.min('date_id').alias('1st_unexposed_date'))
     .withColumn('unexposed_flag', F.lit(1))
    )

    exposure_cust_table = exposed_cust.join(unexposed_cust, 'household_id', 'outer').fillna(0)
    
    #----------------------
    #---- uplift
    # brand buyer, during campaign
    print('-'*30)
    print(f'Customer uplift at {cust_uplift_lv.upper()} under {switching_lv.upper()} level')
    if cust_uplift_lv == 'brand':
        if switching_lv == "subclass":
        
            buyer_dur_campaign = \
            (txn
            .filter(F.col('period_promo_wk').isin(['cmp']))
            .filter(F.col('household_id').isNotNull())
            .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .groupBy('household_id')
            .agg(F.sum('net_spend_amt').alias('spending'),
                F.min('date_id').alias('1st_buy_date'))
            )
        
            # customer movement brand : prior - pre
            cust_prior = \
            (txn
             .filter(F.col('period_promo_wk').isin(['ppp']))
             .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
             .join(brand_of_feature_product, ['brand_name'])
             .filter(F.col('household_id').isNotNull())
             .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
            )

            cust_pre = \
            (txn
            .filter(F.col('period_promo_wk').isin(['pre']))
            .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
            )
            
        elif switching_lv == "class":
            buyer_dur_campaign = \
            (txn
            .filter(F.col('period_promo_wk').isin(['cmp']))
            .filter(F.col('household_id').isNotNull())
            .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
            .join(brand_of_feature_product, ['brand_name'])
            .groupBy('household_id')
            .agg(F.sum('net_spend_amt').alias('spending'),
                F.min('date_id').alias('1st_buy_date'))
            )
            
            # customer movement brand : prior - pre
            cust_prior = \
            (txn
             .filter(F.col('period_promo_wk').isin(['ppp']))
             .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
             .join(brand_of_feature_product, ['brand_name'])
             .filter(F.col('household_id').isNotNull())
             .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
            )

            cust_pre = \
            (txn
            .filter(F.col('period_promo_wk').isin(['pre']))
            .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
            .join(brand_of_feature_product, ['brand_name'])         
            .filter(F.col('household_id').isNotNull())
            .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
            )
        else:
            print(f"Switching parameter : {switching_lv} unknown")
        
    elif cust_uplift_lv == 'sku':

        buyer_dur_campaign = \
        (txn
         .filter(F.col('period_promo_wk').isin('cmp'))
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id')
         .agg(F.sum('net_spend_amt').alias('spending'),
              F.min('date_id').alias('1st_buy_date'))
        )
        # customer movement brand : prior - pre
        cust_prior = \
        (txn
         .filter(F.col('period_promo_wk').isin(['ppp']))
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
        )

        cust_pre = \
        (txn
         .filter(F.col('period_promo_wk').isin(['pre']))         
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('upc_id').isin(feat_list))
         .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
        )
        
    # brand buyer, exposed-unexposed cust
    dur_camp_exposure_cust_and_buyer = \
    (exposure_cust_table
     .join(buyer_dur_campaign, 'household_id', 'left')
     .withColumn('exposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNotNull() ) & \
                                                 (F.col('1st_buy_date').isNotNull() ) & \
                                                 (F.col('1st_exposed_date') <= F.col('1st_buy_date')), '1').otherwise(0))
     .withColumn('unexposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNull()) & \
                                                   (F.col('1st_unexposed_date').isNotNull()) & \
                                                   (F.col('1st_buy_date').isNotNull()) & \
                                                   (F.col('1st_unexposed_date') <= F.col('1st_buy_date')), '1').otherwise(0)) 
    )
    dur_camp_exposure_cust_and_buyer.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().show()
    
    prior_pre_cust_buyer = cust_prior.join(cust_pre, 'household_id', 'outer').fillna(0)
    
    # brand buyer, exposed-unexposed cust x customer movement
    movement_and_exposure = \
    (dur_camp_exposure_cust_and_buyer
     .join(prior_pre_cust_buyer,'household_id', 'left')
     .withColumn('customer_group', F.when(F.col('pre_spending')>0,'existing')
                 .when(F.col('prior_spending')>0,'lapse').otherwise('new'))
    )
    
    movement_and_exposure.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.countDistinct('household_id')).show()
    
    # BRAND : uplift
    cust_uplift = \
    (movement_and_exposure
     .groupby('customer_group','exposed_flag','unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag')
     .agg(F.countDistinct('household_id').alias('customers'))
    )
    # find customer by group
    group_expose = cust_uplift.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_customers'))
    group_expose_buy = \
    (cust_uplift.filter(F.col('exposed_and_buy_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_shoppers')))

    group_unexpose = \
    (cust_uplift
     .filter( (F.col('exposed_flag')==0) & (F.col('unexposed_flag')==1) )
     .groupBy('customer_group').agg(F.sum('customers').alias('unexposed_customers'))
    )
    group_unexpose_buy = \
    (cust_uplift
     .filter(F.col('unexposed_and_buy_flag')==1)
     .groupBy('customer_group')
     .agg(F.sum('customers').alias('unexposed_shoppers'))
    )

    uplift = group_expose.join(group_expose_buy,'customer_group') \
                               .join(group_unexpose,'customer_group') \
                               .join(group_unexpose_buy,'customer_group')

    total_cust_uplift = (uplift
                         .agg(F.sum("exposed_customers").alias("exposed_customers"),
                              F.sum("exposed_shoppers").alias("exposed_shoppers"),
                              F.sum("unexposed_customers").alias("unexposed_customers"),
                              F.sum("unexposed_shoppers").alias("unexposed_shoppers"))
                         .withColumn("customer_group", F.lit("Total"))
                        )

    uplift_result = uplift.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
                          .withColumn('cvs_rate_test', F.col('exposed_shoppers')/F.col('exposed_customers'))\
                          .withColumn('cvs_rate_ctr', F.col('unexposed_shoppers')/F.col('unexposed_customers'))\
                          .withColumn('pct_uplift', F.col('cvs_rate_test')/F.col('cvs_rate_ctr') - 1 )\
                          .withColumn('uplift_cust',(F.col('cvs_rate_test')-F.col('cvs_rate_ctr'))*F.col('exposed_customers'))                          

    uplift_out = uplift_result.unionByName(total_cust_uplift, allowMissingColumns=True)
    
    return uplift_out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get category average survival rate

# COMMAND ----------

def get_avg_cate_svv_bk(svv_df, cate_lvl, cate_code_list):
    """
    svv_df : Spark dataframe of survival rate table
    cate_lvl : string : catevory level 
    cate_code_list : list of category code that define brand category
    return cate_avg_survival_rate as sparkDataframe
    """
    if cate_lvl == 'subclass':
    
        svv  = svv_df.withColumn('subclass_code', concat(svv_df.division_id, lit('_'), svv_df.department_id, lit('_'), svv_df.section_id, lit('_'), svv_df.class_id, lit('_'), svv_df.subclass_id))
        ##cond = """subclass_code in ({}) """.format(cate_code_list)
        
        svv_cate = svv.where(svv.subclass_code.isin(cate_code_list))\
                      .select( svv.section_id.alias('sec_id')
                              ,svv.class_id
                              ,svv.subclass_code.alias('cate_code')
                              ,svv.subclass_name.alias('cate_name')
                              ,svv.brand_name
                              ,svv.CSR_13_wks.alias('q1')
                              ,svv.CSR_26_wks.alias('q2')
                              ,svv.CSR_39_wks.alias('q3')
                              ,svv.CSR_52_wks.alias('q4')
                              ,svv.spending.alias('sales')
                             )
    
    elif cate_lvl == 'class':
        svv  = svv_df.withColumn('class_code', concat(svv_df.division_id, lit('_'), svv_df.department_id, lit('_'), svv_df.section_id, lit('_'), svv_df.class_id))
        #cond = """class_code in ({}) """.format(cate_code_list)
        svv_cate = svv.where(svv.class_code.isin(cate_code_list))\
                       .select( svv.section_id.alias('sec_id')
                               ,svv.class_id
                               ,svv.class_code.alias('cate_code')
                               ,svv.class_name.alias('cate_name')
                               ,svv.brand_name
                               ,svv.CSR_13_wks.alias('q1')
                               ,svv.CSR_26_wks.alias('q2')
                               ,svv.CSR_39_wks.alias('q3')
                               ,svv.CSR_52_wks.alias('q4')
                               ,svv.spending.alias('sales')
                              )
    else:
        return None
    ## end if
    
    ## get weighted average svv by sale to subclass
    cate_sales = svv_cate.agg(sum(svv_cate.sales).alias('cate_sales')).collect()[0].cate_sales

    ## multiply for weighted    
    svv_cate   = svv_cate.withColumn('pct_share_w', svv_cate.sales/cate_sales)\
                         .withColumn('w_q1', svv_cate.q1 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q2', svv_cate.q2 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q3', svv_cate.q3 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q4', svv_cate.q4 * (svv_cate.sales/cate_sales))

    svv_cate_wg_avg = svv_cate.groupBy(svv_cate.cate_code)\
                          .agg    ( max(svv_cate.cate_name).alias('category_name')
                                   ,sum(svv_cate.w_q1).alias('wg_q1')
                                   ,sum(svv_cate.w_q2).alias('wg_q2')
                                   ,sum(svv_cate.w_q3).alias('wg_q3')
                                   ,sum(svv_cate.w_q4).alias('wg_q4')
                                  )
    
    return svv_cate_wg_avg

## end def    


# COMMAND ----------

def get_avg_cate_svv(svv_df, cate_lvl, cate_code_list):
    """
    svv_df : Spark dataframe of survival rate table
    cate_lvl : string : catevory level 
    cate_code_list : list of category code that define brand category
    return cate_avg_survival_rate as sparkDataframe
    
    ### add 3 more variable to use in case NPD and no survival rate at brand
    ## Pat 30 Jun 2022
    one_time_ratio
    spc_per_day
    AUC
    spc
    """
    if cate_lvl == 'subclass':
    
        svv  = svv_df.withColumn('subclass_code', concat(svv_df.division_id, lit('_'), svv_df.department_id, lit('_'), svv_df.section_id, lit('_'), svv_df.class_id, lit('_'), svv_df.subclass_id))
        ##cond = """subclass_code in ({}) """.format(cate_code_list)
        
        svv_cate = svv.where(svv.subclass_code.isin(cate_code_list))\
                      .select( svv.section_id.alias('sec_id')
                              ,svv.class_id
                              ,svv.subclass_code.alias('cate_code')
                              ,svv.subclass_name.alias('cate_name')
                              ,svv.brand_name
                              ,svv.CSR_13_wks.alias('q1')
                              ,svv.CSR_26_wks.alias('q2')
                              ,svv.CSR_39_wks.alias('q3')
                              ,svv.CSR_52_wks.alias('q4')
                              ,svv.spending.alias('sales')
                              ,svv.one_time_ratio.alias('otr')
                              ,svv.AUC.alias('auc')
                              ,svv.spc_per_day.alias('spd')
                             )
    
    elif cate_lvl == 'class':
        svv  = svv_df.withColumn('class_code', concat(svv_df.division_id, lit('_'), svv_df.department_id, lit('_'), svv_df.section_id, lit('_'), svv_df.class_id))
        #cond = """class_code in ({}) """.format(cate_code_list)
        svv_cate = svv.where(svv.class_code.isin(cate_code_list))\
                      .select( svv.section_id.alias('sec_id')
                              ,svv.class_id
                              ,svv.class_code.alias('cate_code')
                              ,svv.class_name.alias('cate_name')
                              ,svv.brand_name
                              ,svv.CSR_13_wks.alias('q1')
                              ,svv.CSR_26_wks.alias('q2')
                              ,svv.CSR_39_wks.alias('q3')
                              ,svv.CSR_52_wks.alias('q4')
                              ,svv.spending.alias('sales')
                              ,svv.one_time_ratio.alias('otr')
                              ,svv.AUC.alias('auc')
                              ,svv.spc_per_day.alias('spd')
                             )
    else:
        return None
    ## end if
    
    ## get weighted average svv by sale to subclass
    cate_sales = svv_cate.agg(sum(svv_cate.sales).alias('cate_sales')).collect()[0].cate_sales

    ## multiply for weighted    
    svv_cate   = svv_cate.withColumn('pct_share_w', svv_cate.sales/cate_sales)\
                         .withColumn('w_q1', svv_cate.q1 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q2', svv_cate.q2 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q3', svv_cate.q3 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_q4', svv_cate.q4 * (svv_cate.sales/cate_sales))\
                         .withColumn('w_otr',svv_cate.otr * (svv_cate.sales/cate_sales))\
                         .withColumn('w_auc',svv_cate.auc * (svv_cate.sales/cate_sales))\
                         .withColumn('w_spd',svv_cate.spd * (svv_cate.sales/cate_sales))

    svv_cate_wg_avg = svv_cate.groupBy(svv_cate.cate_code)\
                              .agg    ( max(svv_cate.cate_name).alias('category_name')
                                       ,lit(1).alias('CSR_0_wks')
                                       ,sum(svv_cate.w_q1).alias('CSR_13_wks_wavg')
                                       ,sum(svv_cate.w_q2).alias('CSR_26_wks_wavg')
                                       ,sum(svv_cate.w_q3).alias('CSR_39_wks_wavg')
                                       ,sum(svv_cate.w_q4).alias('CSR_52_wks_wavg')
                                       ,sum(svv_cate.w_otr).alias('one_time_ratio')
                                       ,sum(svv_cate.w_auc).alias('AUC')
                                       ,sum(svv_cate.w_spd).alias('spc_per_day')
                                      )
    
    return svv_cate_wg_avg

## end def    


# COMMAND ----------

# MAGIC %md ##CLTV

# COMMAND ----------

def get_customer_cltv( txn 
                      ,test_store_sf
                      ,adj_prod_id
                      ,lv_cltv
                      ,uplift_brand
                      ,media_spend
                      ,feat_list
                      ,svv_table
                      ,pcyc_table
                      ,cate_cd_list  ## add by Pat.  30 Jun 2022
                      ):
    """customer id that expose (shop adjacency product during campaing in test store)
    unexpose (shop adjacency product during campaing in control store)
    expose superseded unexpose
    """
    from pyspark.sql import functions as F
    print('Customer Life Time Value - Promo week')

    #---- Section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    ).persist()
    
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    ).persist()
    
    # brand of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    ).persist()
    
    #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
    print('Customer Life Time Value, based on "OFFLINE" exposed customer')
    txn_exposed = \
    (txn
     .filter(F.col('period_fis_wk').isin(['cmp']))
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .join(test_store_sf, 'store_id', 'inner')
     .join(adj_prod_id, 'upc_id', 'inner')
    ).persist()
    
    exposed_cust = \
    (txn_exposed
     .filter(F.col('household_id').isNotNull())
     .select('household_id')
     .drop_duplicates()
    )
    
    # During campaign brand buyer and exposed
    dur_campaign_brand_buyer_and_exposed = \
    (txn
     .filter(F.col('period_fis_wk').isin(['cmp']))
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
#          .filter(F.col('section_name')== sel_sec)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('brand_name')== sel_brand)
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .join(brand_of_feature_product, ['brand_name'])
     
     .join(exposed_cust, 'household_id', 'inner')
     .groupBy('household_id')
     .agg(F.sum('net_spend_amt').alias('spending'),
          F.countDistinct('transaction_uid').alias('visits'))
    ).persist()
    
    # get spend per customer for both multi and one time buyer
    spc_brand_mulibuy = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') > 1)
     .agg(F.avg('spending').alias('spc_multi'))
    )
    
    spc_brand_onetime = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') == 1)
     .agg(F.avg('spending').alias('spc_onetime'))
    )
    
    # get metrics
    spc_multi = spc_brand_mulibuy.select('spc_multi').collect()[0][0]
    spc_onetime = spc_brand_onetime.select('spc_onetime').collect()[0][0]
    print(f'Spend per customers (multi): {spc_multi}')
    print(f'Spend per customers (one time): {spc_onetime}')
    
    #----------------------
    #---- Level of CLVT, Purchase cycle
    
    svv_df = spark.table(svv_table)
    
    # import Customer survival rate
    lv_cltv = lv_cltv.lower()
    
    if lv_cltv.lower() == 'class':
        brand_csr_df   = svv_df.join(sec_id_class_id_feature_product, ['section_id', 'class_id'])\
                               .join(brand_of_feature_product, ['brand_name'])
                               
        brand_csr      = to_pandas(brand_csr_df)
                               
        cate_nm_lst    = brand_csr['class_name'].to_list()
        
    elif lv_cltv.lower() == 'subclass':
        brand_csr_df   = svv_df.join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])\
                               .join(brand_of_feature_product, ['brand_name'])
#                              .filter(F.col('brand_name')==sel_brand)\
#                              .filter(F.col('class_name')==sel_class)\
#                              .filter(F.col('subclass_name')==sel_subclass)

        brand_csr      = to_pandas(brand_csr_df)
                    
        cate_nm_lst    = brand_csr['subclass_name'].to_list()
        
    ## end if
    
    ## Add check brand SVV, incase NPD will not have Brand SVV, will need to use category information instead.
    brand_nm_lst = brand_of_feature_product.select(brand_of_feature_product.brand_name).dropDuplicates().toPandas()['brand_name'].to_list()
    brand_nm_txt = list2string(brand_nm_lst)
    
    if len(brand_csr) == 0:
        ## use category average instead (call function 'get_avg_cate_svv')
        brand_csr_df = get_avg_cate_svv(svv_df, lv_cltv, cate_cd_list)
        brand_csr    = brand_csr_df.toPandas()
        
        print('#'*80)
        print(' Warning !! Brand "' + str(brand_nm_txt) + '" has no survival rate, Will use category average survival rate instead.')
        print('#'*80)
        
        print(' Display brand_csr use \n')
        
        brand_csr.display()
        
        use_cate_svv_flag = 1
        use_average_flag  = 0
        
    elif len(brand_csr) > 1 :  ## case has multiple brand in SKU, will need to do average 
        
        brand_nm_txt = list2string(brand_nm_lst, ' and ')
        cate_nm_txt  = list2string(cate_nm_lst, ' and ')
        
        brand_nm_txt = str(brand_nm_txt)
        cate_nm_txt  = str(cate_nm_txt)
        
        print('#'*80)
        print(' Warning !! Feature SKUs are in multiple brand name/multiple category, will need to do average between all data.')
        print(' Brand Name = ' + str(brand_nm_txt) )
        print(' Category code = ' + str(cate_nm_txt) )
        print('#'*80)
        
        print(' Display brand_csr use (before average) \n')
        
        brand_csr.display()
        
        ## get weighted average svv by sale of all brand/category
        
        brand_sales = brand_csr_df.agg(sum(brand_csr_df.spending).alias('brand_sales')).collect()[0].brand_sales  ## sum sales value of all brand
        
        brand_csrw     = brand_csr_df.withColumn('pct_share_w', brand_csr_df.spending/brand_sales)\
                                     .withColumn('w_q1', brand_csr_df.CSR_13_wks * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_q2', brand_csr_df.CSR_26_wks * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_q3', brand_csr_df.CSR_39_wks * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_q4', brand_csr_df.CSR_52_wks * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_otr',brand_csr_df.one_time_ratio * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_auc',brand_csr_df.AUC            * (brand_csr_df.spending/brand_sales))\
                                     .withColumn('w_spd',brand_csr_df.spc_per_day    * (brand_csr_df.spending/brand_sales))
                       
        brand_csr_wdf  = brand_csrw.agg ( lit(cate_nm_txt).alias('category_name')
                                        ,lit(brand_nm_txt).alias('brand_name')
                                        ,lit(1).alias('CSR_0_wks')
                                        ,sum(brand_csrw.w_q1).alias('CSR_13_wks_wavg')
                                        ,sum(brand_csrw.w_q2).alias('CSR_26_wks_wavg')
                                        ,sum(brand_csrw.w_q3).alias('CSR_39_wks_wavg')
                                        ,sum(brand_csrw.w_q4).alias('CSR_52_wks_wavg')
                                        ,sum(brand_csrw.w_otr).alias('one_time_ratio')
                                        ,sum(brand_csrw.w_auc).alias('AUC')
                                        ,sum(brand_csrw.w_spd).alias('spc_per_day')
                                        ,avg(brand_csrw.spending).alias('average_spending')
                                        ,lit(brand_sales).alias('all_brand_spending')
                                       )
                                    
        brand_csr      = to_pandas(brand_csr_wdf)
        
        print(' Display brand_csr use (after average) \n')
        
        brand_csr.display()
        
        use_cate_svv_flag = 0
        use_average_flag  = 1
    else:
        use_cate_svv_flag = 0
        use_average_flag  = 0
        
        print(' Display brand_csr use \n')
        
        brand_csr.display()
        
 #   ## end if
  
    
    # import purchase_cycle table
    if lv_cltv.lower() == 'class':
        pc_table = to_pandas(spark.table(pcyc_table)\
                              .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
#                             .filter(F.col('class_name')==sel_class)
                            )

    elif lv_cltv.lower() == 'subclass':
        pc_table = to_pandas(spark.table(pcyc_table)\
                             .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
#                              .filter(F.col('class_name')==sel_class)\
#                              .filter(F.col('subclass_name')==sel_subclass)
                            )
    
    # CSR details
    brand_csr_graph = brand_csr[[c for c in brand_csr.columns if 'CSR' in c]].T
    brand_csr_graph.columns = ['survival_rate']
    brand_csr_graph.display()
    # pandas_to_csv_filestore(brand_csr_graph, 'brand_survival_rate.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    # ---- Uplift calculation
    total_uplift = uplift_brand.agg(F.sum('uplift_cust')).collect()[0][0]
    
    # Add total uplift adjust negative - 8 Jun 2022
    total_uplift_adj = uplift_brand.where(F.col('uplift_cust')>=0).agg(F.sum('uplift_cust')).collect()[0][0]
    if total_uplift_adj > total_uplift:
        print('Some customer group uplift have negative values, use uplift adj. negative to calculate CLTV')
        
    one_time_ratio = brand_csr.one_time_ratio[0]
    one_time_ratio = float(one_time_ratio)
    
    auc = brand_csr.AUC[0]
    cc_pen = pc_table.cc_penetration[0]
    spc_per_day = brand_csr.spc_per_day[0]
    
    # calculate CLTV 
    dur_cp_value = total_uplift* float(one_time_ratio) *float(spc_onetime) + total_uplift*(1-one_time_ratio)*float(spc_multi)
    post_cp_value = total_uplift* float(auc) * float(spc_per_day)
    cltv = dur_cp_value+post_cp_value
    epos_cltv = cltv/cc_pen
    print("EPOS CLTV: ",np.round(epos_cltv,2))
    
    #----- Break-even
    breakeven_df = brand_csr_graph.copy()
    # change first point to be one time ratio
    breakeven_df.loc['CSR_0_wks'] = one_time_ratio

    # calculate post_sales at each point of time in quarter value
    breakeven_df['post_sales'] = breakeven_df.survival_rate.astype(float) * total_uplift * float(spc_per_day) * 13*7  ## each quarter has 13 weeks and each week has 7 days
    
    breakeven_df.display()
    
    # calculate area under curve using 1/2 * (x+y) * a
    area = []
    for i in range(breakeven_df.shape[0]-1):
        area.append(0.5*(breakeven_df.iloc[i,1]+breakeven_df.iloc[i+1,1]))

    #create new data frame with value of sales after specific weeks
    breakeven_df2 = pd.DataFrame(area,index=[13,26,39,52],columns=['sales'])

    print("Display breakeven df area for starter : \n ")
    
    breakeven_df2.display()
    
    #set first day to be 0
    breakeven_df2.loc[0,'sales'] = 0
    breakeven_df2 = breakeven_df2.sort_index()

    #adding during campaign to every point in time
    breakeven_df2['sales+during'] = breakeven_df2+(dur_cp_value)

    #create accumulative columns
    breakeven_df2['acc_sales'] = breakeven_df2['sales+during'].cumsum()

    ## pat add
    print("Display breakeven table after add cumulative sales : \n ")
    breakeven_df2.display()
    
    #create epos value
    breakeven_df2['epos_acc_sales'] = breakeven_df2['acc_sales']/cc_pen.astype('float')
    breakeven_df2 = breakeven_df2.reset_index()
    breakeven_df2.round(2)
    
    #if use time more than 1 year, result show 1 year
    if media_spend > breakeven_df2.loc[4,'epos_acc_sales']:
        day_break = 'over one year'
    #else find point where it is breakeven
    else:
        index_break = breakeven_df2[breakeven_df2.epos_acc_sales>media_spend].index[0]
        
        # if it's in first period -> breakeven during campaing
        if index_break == 0:
            day_break = 0
            # else find lower bound and upper bound to calculate day left before break as a straight line  
        else:
            low_bound = breakeven_df2.loc[index_break-1,'epos_acc_sales']
            up_bound = breakeven_df2.loc[index_break,'epos_acc_sales']
            low_bound_day =  breakeven_df2.loc[index_break-1,'index']*13*7
            day_break = low_bound_day+((media_spend - low_bound) *13*7 / (up_bound-low_bound))

    if type(day_break) == int:
        breakeven_time_month = day_break//30
        breakeven_time_day = day_break%30
        print(f"Breakeven time: {int(breakeven_time_month)} months {int(breakeven_time_day)} days")
    else:
        print(f"Breakeven time: More than a year")
        breakeven_time_month = 'More than a year'
        breakeven_time_day = 'More than a year'
        
    # ---- Calculate Customer Acquisition Cost (CAC) : 8 Jun 2022
    cac = media_spend/total_uplift_adj
    
    #create data frame for save
    df_cltv = pd.DataFrame({'measures':['Total Uplift Customers',
                                        'Total Uplift Customer (adj. negative)',
                                        'One time ratio',
                                        'Brand SpC onetime-buyer',
                                        'Brand SpC multi-buyer',
                                        'AUC',
                                        'Spend per Customer per Day',
                                        'CLTV', 
                                        'CC Penetration',
                                        'Media Fee',
                                        'Customer Acquisition Cost (CAC)',
                                        'EPOS CLTV', 
                                        'Breakeven Month', 
                                        'Breakeven Day',
                                        'use_category_svv_flag',
                                        'use_average_cate_brand_flag'
                                       ],
                            'value':[total_uplift, 
                                     total_uplift_adj,
                                     one_time_ratio,
                                     spc_onetime,
                                     spc_multi,
                                     auc,
                                     spc_per_day,
                                     cltv,
                                     cc_pen,
                                     media_spend,
                                     cac,
                                     epos_cltv,
                                     breakeven_time_month,
                                     breakeven_time_day,
                                     use_cate_svv_flag,
                                     use_average_flag 
                                   ]
                           })
    df_cltv.round(2)

    return df_cltv, brand_csr_graph

# COMMAND ----------

def get_customer_cltv_backup(txn, 
#                       cp_start_date, cp_end_date, 
                      test_store_sf, adj_prod_id, 
#                       sel_sec, sel_class, sel_brand, 
                      lv_cltv,
                      uplift_brand, 
                      media_spend,
                      feat_list,
                      svv_table,
                      pcyc_table):
    """customer id that expose (shop adjacency product during campaing in test store)
    unexpose (shop adjacency product during campaing in control store)
    expose superseded unexpose
    """
    from pyspark.sql import functions as F
    print('Customer Life Time Value - Promo week')

    #---- Section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    
    # brand of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
    print('Customer Life Time Value, based on "OFFLINE" exposed customer')
    txn_exposed = \
    (txn
     .filter(F.col('period_fis_wk').isin(['cmp']))
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .join(test_store_sf, 'store_id', 'inner')
     .join(adj_prod_id, 'upc_id', 'inner')
    )
    
    exposed_cust = \
    (txn_exposed
     .filter(F.col('household_id').isNotNull())
     .select('household_id')
     .drop_duplicates()
    )
    
    # During campaign brand buyer and exposed
    dur_campaign_brand_buyer_and_exposed = \
    (txn
     .filter(F.col('period_fis_wk').isin(['cmp']))
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
#          .filter(F.col('section_name')== sel_sec)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('brand_name')== sel_brand)
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .join(brand_of_feature_product, ['brand_name'])
     
     .join(exposed_cust, 'household_id', 'inner')
     .groupBy('household_id')
     .agg(F.sum('net_spend_amt').alias('spending'),
          F.countDistinct('transaction_uid').alias('visits'))
    )
    
    # get spend per customer for both multi and one time buyer
    spc_brand_mulibuy = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') > 1)
     .agg(F.avg('spending').alias('spc_multi'))
    )
    
    spc_brand_onetime = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') == 1)
     .agg(F.avg('spending').alias('spc_onetime'))
    )
    
    # get metrics
    spc_multi = spc_brand_mulibuy.select('spc_multi').collect()[0][0]
    spc_onetime = spc_brand_onetime.select('spc_onetime').collect()[0][0]
    print(f'Spend per customers (multi): {spc_multi}')
    print(f'Spend per customers (one time): {spc_onetime}')
    
    #----------------------
    #---- Level of CLVT, Purchase cycle
    
    # import Customer survival rate
    if lv_cltv.lower() == 'class':
        brand_csr = to_pandas(spark.table(svv_table)\
                              .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
                              .join(brand_of_feature_product, ['brand_name'])
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class)
                             )
    elif lv_cltv.lower() == 'subclass':
        brand_csr = to_pandas(spark.table(svv_table)\
                              .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
                              .join(brand_of_feature_product, ['brand_name'])
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class)\
#                             .filter(F.col('subclass_name')==sel_subclass)
                             )

    # import purchase_cycle table
    if lv_cltv.lower() == 'class':
        pc_table = to_pandas(spark.table(pcyc_table)\
                              .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
#                             .filter(F.col('class_name')==sel_class)
                            )

    elif lv_cltv.lower() == 'subclass':
        pc_table = to_pandas(spark.table(pcyc_table)\
                             .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
#                              .filter(F.col('class_name')==sel_class)\
#                              .filter(F.col('subclass_name')==sel_subclass)
                            )
    
    # CSR details
    brand_csr_graph = brand_csr[[c for c in brand_csr.columns if 'CSR' in c]].T
    brand_csr_graph.columns = ['survival_rate']
    brand_csr_graph.display()
    # pandas_to_csv_filestore(brand_csr_graph, 'brand_survival_rate.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    # ---- Uplift calculation
    total_uplift = uplift_brand.agg(F.sum('uplift_cust')).collect()[0][0]
    
    # Add total uplift adjust negative - 8 Jun 2022
    total_uplift_adj = uplift_brand.where(F.col('uplift_cust')>=0).agg(F.sum('uplift_cust')).collect()[0][0]
    if total_uplift_adj > total_uplift:
        print('Some customer group uplift have negative values, use uplift adj. negative to calculate CLTV')
        
    one_time_ratio = brand_csr.one_time_ratio[0]
    auc = brand_csr.AUC[0]
    cc_pen = pc_table.cc_penetration[0]
    spc_per_day = brand_csr.spc_per_day[0]
    
    # calculate CLTV 
    dur_cp_value = total_uplift*one_time_ratio*float(spc_onetime) + total_uplift*(1-one_time_ratio)*float(spc_multi)
    post_cp_value = total_uplift*auc*spc_per_day
    cltv = dur_cp_value+post_cp_value
    epos_cltv = cltv/cc_pen
    print("EPOS CLTV: ",np.round(epos_cltv,2))
    
    #----- Break-even
    breakeven_df = brand_csr_graph.copy()
    # change first point to be one time ratio
    breakeven_df.loc['CSR_0_wks'] = one_time_ratio

    # calculate post_sales at each point of time in quarter value
    breakeven_df['post_sales'] = breakeven_df.survival_rate * total_uplift * spc_per_day * 13*7  ## each quarter has 13 weeks and each week has 7 days
    
    breakeven_df.display()
    
    # calculate area under curve using 1/2 * (x+y) * a
    area = []
    for i in range(breakeven_df.shape[0]-1):
        area.append(0.5*(breakeven_df.iloc[i,1]+breakeven_df.iloc[i+1,1]))

    #create new data frame with value of sales after specific weeks
    breakeven_df2 = pd.DataFrame(area,index=[13,26,39,52],columns=['sales'])

    print("Display breakeven df area for starter : \n ")
    
    breakeven_df2.display()
    
    #set first day to be 0
    breakeven_df2.loc[0,'sales'] = 0
    breakeven_df2 = breakeven_df2.sort_index()

    #adding during campaign to every point in time
    breakeven_df2['sales+during'] = breakeven_df2+(dur_cp_value)

    #create accumulative columns
    breakeven_df2['acc_sales'] = breakeven_df2['sales+during'].cumsum()

    ## pat add
    print("Display breakeven table after add cumulative sales : \n ")
    breakeven_df2.display()
    
    #create epos value
    breakeven_df2['epos_acc_sales'] = breakeven_df2['acc_sales']/cc_pen.astype('float')
    breakeven_df2 = breakeven_df2.reset_index()
    breakeven_df2.round(2)
    
    #if use time more than 1 year, result show 1 year
    if media_spend > breakeven_df2.loc[4,'epos_acc_sales']:
        day_break = 'over one year'
    #else find point where it is breakeven
    else:
        index_break = breakeven_df2[breakeven_df2.epos_acc_sales>media_spend].index[0]
        
        # if it's in first period -> breakeven during campaing
        if index_break == 0:
            day_break = 0
            # else find lower bound and upper bound to calculate day left before break as a straight line  
        else:
            low_bound = breakeven_df2.loc[index_break-1,'epos_acc_sales']
            up_bound = breakeven_df2.loc[index_break,'epos_acc_sales']
            low_bound_day =  breakeven_df2.loc[index_break-1,'index']*13*7
            day_break = low_bound_day+((media_spend - low_bound) *13*7 / (up_bound-low_bound))

    if type(day_break) == int:
        breakeven_time_month = day_break//30
        breakeven_time_day = day_break%30
        print(f"Breakeven time: {int(breakeven_time_month)} months {int(breakeven_time_day)} days")
    else:
        print(f"Breakeven time: More than a year")
        breakeven_time_month = 'More than a year'
        breakeven_time_day = 'More than a year'
        
    # ---- Calculate Customer Acquisition Cost (CAC) : 8 Jun 2022
    cac = media_spend/total_uplift_adj
    
    #create data frame for save
    df_cltv = pd.DataFrame({'measures':['Total Uplift Customers',
                                        'Total Uplift Customer (adj. negative)',
                                        'One time ratio',
                                        'Brand SpC onetime-buyer',
                                        'Brand SpC multi-buyer',
                                        'AUC',
                                        'Spend per Customer per Day',
                                        'CLTV', 
                                        'CC Penetration',
                                        'Media Fee',
                                        'Customer Acquisition Cost (CAC)',
                                        'EPOS CLTV', 
                                        'Breakeven Month', 
                                        'Breakeven Day'],
                            'value':[total_uplift, 
                                     total_uplift_adj,
                                     one_time_ratio,
                                     spc_onetime,
                                     spc_multi,
                                     auc,
                                     spc_per_day,
                                     cltv,
                                     cc_pen,
                                     media_spend,
                                     cac,
                                     epos_cltv,
                                     breakeven_time_month,
                                     breakeven_time_day
                                   ]
                           })
    df_cltv.round(2)

    return df_cltv, brand_csr_graph

# COMMAND ----------

# MAGIC %md ##GoFresh : CLTV-PromoWk

# COMMAND ----------

def get_customer_cltv_promo_wk(txn, 
#                       cp_start_date, cp_end_date, 
                      test_store_sf, 
#                                adj_prod_id, 
#                       sel_sec, sel_class, sel_brand, 
                      lv_cltv,
                      uplift_brand, 
                      media_spend,
                      feat_list,
                      svv_table,
                      pcyc_table):
    """customer id that expose (shop adjacency product during campaing in test store)
    unexpose (shop adjacency product during campaing in control store)
    expose superseded unexpose
    """
    from pyspark.sql import functions as F
    
    print('Customer Life Time Value - Promo week')
    #---- Section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    
    # brand of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
    print('Customer Life Time Value, based on "OFFLINE" exposed customer')
    txn_exposed = \
    (txn
     .filter(F.col('period_promo_wk').isin(['cmp']))
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('channel')=='OFFLINE') # for offline media
     .join(test_store_sf, 'store_id', 'inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
    )
    
    exposed_cust = \
    (txn_exposed
     .filter(F.col('household_id').isNotNull())
     .select('household_id')
     .drop_duplicates()
    )
    
    # During campaign brand buyer and exposed
    dur_campaign_brand_buyer_and_exposed = \
    (txn
     .filter(F.col('period_promo_wk').isin(['cmp']))
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
     .filter(F.col('household_id').isNotNull())
#          .filter(F.col('section_name')== sel_sec)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('brand_name')== sel_brand)
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .join(brand_of_feature_product, ['brand_name'])
     
     .join(exposed_cust, 'household_id', 'inner')
     .groupBy('household_id')
     .agg(F.sum('net_spend_amt').alias('spending'),
          F.countDistinct('transaction_uid').alias('visits'))
    )
    
    # get spend per customer for both multi and one time buyer
    spc_brand_mulibuy = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') > 1)
     .agg(F.avg('spending').alias('spc_multi'))
    )
    
    spc_brand_onetime = \
    (dur_campaign_brand_buyer_and_exposed
     .filter(F.col('visits') == 1)
     .agg(F.avg('spending').alias('spc_onetime'))
    )
    
    # get metrics
    spc_multi = spc_brand_mulibuy.select('spc_multi').collect()[0][0]
    spc_onetime = spc_brand_onetime.select('spc_onetime').collect()[0][0]
    print(f'Spend per customers (multi): {spc_multi}')
    print(f'Spend per customers (one time): {spc_onetime}')
    
    #----------------------
    #---- Level of CLVT, Purchase cycle
    
    # import Customer survival rate
    if lv_cltv.lower() == 'class':
        brand_csr = to_pandas(spark.table(svv_table)\
                              .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
                              .join(brand_of_feature_product, ['brand_name'])
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class)
                             )
    elif lv_cltv.lower() == 'subclass':
        brand_csr = to_pandas(spark.table(svv_table)\
                              .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
                              .join(brand_of_feature_product, ['brand_name'])
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class)\
#                             .filter(F.col('subclass_name')==sel_subclass)
                             )

    # import purchase_cycle table
    if lv_cltv.lower() == 'class':
        pc_table = to_pandas(spark.table(pcyc_table)\
                              .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
#                             .filter(F.col('class_name')==sel_class)
                            )

    elif lv_cltv.lower() == 'subclass':
        pc_table = to_pandas(spark.table(pcyc_table)\
                             .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
#                              .filter(F.col('class_name')==sel_class)\
#                              .filter(F.col('subclass_name')==sel_subclass)
                            )
    
    # CSR details
    brand_csr_graph = brand_csr[[c for c in brand_csr.columns if 'CSR' in c]].T
    brand_csr_graph.columns = ['survival_rate']
    brand_csr_graph.display()
    # pandas_to_csv_filestore(brand_csr_graph, 'brand_survival_rate.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    # ---- Uplift calculation
    total_uplift = uplift_brand.agg(F.sum('uplift_cust')).collect()[0][0]
    
    # Add total uplift adjust negative - 8 Jun 2022
    total_uplift_adj = uplift_brand.where(F.col('uplift_cust')>=0).agg(F.sum('uplift_cust')).collect()[0][0]
    if total_uplift_adj > total_uplift:
        print('Some customer group uplift have negative values, use uplift adj. negative to calculate CLTV')
        
    one_time_ratio = brand_csr.one_time_ratio[0]
    auc = brand_csr.AUC[0]
    cc_pen = pc_table.cc_penetration[0]
    spc_per_day = brand_csr.spc_per_day[0]
    
    # calculate CLTV 
    dur_cp_value = total_uplift*one_time_ratio*float(spc_onetime) + total_uplift*(1-one_time_ratio)*float(spc_multi)
    post_cp_value = total_uplift*auc*spc_per_day
    cltv = dur_cp_value+post_cp_value
    epos_cltv = cltv/cc_pen
    print("EPOS CLTV: ",np.round(epos_cltv,2))
    
    #----- Break-even
    breakeven_df = brand_csr_graph.copy()
    # change first point to be one time ratio
    breakeven_df.loc['CSR_0_wks'] = one_time_ratio

    # calculate post_sales at each point of time in quarter value
    breakeven_df['post_sales'] = breakeven_df.survival_rate * total_uplift * spc_per_day * 13*7  ## each quarter has 13 weeks and each week has 7 days
    
    breakeven_df.display()
    
    # calculate area under curve using 1/2 * (x+y) * a
    area = []
    for i in range(breakeven_df.shape[0]-1):
        area.append(0.5*(breakeven_df.iloc[i,1]+breakeven_df.iloc[i+1,1]))

    #create new data frame with value of sales after specific weeks
    breakeven_df2 = pd.DataFrame(area,index=[13,26,39,52],columns=['sales'])

    print("Display breakeven df area for starter : \n ")
    
    breakeven_df2.display()
    
    #set first day to be 0
    breakeven_df2.loc[0,'sales'] = 0
    breakeven_df2 = breakeven_df2.sort_index()

    #adding during campaign to every point in time
    breakeven_df2['sales+during'] = breakeven_df2+(dur_cp_value)

    #create accumulative columns
    breakeven_df2['acc_sales'] = breakeven_df2['sales+during'].cumsum()

    ## pat add
    print("Display breakeven table after add cumulative sales : \n ")
    breakeven_df2.display()
    
    #create epos value
    breakeven_df2['epos_acc_sales'] = breakeven_df2['acc_sales']/cc_pen.astype('float')
    breakeven_df2 = breakeven_df2.reset_index()
    breakeven_df2.round(2)
    
    #if use time more than 1 year, result show 1 year
    if media_spend > breakeven_df2.loc[4,'epos_acc_sales']:
        day_break = 'over one year'
    #else find point where it is breakeven
    else:
        index_break = breakeven_df2[breakeven_df2.epos_acc_sales>media_spend].index[0]
        
        # if it's in first period -> breakeven during campaing
        if index_break == 0:
            day_break = 0
            # else find lower bound and upper bound to calculate day left before break as a straight line  
        else:
            low_bound = breakeven_df2.loc[index_break-1,'epos_acc_sales']
            up_bound = breakeven_df2.loc[index_break,'epos_acc_sales']
            low_bound_day =  breakeven_df2.loc[index_break-1,'index']*13*7
            day_break = low_bound_day+((media_spend - low_bound) *13*7 / (up_bound-low_bound))

    if type(day_break) == int:
        breakeven_time_month = day_break//30
        breakeven_time_day = day_break%30
        print(f"Breakeven time: {int(breakeven_time_month)} months {int(breakeven_time_day)} days")
    else:
        print(f"Breakeven time: More than a year")
        breakeven_time_month = 'More than a year'
        breakeven_time_day = 'More than a year'
    
    # ---- Calculate Customer Acquisition Cost (CAC) : 8 Jun 2022
    cac = media_spend/total_uplift_adj
            
    #create data frame for save
    df_cltv = pd.DataFrame({'measures':['Total Uplift Customers',
                                        'Total Uplift Customer (adj. negative)',
                                        'One time ratio',
                                        'Brand SpC onetime-buyer',
                                        'Brand SpC multi-buyer',
                                        'AUC',
                                        'Spend per Customer per Day',
                                        'CLTV', 
                                        'CC Penetration',
                                        'Media Fee',
                                        'Customer Acquisition Cost (CAC)',
                                        'EPOS CLTV', 
                                        'Breakeven Month', 
                                        'Breakeven Day'],
                            'value':[total_uplift, 
                                     total_uplift_adj,
                                     one_time_ratio,
                                     spc_onetime,
                                     spc_multi,
                                     auc,
                                     spc_per_day,
                                     cltv,
                                     cc_pen,
                                     media_spend,
                                     cac,
                                     epos_cltv,
                                     breakeven_time_month,
                                     breakeven_time_day
                                   ]
                           })
    df_cltv.round(2)

    return df_cltv, brand_csr_graph

# COMMAND ----------

# MAGIC %md ##Sales Uplift

# COMMAND ----------

 def sales_uplift(txn, cp_start_date, cp_end_date, prior_start_date, pre_end_date,
                 sales_uplift_lv, 
                 sel_sec, sel_class, 
                 sel_brand, 
                 sel_sku,
                 matching_df, matching_methodology: str = 'varience'):
    """Support multi class, subclass
    
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    ## add pre period #weeks
    
    pre_days = pre_end_date - prior_start_date  ## result as timedelta
    pre_wk   = (pre_days.days + 1)/7  ## get # week
    
    print('----- Number of ppp+pre period = ' + str(pre_wk) + ' weeks. ----- \n')    
    
    #---- Filter Hierarchy with features products
    print('-'*20)
    print('Store matching v.2')
    features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    features_sku_sf = spark.createDataFrame(features_sku_df)
    features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    print('Multi-Section / Class / Subclass')
    (spark
     .table(TBL_PROD)
     .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
     .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
     .drop_duplicates()
     .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    ).show(truncate=False)
    print('-'*20)
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
        
    try:
        #print('In Try --- naja ')
        
        col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        matching_df_2 = matching_df[['store_id',col_for_store_id]].copy()
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        #print(test_ctr_store_id)
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        
        #print('---- test_store_id_pd - show data ----')
        #test_store_id_pd.display()
        
#         ctrl_store_id_pd = matching_df_2[['store_id_ctr']].drop_duplicates()\
#                                                           .assign(flag_store = 'cont')\
#                                                           .rename(columns={'store_id_ctr':'store_id'})
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

## end if

## ----- Backup code by Pat ---6 May 2022
#     #----------------------
#     #---- Uplift  
#     # brand buyer, during campaign
#     if sales_uplift_lv.lower() == 'brand':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at BRAND level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('brand_name')==sel_brand)
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('brand_name')==sel_brand)
#         )

#     elif sales_uplift_lv.lower() == 'sku':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at SKU level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )
############################################
     # ---------------------------------------------------------------
     ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
     ## need to get txn of target/control store separately to distinct #customers    
     # ---------------------------------------------------------------
     #---- Uplift
        
     # brand buyer, during campaign
    if sales_uplift_lv.lower() == 'brand':
        print('-'*30)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        prior_pre_txn_selected_prod_test_ctr_store = \
        (txn
         .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
         ## pat Add join on store list (control already duplicate here)
         .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')         
         .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('brand_name')==sel_brand)
         )        
        #prior_pre_txn_selected_prod_test_ctr_store.printSchema()
        
        dur_txn_selected_prod_test_ctr_store = \
        (txn
         .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
         ## pat Add join on store list (control already duplicate here)
         .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')
         .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('brand_name')==sel_brand)
        )
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*30)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        ## pre
        prior_pre_txn_selected_prod_test_ctr_store = \
        (txn
         ## pat Add join on store list (control already duplicate here)
         .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')
         .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('upc_id').isin(sel_sku))
        )
        ## during
        dur_txn_selected_prod_test_ctr_store = \
        (txn
         ## pat Add join on store list (control already duplicate here)
         #.join(broadcast(test_ctrl_store_df), [txn.store_id == test_ctrl_store_df.store_id] , 'inner')
         .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')
         .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('upc_id').isin(sel_sku))
        )

    #---- sales by store by period    EPOS
    sales_pre = prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    sales_dur = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)

    #---- sales by store by period clubcard
    #---- not use pre period as not use control factor
#    cc_sales_pre = prior_pre_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNulll())\
#                                                              .groupBy('store_id')\
#                                                              .agg(F.sum('net_spend_amt').alias('sales'))
    #dur_txn_selected_prod_test_ctr_store.printSchema()
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                     .groupBy(F.col('store_id'))\
                                                     .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                          ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                          ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                         )
#    cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = to_pandas(cc_kpi_dur)

#     #---- visits by store by period    
# #     cc_bask_pre = prior_pre_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNulll())\
# #                                                          .groupBy('store_id')\
# #                                                          .agg(F.countDistinct('transaction_uid').alias('bask'))
    
#     cc_bask_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNulll())\
#                                                    .groupBy('store_id')\
#                                                    .agg(F.countDistinct('transaction_uid').alias('cc_bask'))
    
# #     cc_bask_pre_df = to_pandas(cc_bask_pre)
#     cc_bask_dur_df = to_pandas(cc_bask_dur)
                                                                                   
#     #---- quantity (units) by store by period    
# #     cc_qty_pre = prior_pre_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNulll())\
# #                                                         .groupBy('store_id')\
# #                                                         .agg(F.sum('pkg_weight_unit').alias('qty'))
    
#     cc_qty_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNulll())\
#                                                   .groupBy('store_id')\
#                                                   .agg(F.sum('pkg_weight_unit').alias('cc_qty'))
# #    cc_qty_pre_df = to_pandas(cc_qty_pre)
#     cc_qty_dur_df = to_pandas(cc_qty_dur)

    
    ## create separate test/control txn table for #customers 
    
    #pre_txn_test_store = prior_pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = prior_pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    #---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ## will be count in both stores (as need to check market share & behaviour)

##   Pat Mark out pre-period as will not use control factor anymore
#     ## pre                                                                                   
#     cust_pre_test = pre_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].cust_test
#     cust_pre_ctrl = pre_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect()[0].cust_ctrl
    
#     ## create control factor to get value of expected customer in during period
#     cf_cust        = (cust_pre_test/cust_pre_ctrl)
#     ## try add control factor of avg weekly level
#     cf_cust_avg_wk = avg_test_pre_cust_wk/avg_ctrl_pre_cust_wk
    
    ## during period with by store (will duplicate number of customer by #times control store being use.)
    
    ## Test --> use the same way to count customers as control 
    ##          (count #customer per store and sum later --> customer will more than normal cust )
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = to_pandas(cust_dur_ctrl)
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
     
                                                                                
    #create weekly sales for each item
    wk_sales_pre = to_pandas(prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    # Prior-Pre sales
#     matching_df = matching_df_2.copy()
#     matching_df['test_pre_sum'] = matching_df_2[['store_id_test']].merge(wk_sales_pre,left_on='store_id_test',right_on='store_id').iloc[:,2:].sum(axis=1)
#     matching_df['ctr_pre_sum'] = matching_df_2[['store_id_ctr']].merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id').iloc[:,2:].sum(axis=1)
#     matching_df['ctr_factor'] = matching_df['test_pre_sum'] / matching_df['ctr_pre_sum']
#     matching_df.fillna({'ctr_factor':1},inplace=True)
#     matching_df.display()
    # Dur sales
#     matching_df['test_dur_sum'] = matching_df_2[['store_id_test']].merge(wk_sales_dur,left_on='store_id_test',right_on='store_id').iloc[:,2:].sum(axis=1)
#     matching_df['ctr_dur_sum'] = matching_df_2[['store_id_ctr']].merge(wk_sales_dur,left_on='store_id_ctr',right_on='store_id').iloc[:,2:].sum(axis=1)
#     matching_df['ctr_dur_sum_adjust'] = matching_df['ctr_dur_sum'] *  matching_df['ctr_factor']
#     matching_df

#### Back up code 6 May 2022 -- Pat 
#     matching_df = \
#     (matching_df_2
#      .merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
#      .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .assign(ctr_factor = lambda x : x['test_pre_sum'] / x['ctr_pre_sum'])
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
#      .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
#      .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
#     ).loc[:, ['store_id_test', 'store_id_ctr', 'test_pre_sum', 'ctr_pre_sum', 'ctr_factor', 'test_dur_sum', 'ctr_dur_sum', 'ctr_dur_sum_adjust']]

#### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
            
    matching_df = \
    (matching_df_2
     .merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
     .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
     #--
     .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
     #-- get control factor for all sales, unit,visits
     .assign(ctr_factor      = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])
     #--
     .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
     
     #-- during multiply control factor
     #-- sales
     .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
     .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
     .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
     
     #-- club card KPI
     .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                          ,'cc_bask':'cc_test_dur_bask'
                                                                                          ,'cc_qty':'cc_test_dur_qty'
                                                                                        })
     .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                         ,'cc_bask':'cc_ctr_dur_bask'
                                                                                         ,'cc_qty':'cc_ctr_dur_qty'
                                                                                          })
     
#      #-- cc basket
#      .merge(cc_bask_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'cc_bask':'cc_test_dur_bask'})
#      .merge(cc_bask_dur_df, left_on='store_id_ctr' , right_on='store_id').rename(columns={'cc_bask':'cc_ctr_dur_bask'})
     
#      #-- cc unit
#      .merge(cc_qty_dur_df, left_on ='store_id_test', right_on='store_id').rename(columns={'qty':'cc_test_dur_qty'})
#      .merge(cc_qty_dur_df, left_on ='store_id_ctr' , right_on='store_id').rename(columns={'qty':'cc_ctr_dur_qty'})
         
    ).loc[:, ['store_id_test', 'store_id_ctr', 'test_pre_sum', 'ctr_pre_sum', 'ctr_factor', 'test_dur_sum', 'ctr_dur_sum', 'ctr_dur_sum_adjust' 
                                             , 'cc_test_dur_sum', 'cc_ctr_dur_sum', 'cc_test_dur_bask', 'cc_ctr_dur_bask', 'cc_test_dur_qty', 'cc_ctr_dur_qty' ]]
    # -- sales uplift
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()                                                                                 
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()                                                                              
                                                                                     
    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    
    ## Get KPI   -- Pat 8 May 2022
    
    #customers calculated above already 
#     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
#     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
#     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test   = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl   = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                           
    sum_cc_bask_test     = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl     = matching_df.cc_ctr_dur_bask.sum()    
                                                                                   
    ## Unit
    sum_cc_qty_test     = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl     = matching_df.cc_ctr_dur_qty.sum()                                                                                                                                                                      

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var                                
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl                                                            
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'                              
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'                                                          
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
        
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

#     ctr_for_graph_final.display()
    
    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table

# COMMAND ----------

def sales_uplift_promo_wk(txn, cp_start_date, cp_end_date, prior_start_date, pre_end_date,
                 sales_uplift_lv, 
                 sel_sec, sel_class, 
                 sel_brand, 
                 sel_sku,
                 matching_df, matching_methodology: str = 'varience'):
    """
    
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    #---- Filter Hierarchy with features products
    print('-'*20)
    print('Store matching v.2')
    features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    features_sku_sf = spark.createDataFrame(features_sku_df)
    features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    print('Multi-Section / Class / Subclass')
    (spark
     .table(TBL_PROD)
     .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
     .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
     .drop_duplicates()
     .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    ).show(truncate=False)
    print('-'*20)
    
    #--- Map Promoweek_id
    promoweek_id = \
    (spark
     .table('tdm.v_date_dim')
     .select('date_id', 'promoweek_id')
    ).drop_duplicates()
     
    txn = txn.join(F.broadcast(promoweek_id), on='date_id', how='left')
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
    try:
        col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        matching_df_2 = matching_df[['store_id',col_for_store_id]].copy()
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        # matching_df_2.display()
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr']))
        
    except:
        print('Unrecognized store matching methodology')

    #----------------------
    #---- Uplift
    # brand buyer, during campaign
    if sales_uplift_lv.lower() == 'brand':
        print('-'*30)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        prior_pre_txn_selected_prod_test_ctr_store = \
        (txn
         .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
         .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('store_id').isin(test_ctr_store_id))
        .filter(F.col('brand_name')==sel_brand)
        )

        dur_txn_selected_prod_test_ctr_store = \
        (txn
         .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
         .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('store_id').isin(test_ctr_store_id))
         .filter(F.col('brand_name')==sel_brand)
        )
                 
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*30)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        prior_pre_txn_selected_prod_test_ctr_store = \
        (txn
         .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('store_id').isin(test_ctr_store_id))
         .filter(F.col('upc_id').isin(sel_sku))
        )

        dur_txn_selected_prod_test_ctr_store = \
        (txn
         .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('channel')=='OFFLINE')
         .filter(F.col('store_id').isin(test_ctr_store_id))
         .filter(F.col('upc_id').isin(sel_sku))
        )

    #---- sales by store by period    
    sales_pre = prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    sales_dur = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)
    
    #create weekly sales for each item
    wk_sales_pre = to_pandas(prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    matching_df = \
    (matching_df_2
     .merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
     .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
     .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
     .assign(ctr_factor = lambda x : x['test_pre_sum'] / x['ctr_pre_sum'])
     .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
     .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
     .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
     .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum']*x['ctr_factor'])
    ).loc[:, ['store_id_test', 'store_id_ctr', 'test_pre_sum', 'ctr_pre_sum', 'ctr_factor', 'test_dur_sum', 'ctr_dur_sum', 'ctr_dur_sum_adjust']]
    
    sales_uplift = matching_df.test_dur_sum.sum() - matching_df.ctr_dur_sum_adjust.sum()
    pct_sales_uplift = (sales_uplift / matching_df.ctr_dur_sum_adjust.sum()) 
    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    #---- Weekly sales for plotting
    # Test store
    test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]
                  .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')
                  .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']].merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id').merge(wk_sales_dur,on='store_id')
    
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
        
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns
    
    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift

# COMMAND ----------

##----------------------------------------------------------------------------
## Pat Add sales uplift by region on Top of sales uplift function
## Initial : 25 May 2022
## detail adjust :
## > Use only pre-period (13 weeks) for uplift/control factor 
## > add uplift by region
## > Expect to have "store_region" in store matching table already
##----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Uplift Region

# COMMAND ----------

print('Sales uplift region def')

# COMMAND ----------

##----------------------------------------------------------------------------
## Pat Add sales uplift by region on Top of sales uplift function
## Initial : 25 May 2022
## detail adjust :
## > Use only pre-period (13 weeks) for uplift/control factor 
## > add uplift by region
## > Expect to have "store_region" in store matching table already
##----------------------------------------------------------------------------

def sales_uplift_reg(txn, 
                     sales_uplift_lv, 
                     brand_df,
                     feat_list,
                     matching_df, 
                     matching_methodology: str = 'varience'):
    """Support multi class, subclass
    change input paramenter to use brand_df and feat_list
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    ## add pre period #weeks  -- Pat comment out
    
    #pre_days = pre_end_date - pre_start_date  ## result as timedelta
    #pre_wk   = (pre_days.days + 1)/7  ## get number of weeks
    #
    #print('----- Number of ppp+pre period = ' + str(pre_wk) + ' weeks. ----- \n')    
    
    
    ##---- Filter Hierarchy with features products
    #print('-'*20)
    #print('Store matching v.2')
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    #print('Multi-Section / Class / Subclass')
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*20)
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
        
    try:
        #print('In Try --- naja ')
        
        col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        matching_df_2 = matching_df[['store_id','store_region', col_for_store_id]].copy()
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

    ## end Try 

     # ---------------------------------------------------------------
     ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
     ## need to get txn of target/control store separately to distinct #customers    
     # ---------------------------------------------------------------
     #---- Uplift
        
     # brand buyer, during campaign
    if sales_uplift_lv.lower() == 'brand':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        brand_txn = txn.join(brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                       .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                       .where((txn.channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp'))
                             )

        ## get transcation at brand level for pre period
        pre_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'pre')
        
        ## get transcation at brand level for dur period
        dur_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'cmp')
        
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        feat_txn = txn.join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                      .where( (txn.channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp')) &
                              (txn.upc_id.isin(feat_list))
                            )
        
        ## pre
        pre_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'pre')
        #print(' Check Display(10) pre txn below \n')
        #pre_txn_selected_prod_test_ctr_store.display(10)
        ## during
        dur_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'cmp')
        #print(' Check Display(10) during txn below \n')
        #dur_txn_selected_prod_test_ctr_store.display(10)
    ## end if

    #---- sales by store by period    EPOS
    sales_pre    = pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    ## region is in matching table already 
    sales_dur    = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)

    #---- sales by store by period clubcard
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                     .groupBy(F.col('store_id'))\
                                                     .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                          ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                          ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                         )
#    cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = to_pandas(cc_kpi_dur)

    ## create separate test/control txn table for #customers -- Pat May 2022
    
    #pre_txn_test_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    #---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ## will be count in both stores (as need to check market share & behaviour)
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = to_pandas(cust_dur_ctrl)
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
     
                                                                                
    #create weekly sales for each item
    wk_sales_pre = to_pandas(pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    #### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
    
    ## Replace matching_df Pat add region in matching_df -- May 2022
    
    matching_df = (matching_df_2.merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
                                .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                #-- get control factor for all sales, unit,visits
                                .assign(ctr_factor      = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                
                                #-- during multiply control factor
                                #-- sales
                                .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
                                .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
                                .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
                                
                                #-- club card KPI
                                .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                                                     ,'cc_bask':'cc_test_dur_bask'
                                                                                                                     ,'cc_qty':'cc_test_dur_qty'
                                                                                                                   })
                                .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                                       ,'cc_bask':'cc_ctr_dur_bask'
                                                                                                       ,'cc_qty':'cc_ctr_dur_qty'
                                                                                                        })
                 ).loc[:, [ 'store_id_test'
                           ,'store_id_ctr'
                           , 'store_region'
                           , 'test_pre_sum'
                           , 'ctr_pre_sum'
                           , 'ctr_factor'
                           , 'test_dur_sum'
                           , 'ctr_dur_sum'
                           , 'ctr_dur_sum_adjust' 
                           , 'cc_test_dur_sum'
                           , 'cc_ctr_dur_sum'
                           , 'cc_test_dur_bask'
                           , 'cc_ctr_dur_bask'
                           , 'cc_test_dur_qty'
                           , 'cc_ctr_dur_qty' ]]
    ##-----------------------------------------------------------
    ## -- Sales uplift at overall level  
    ##-----------------------------------------------------------
    
    # -- sales uplift overall
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()

    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    ##-----------------------------------------------------------
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Region -- 24 May 2022
    ##-----------------------------------------------------------
    # -- sales uplift by region
    #df4 = df.groupby('Courses', sort=False).agg({'Fee': ['sum'], 'Courses':['count']}).reset_index()
    #df4.columns = ['course', 'c_fee', 'c_count']
    
    ## add sort = false to optimize performance only
    
    sum_sales_test_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'test_dur_sum':['sum'], 'store_region':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_region         = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_region.columns = ['store_region', 'trg_sales_reg', 'trg_store_cnt', 'ctr_sales_reg', 'ctr_sales_adj_reg']
    
    ## get uplift region
    uplift_region['s_uplift_reg'] = uplift_region['trg_sales_reg'] - uplift_region['ctr_sales_adj_reg']
    uplift_region['pct_uplift']   = uplift_region['s_uplift_reg'] / uplift_region['ctr_sales_adj_reg']

    print('\n' + '-'*80 + '\n Uplift by Region at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_region.display()
    ## ------------------------------------------------------------
    
    
    ## Get KPI   -- Pat 8 May 2022
    
    #customers calculated above already 
#     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
#     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
#     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test   = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl   = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                           
    sum_cc_bask_test     = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl     = matching_df.cc_ctr_dur_bask.sum()    
                                                                                   
    ## Unit
    sum_cc_qty_test     = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl     = matching_df.cc_ctr_dur_qty.sum() 

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl 
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
        
    ## end for
    
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
    
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,
                             columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    #sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table, uplift_region
## End def    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uplift Region Promo Week

# COMMAND ----------

##----------------------------------------------------------------------------
## Pat Add sales uplift by region on Top of sales uplift function
## Initial : 25 May 2022
## detail adjust :
## > Use only pre-period (13 weeks) for uplift/control factor 
## > add uplift by region
## > Expect to have "store_region" in store matching table already
##----------------------------------------------------------------------------

def sales_uplift_promo_reg(txn, 
                           sales_uplift_lv, 
                           brand_df,
                           feat_list,
                           matching_df, 
                           period_col:str = 'period_promo_wk',
                           matching_methodology: str = 'varience'):
    """Support multi class, subclass
    change input paramenter to use brand_df and feat_list
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    ## add pre period #weeks  -- Pat comment out
    
    #pre_days = pre_end_date - pre_start_date  ## result as timedelta
    #pre_wk   = (pre_days.days + 1)/7  ## get number of weeks
    #
    #print('----- Number of ppp+pre period = ' + str(pre_wk) + ' weeks. ----- \n')    
    
    
    ##---- Filter Hierarchy with features products
    #print('-'*20)
    #print('Store matching v.2')
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    #print('Multi-Section / Class / Subclass')
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*20)
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
        
    try:
        #print('In Try --- naja ')
        
        col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        matching_df_2 = matching_df[['store_id','store_region', col_for_store_id]].copy()  ## Pat Add store_region - May 2022
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

    ## end Try 

     # ---------------------------------------------------------------
     ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
     ## need to get txn of target/control store separately to distinct #customers    
     # ---------------------------------------------------------------
     #---- Uplift
    
    feat_txt       = list2string(feat_list, ',')
    
    ## For promo, sales uplift using difference column for 13 weeks uplift >> 'period_promo_mv_wk' will be pass as inputs
    ## For KPI will still use normal promoweek_id column which will have pre period week = during period week
        
    txn_main_cond  = """ ({0} in ('pre', 'cmp')) and 
                         ( channel == 'OFFLINE')
                     """.format(period_col)
                   
    txn_pre_cond   = """ ({0} == 'pre') """.format(period_col)
    txn_dur_cond   = """ ({0} == 'cmp') """.format(period_col)
    
    txn_feat_cond  = """ ({0} in ('pre', 'cmp')) and 
                         ( channel == 'OFFLINE') and 
                         ( upc_id in ({1}) )
                     """.format(period_col, feat_txt)
    
    print(' txn_feat_cond = ' + txn_feat_cond + '\n')
    
     # brand buyer, during campaign
    if sales_uplift_lv.lower() == 'brand':
        print('-'*80)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        brand_txn = txn.join(brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                       .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                       .where(txn_main_cond)

        ## get transcation at brand level for pre period
        pre_txn_selected_prod_test_ctr_store     = brand_txn.where(txn_pre_cond)
                
        ## get transcation at brand level for dur period
        dur_txn_selected_prod_test_ctr_store     = brand_txn.where(txn_dur_cond)
                
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*80)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        
        ## get transcation at feat level for all period  - Pat 25 May 2022
        
        feat_txn     = txn.join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                          .where( txn_feat_cond )
                             
        ## pre
        pre_txn_selected_prod_test_ctr_store     = feat_txn.where(txn_pre_cond)

        ## during
        dur_txn_selected_prod_test_ctr_store     = feat_txn.where(txn_dur_cond)

    ## end if

    #---- sales by store by period    EPOS
    sales_pre    = pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    ## region is in matching table already 
    sales_dur    = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)
    
    #---- sales by store by period clubcard -- for KPI
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                         .groupBy(F.col('store_id'))\
                                                         .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                              ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                              ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                             )
#   # cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = to_pandas(cc_kpi_dur)

    ## create separate test/control txn table for #customers -- Pat May 2022
    
    #pre_txn_test_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    ##---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ### will be count in both stores (as need to check market share & behaviour)
    ## KPI
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = to_pandas(cust_dur_ctrl)
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    print('=' * 80)
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    print('=' * 80)
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
     
                                                                                
    #create weekly sales for each item
    # period_col
    wk_sales_pre = to_pandas(pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    print('=' * 80)
    print(' Display wk_sales_pre' )
    
    wk_sales_pre.display()
    
    print('=' * 80)
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    #### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
    
    ## Replace matching_df - Pat add region in matching_df -- May 2022
    
    matching_df = (matching_df_2.merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
                                .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                #-- get control factor for all sales, unit,visits
                                .assign(ctr_factor = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])  ## control factor per store across all pre-period (not by week)
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                
                                #-- during multiply control factor
                                #-- sales
                                .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
                                .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
                                .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
                                
                                #-- club card KPI
                                .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                                                     ,'cc_bask':'cc_test_dur_bask'
                                                                                                                     ,'cc_qty':'cc_test_dur_qty'
                                                                                                                   })
                                .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                                       ,'cc_bask':'cc_ctr_dur_bask'
                                                                                                       ,'cc_qty':'cc_ctr_dur_qty'
                                                                                                        })
                 ).loc[:, [ 'store_id_test'
                           ,'store_id_ctr'
                           , 'store_region'
                           , 'test_pre_sum'
                           , 'ctr_pre_sum'
                           , 'ctr_factor'
                           , 'test_dur_sum'
                           , 'ctr_dur_sum'
                           , 'ctr_dur_sum_adjust' 
                           , 'cc_test_dur_sum'
                           , 'cc_ctr_dur_sum'
                           , 'cc_test_dur_bask'
                           , 'cc_ctr_dur_bask'
                           , 'cc_test_dur_qty'
                           , 'cc_ctr_dur_qty' ]]  
                           
    ##-----------------------------------------------------------
    ## -- Sales uplift at overall level  
    ##-----------------------------------------------------------
    
    # -- sales uplift overall
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()

    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    ##-----------------------------------------------------------
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Region -- 24 May 2022
    ##-----------------------------------------------------------
    # -- sales uplift by region
    #df4 = df.groupby('Courses', sort=False).agg({'Fee': ['sum'], 'Courses':['count']}).reset_index()
    #df4.columns = ['course', 'c_fee', 'c_count']
    
    ## add sort = false to optimize performance only
    
    sum_sales_test_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'test_dur_sum':['sum'], 'store_region':['count']})
    sum_sales_ctrl_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_region         = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_region.columns = ['store_region', 'trg_sales_reg', 'trg_store_cnt', 'ctr_sales_reg', 'ctr_sales_adj_reg']
    
    ## get uplift region
    uplift_region['s_uplift_reg'] = uplift_region['trg_sales_reg'] - uplift_region['ctr_sales_adj_reg']
    uplift_region['pct_uplift']   = uplift_region['s_uplift_reg'] / uplift_region['ctr_sales_adj_reg']

    print('\n' + '-'*80 + '\n Uplift by Region at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_region.display()
    ## ------------------------------------------------------------
    
    
#    ## Get KPI   -- Pat 8 May 2022
    
#    #customers calculated above already 
##     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
##     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
##     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                         
    sum_cc_bask_test  = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl  = matching_df.cc_ctr_dur_bask.sum()    
                                                                                 
    ## Unit
    sum_cc_qty_test   = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl   = matching_df.cc_ctr_dur_qty.sum() 

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl 
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
        
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    #print('Showing : ctr_for_graph.display() before ')    
    #ctr_for_graph.display()
    
    for c in ctr_for_graph.columns[3:]:
        new_c                = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
    
    ## end for
    
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
        
    #ctr_for_graph_2.display()
    
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

#     ctr_for_graph_final.display()
    
    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    #sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table, uplift_region
## End def    

# COMMAND ----------

##----------------------------------------------------------------------------
## Pat Add sales uplift by region on Top of sales uplift function
## Initial : 25 May 2022
## detail adjust :
## > Use only pre-period (13 weeks) for uplift/control factor 
## > add uplift by region
## > Expect to have "store_region" in store matching table already
##----------------------------------------------------------------------------

def sales_uplift_promo_reg_backup(txn, 
                                  sales_uplift_lv, 
                                  brand_df,
                                  feat_list,
                                  matching_df, 
                                  matching_methodology: str = 'varience'):
    """Support multi class, subclass
    change input paramenter to use brand_df and feat_list
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    ## add pre period #weeks  -- Pat comment out
    
    #pre_days = pre_end_date - pre_start_date  ## result as timedelta
    #pre_wk   = (pre_days.days + 1)/7  ## get number of weeks
    #
    #print('----- Number of ppp+pre period = ' + str(pre_wk) + ' weeks. ----- \n')    
    
    
    ##---- Filter Hierarchy with features products
    #print('-'*20)
    #print('Store matching v.2')
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    #print('Multi-Section / Class / Subclass')
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*20)
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
        
    try:
        #print('In Try --- naja ')
        
        col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        matching_df_2 = matching_df[['store_id','store_region', col_for_store_id]].copy()  ## Pat Add store_region - May 2022
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

    ## end Try 

     # ---------------------------------------------------------------
     ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
     ## need to get txn of target/control store separately to distinct #customers    
     # ---------------------------------------------------------------
     #---- Uplift
        
     # brand buyer, during campaign
    if sales_uplift_lv.lower() == 'brand':
        print('-'*80)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        brand_txn = txn.join(brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                       .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                       .where((txn.channel =='OFFLINE') &
                              (txn.period_promo_wk.isin('pre','cmp'))   ## change to period_promo_wk for uplift by promo week
                             )

        ## get transcation at brand level for pre period
        pre_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_promo_wk == 'pre')
        
        ## get transcation at brand level for dur period
        dur_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_promo_wk == 'cmp')
        
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*80)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        
        ## get transcation at feat level for all period  - Pat 25 May 2022
        
        feat_txn = txn.join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                      .where( (txn.channel =='OFFLINE') &
                              (txn.period_promo_wk.isin('pre','cmp')) &
                              (txn.upc_id.isin(feat_list))
                            )
        
        ## pre
        pre_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_promo_wk == 'pre')

        ## during
        dur_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_promo_wk == 'cmp')

    ## end if

    #---- sales by store by period    EPOS
    sales_pre    = pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    ## region is in matching table already 
    sales_dur    = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)

    #---- sales by store by period clubcard -- for KPI
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                     .groupBy(F.col('store_id'))\
                                                     .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                          ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                          ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                         )
#   # cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = to_pandas(cc_kpi_dur)

    ## create separate test/control txn table for #customers -- Pat May 2022
    
    #pre_txn_test_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    ##---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ### will be count in both stores (as need to check market share & behaviour)
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = to_pandas(cust_dur_ctrl)
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
     
                                                                                
    #create weekly sales for each item
    wk_sales_pre = to_pandas(pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    #### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
    
    ## Replace matching_df - Pat add region in matching_df -- May 2022
    
    matching_df = (matching_df_2.merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
                                .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                #-- get control factor for all sales, unit,visits
                                .assign(ctr_factor = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])  ## control factor per store across all pre-period (not by week)
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                
                                #-- during multiply control factor
                                #-- sales
                                .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
                                .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
                                .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
                                
                                #-- club card KPI
                                .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                                                     ,'cc_bask':'cc_test_dur_bask'
                                                                                                                     ,'cc_qty':'cc_test_dur_qty'
                                                                                                                   })
                                .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                                       ,'cc_bask':'cc_ctr_dur_bask'
                                                                                                       ,'cc_qty':'cc_ctr_dur_qty'
                                                                                                        })
                 ).loc[:, [ 'store_id_test'
                           ,'store_id_ctr'
                           , 'store_region'
                           , 'test_pre_sum'
                           , 'ctr_pre_sum'
                           , 'ctr_factor'
                           , 'test_dur_sum'
                           , 'ctr_dur_sum'
                           , 'ctr_dur_sum_adjust' 
                           , 'cc_test_dur_sum'
                           , 'cc_ctr_dur_sum'
                           , 'cc_test_dur_bask'
                           , 'cc_ctr_dur_bask'
                           , 'cc_test_dur_qty'
                           , 'cc_ctr_dur_qty' ]]  
                           
    ##-----------------------------------------------------------
    ## -- Sales uplift at overall level  
    ##-----------------------------------------------------------
    
    # -- sales uplift overall
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()

    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    ##-----------------------------------------------------------
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Region -- 24 May 2022
    ##-----------------------------------------------------------
    # -- sales uplift by region
    #df4 = df.groupby('Courses', sort=False).agg({'Fee': ['sum'], 'Courses':['count']}).reset_index()
    #df4.columns = ['course', 'c_fee', 'c_count']
    
    ## add sort = false to optimize performance only
    
    sum_sales_test_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'test_dur_sum':['sum'], 'store_region':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_region         = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_region.columns = ['store_region', 'trg_sales_reg', 'trg_store_cnt', 'ctr_sales_reg', 'ctr_sales_adj_reg']
    
    ## get uplift region
    uplift_region['s_uplift_reg'] = uplift_region['trg_sales_reg'] - uplift_region['ctr_sales_adj_reg']
    uplift_region['pct_uplift']   = uplift_region['s_uplift_reg'] / uplift_region['ctr_sales_adj_reg']

    print('\n' + '-'*80 + '\n Uplift by Region at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_region.display()
    ## ------------------------------------------------------------
    
    
#    ## Get KPI   -- Pat 8 May 2022
    
#    #customers calculated above already 
##     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
##     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
##     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test   = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl   = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                           
    sum_cc_bask_test     = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl     = matching_df.cc_ctr_dur_bask.sum()    
                                                                                   
    ## Unit
    sum_cc_qty_test     = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl     = matching_df.cc_ctr_dur_qty.sum() 

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl 
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
        
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    #print('Showing : ctr_for_graph.display() before ')    
    #ctr_for_graph.display()
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
    
    ## end for
    
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
        
    #ctr_for_graph_2.display()
    
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

#     ctr_for_graph_final.display()
    
    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    #sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table, uplift_region
## End def    

# COMMAND ----------

# MAGIC %md ##PromoZone Eval : Customer Switching

# COMMAND ----------

def cust_movement_promo_wk_prmzn(switching_lv, 
                                 txn, 
                                 cp_start_date, 
                                 cp_end_date, 
                                 brand_df,
                                 test_store_sf, 
                                 feat_list):
    """Media evaluation solution, Customer movement and switching v3 - PromoZone
    - Activated shopper = feature product shopper at test store
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    print('Customer movement for PromoZone Evaluation')
    print('Customer movement for "OFFLINE" + "ONLINE"')
    print("Use stampped period column 'period_promo_mv_wk'")
    
    #---- Get scope for brand in class / brand in subclass
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Get list of feature brand name
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
        
    #---- During camapign - exposed customer, 
    dur_campaign_exposed_cust_and_brand_shopper = \
    (txn
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     .join(brand_df, 'upc_id', "inner")
     .select('household_id')
     .drop_duplicates()
    )
    
    activated_brand = dur_campaign_exposed_cust_and_brand_shopper.count()
    print(f'Total exposed and brand shopper (Activated Brand) : {activated_brand}')    
    
    #---- During campaign - Exposed & Features SKU shopper    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (txn
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )

    activated_sku = dur_campaign_exposed_cust_and_sku_shopper.count()
    print(f'Total exposed and sku shopper (Activated SKU) : {activated_sku}')    
    
    activated_df = pd.DataFrame({'customer_exposed_brand_activated':[activated_brand], 'customer_exposed_sku_activated':[activated_sku]})
    
    #---- Find Customer movement from (PPP+PRE) -> CMP period
    
    # Existing and New SKU buyer (movement at micro level)
    prior_pre_sku_shopper = \
    (txn
     .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp']))
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )
    
    existing_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
    )
    
    existing_exposed_cust_and_sku_shopper = existing_exposed_cust_and_sku_shopper.checkpoint()
    
    new_exposed_cust_and_sku_shopper = \
    (dur_campaign_exposed_cust_and_sku_shopper
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
    )
    new_exposed_cust_and_sku_shopper = new_exposed_cust_and_sku_shopper.checkpoint()
        
    #---- Movement macro level for New to SKU
    prior_pre_cc_txn = \
    (txn
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
     .select('household_id')
    ).drop_duplicates()
    
    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
     .select('household_id')
    ).drop_duplicates()
        
    #---- Grouping, flag customer macro flag
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )
    
    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )
    
    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )
        
        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()
        
        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    elif switching_lv == 'class':
        
        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
         .join(brand_of_feature_product, ['brand_name'])
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
        )
        result_movement = result_movement.checkpoint()
        return result_movement, new_exposed_cust_and_sku_shopper, activated_df

    else:
        print('Not recognized Movement and Switching level param')
        return None

# COMMAND ----------

def cust_switching_promo_wk_prmzn(switching_lv, 
                                   cust_movement_sf,
                                   txn, 
                                   feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as sparkDataFrame
    
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    print('Customer switching for "OFFLINE" + "ONLINE"')
    print("For PromoZone eval, use col 'period_promo_mv_wk'")
    
    #---- switching fn from Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: sparkDataFrame, prod_trans: sparkDataFrame, grp: List, 
                   prod_lev: str, full_prod_lev: str , col_rename: str, period: str) -> sparkDataFrame:
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
#         print('cust_micro_df')
#         cust_micro_df.display()
        # Transaction of focus customer group
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )
#         print('cust_mirco_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        # total of other brand/subclass/class spending
        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]
        
        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))
        
#         print('cust_micro_kpi_prod_lv')
#         cust_micro_kpi_prod_lv.display()
        
        print("\t\t\t\t\t\t**Running micro df2")

#         print('cust_micro_df2')
#         cust_micro_df2.display()

        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
#                       'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )            
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
        elif prod_lev == 'class':
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        
        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name','class_name','original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
         .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(), F.col('pct_spend_oth_'+full_prod_lev).desc())
        )
        # display(micro_df_summ)
        switching_result = switching_result.checkpoint()
        
        return switching_result
    
    #---- Main
    # Section id, class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    # Brand name of feature products
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
    
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement
        prior_pre_cc_txn_subcl = \
        (txn
         .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp']))
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        prior_pre_cc_txn_subcl_sel_brand = prior_pre_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_subcl_sel_brand_kpi = \
        (prior_pre_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_subcl = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_promo_mv_wk').isin(['cmp']))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
        )
        
        dur_cc_txn_subcl_sel_brand = dur_cc_txn_subcl.join(brand_of_feature_product, ['brand_name'])
        
        dur_subcl_sel_brand_kpi = \
        (dur_cc_txn_subcl_sel_brand
         .groupBy('division_name','department_name','section_name','class_name',
#                   'subclass_name',
                  'brand_name', 'household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_subcl_sel_brand_kpi
         .join(dur_subcl_sel_brand_kpi, 
               ['division_name','department_name','section_name','class_name',
#                 'subclass_name',
                'brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', 
                                                 cust_movement_pre_dur_spend, prior_pre_cc_txn_subcl, 
                                                 ["class_name", 'brand_name'],
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        subcl_cust = dur_cc_txn_subcl.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_subcl
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(subcl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )
        
        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    
    elif switching_lv == 'class':
        #---- Create class-brand spend by cust movment
        prior_pre_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
         .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        prior_pre_cc_txn_class_sel_brand = prior_pre_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        prior_pre_class_sel_brand_kpi = \
        (prior_pre_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )
        
        dur_cc_txn_class = \
        (txn
         .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
         .filter(F.col('period_promo_mv_wk').isin(['cmp']))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])
        )
        
        dur_cc_txn_class_sel_brand = dur_cc_txn_class.join(brand_of_feature_product, ['brand_name'])
        
        dur_class_sel_brand_kpi = \
        (dur_cc_txn_class_sel_brand
         .groupBy('division_name','department_name','section_name',
                  'class_name','brand_name','household_id')
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )
        
        pre_dur_band_spend = \
        (prior_pre_class_sel_brand_kpi
         .join(dur_class_sel_brand_kpi, ['division_name','department_name','section_name',
                  'class_name','brand_name','household_id'], 'outer')
        )
        
        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_class,
                                                 ['class_name', 'brand_name'], 
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within class
        cl_cust = dur_cc_txn_class.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_class
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(cl_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    else:
        
        print('Unrecognized switching level')
        return None

# COMMAND ----------

def cust_sku_switching_promo_wk_prmzn(switching_lv,
                                      txn,
                                      cp_start_date,
                                      cp_end_date,
                                      test_store_sf,
                                      feat_list):
    
    from typing import List
    from pyspark.sql import DataFrame as sparkDataFrame
    
    """Media evaluation solution, customer sku switching
    """
    print('Customer switching SKU for "OFFLINE" + "ONLINE"')
    print("For PromoZone eval, use col 'period_promo_mv_wk'")

    spark.sparkContext.setCheckpointDir('/dbfs/FileStore/thanakrit/temp/checkpoint')
    
    prod_desc = spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()
    
    # Get section id - class id of feature products
    sec_id_class_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id')
     .drop_duplicates()
    )
    # Get section id - class id - subclass id of feature products
    sec_id_class_id_subclass_id_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('section_id', 'class_id', 'subclass_id')
     .drop_duplicates()
    )
    
    brand_of_feature_product = \
    (spark.table('tdm.v_prod_dim_c')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(feat_list))
     .select('brand_name')
     .drop_duplicates()
    )
            
    #---- During campaign - Exposed & Features SKU shopper    
    dur_campaign_exposed_cust_and_sku_shopper = \
    (txn
     .join(test_store_sf, 'store_id','inner') # Mapping cmp_start, cmp_end, mech_count by store
     .fillna(str(cp_start_date), subset='c_start')
     .fillna(str(cp_end_date), subset='c_end')
     .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end'))) # Filter only period in each mechanics
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('upc_id').isin(feat_list))
     .select('household_id')
     .drop_duplicates()
    )
    
    #----
    if switching_lv == 'subclass':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_subcl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp', 'cmp']))
         .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])

         .withColumn('pre_subcl_sales', F.when( F.col('period_promo_mv_wk').isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_subcl_sales', F.when( F.col('period_promo_mv_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_subcl_sales', F.sum(F.col('pre_subcl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_subcl_sales', F.sum(F.col('dur_subcl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_subcl = txn_sel_subcl_cc.filter( (F.col('cust_tt_pre_subcl_sales')>0) & (F.col('cust_tt_dur_subcl_sales')>0) )
        
        txn_cust_both_period_subcl_not_pre_but_dur_sku = \
        (txn_cust_both_period_subcl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_promo_mv_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_promo_mv_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        
        n_cust_both_subcl_switch_sku = \
        (txn_cust_both_period_subcl_not_pre_but_dur_sku
         .filter(F.col('pre_subcl_sales')>0) # only other products
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')
         .orderBy('custs', ascending=False)
        )
        
        return n_cust_both_subcl_switch_sku
        
    elif switching_lv == 'class':
        #---- Create subclass-brand spend by cust movement / offline+online
        txn_sel_cl_cc = \
        (txn
         .filter(F.col('household_id').isNotNull())
         .filter(F.col('period_promo_mv_wk').isin(['pre', 'ppp', 'cmp']))
#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
         .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])

         .withColumn('pre_cl_sales', F.when( F.col('period_promo_mv_wk').isin(['ppp', 'pre']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_cl_sales', F.when( F.col('period_promo_mv_wk').isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_cl_sales', F.sum(F.col('pre_cl_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_cl_sales', F.sum(F.col('dur_cl_sales')).over(Window.partitionBy('household_id') ))    
        )
        
        txn_cust_both_period_cl = txn_sel_cl_cc.filter( (F.col('cust_tt_pre_cl_sales')>0) & (F.col('cust_tt_dur_cl_sales')>0) )
        
        txn_cust_both_period_cl_not_pre_but_dur_sku = \
        (txn_cust_both_period_cl
         .withColumn('pre_sku_sales', 
                     F.when( (F.col('period_promo_mv_wk').isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('dur_sku_sales', 
                     F.when( (F.col('period_promo_mv_wk').isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
         .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
         .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
         .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
        )
        n_cust_both_cl_switch_sku = \
        (txn_cust_both_period_cl_not_pre_but_dur_sku
         .filter(F.col('pre_cl_sales')>0)
         .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
         .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
         .join(prod_desc, 'upc_id', 'left')         
         .orderBy('custs', ascending=False)
        )
        return n_cust_both_cl_switch_sku
        
    else:
        print('Unrecognized switching level')
        return None
