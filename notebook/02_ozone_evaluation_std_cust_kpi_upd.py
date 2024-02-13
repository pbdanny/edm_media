# Databricks notebook source
  sc

# COMMAND ----------

# MAGIC %md
# MAGIC #Import Library

# COMMAND ----------

# MAGIC %md ## Standard Library

# COMMAND ----------

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

# Add Threshold for spark session.  -- Pat 7 Aug 2023
# DO NOT CONFIG WATCHDOG
spark.conf.set("spark.databricks.queryWatchdog.outputRatioThreshold", 60000)
spark.conf.set("spark.sql.ansi.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Function

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_3

# COMMAND ----------

# MAGIC %md ## Thanakrit Repo function

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev"))

from instore_eval import get_cust_activated \
                        , get_cust_movement \
                        , get_cust_brand_switching_and_penetration \
                        , get_cust_sku_switching \
                        , get_profile_truprice \
                        , get_customer_uplift \
                        , get_cust_activated_prmzn \
                        , check_combine_region \
                        , get_cust_cltv \
                        , get_cust_brand_switching_and_penetration_multi \
                        , get_store_matching_across_region

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev"))

from utils.DBPath          import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval, CampaignEvalO3
from exposure              import exposed
from uplift                import uplift
from cross_cate            import asso_basket
from utils                 import cleanup   ## For clean up temp table in object process

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all parameter

# COMMAND ----------

#dbutils.widgets.removeAll()
## example passing parameter no widgets inside

# test_widgets = dbutils.notebook.entry_point.getCurrentBindings()
# for key in test_widgets:
#     print(f"{key}={test_widgets[key]}")


# COMMAND ----------

# dbutils.widgets.text('cmp_id_is'    , defaultValue='', label='011_cmp_id_is value = :')
# dbutils.widgets.text('cmp_id_online', defaultValue='na', label='012_cmp_id_online value = :')
# dbutils.widgets.text('cmp_id_dgs'   , defaultValue='', label='013_cmp_id_dgs value = :')
# dbutils.widgets.text('cmp_id'       , defaultValue='', label='01_cmp_id_combine_all value = :')
# dbutils.widgets.text('cmp_nm', defaultValue='', label='02_cmp_nm value = :')
# dbutils.widgets.text('eval_type', defaultValue='', label='03_eval_type value = :')
# dbutils.widgets.text('cmp_start', defaultValue='', label='04_cmp_start value = :')
# dbutils.widgets.text('cmp_end', defaultValue='', label='05_cmp_end value = :')
# dbutils.widgets.text('gap_start_date', defaultValue='', label='06_gap start date value = :')
# dbutils.widgets.text('gap_end_date', defaultValue='', label='07_gap end date value = :')
# dbutils.widgets.text('cmp_month', defaultValue='', label='08_cmp_month value = :')
# dbutils.widgets.text('store_fmt', defaultValue='', label='09_store_fmt value = :')
# dbutils.widgets.text('cmp_objective_offline', defaultValue='', label='10.1_cmp_objective offline value = :')
# dbutils.widgets.text('cmp_objective_online', defaultValue='', label='10.2_cmp_objective online value = :')

# dbutils.widgets.text('media_fee_offline', defaultValue='', label='11.1_media_fee_offline value = :')
# dbutils.widgets.text('media_fee_ooh', defaultValue='', label='11.2_media_fee_outoffhomw value = :')
# dbutils.widgets.text('media_fee_online', defaultValue='', label='12_media_fee_online value = :')
# ## Online information

# dbutils.widgets.text('reach_online', defaultValue='', label='13_#Reach Customers (online) = :')
# dbutils.widgets.text('match_rate'  , defaultValue='', label='14_%Match Rate (online) = :')
# dbutils.widgets.text('ca_flag'     , defaultValue='', label='23_Flag having online campaign (0 or 1) = :')
# dbutils.widgets.text('use_ca_tab'  , defaultValue='', label='24_Flag if use consolidate CA table value (0 or 1) = :')
# ##
# dbutils.widgets.text('cate_lvl'    , defaultValue='class', label='15_cate_lvl value = :')
# dbutils.widgets.text('x_cate_flag' , defaultValue='', label='16_Cross category flag (0 or 1) = :')
# dbutils.widgets.text('x_cate_cd'   , defaultValue='', label='17_Cross Category code (if any) = :')

# dbutils.widgets.text('use_reserved_store', defaultValue='', label='18_use_reserved_store value = :')
# dbutils.widgets.text('resrv_store_class', defaultValue='', label='19_resrv_store_class value = :')
# dbutils.widgets.text('hv_ctrl_store', defaultValue='', label='20_hv_ctrl_store value = :')
# dbutils.widgets.text('resrv_store_file', defaultValue='', label='21_resrv_store_file value = :')
# dbutils.widgets.text('adjacency_file', defaultValue='', label='22_adjacency_file value = :')

# ## 24 Jan 2024 -- Pat mark out as no need 
# #dbutils.widgets.text('exp_sto_flg', defaultValue='', label='10_Store Level Exposure Flag = :')
# ## 24 Jan 2024 -- Pat mark out, as code O3 combine with standard eval campaign >> always use the same trans
# #dbutils.widgets.text('use_txn_ofn', defaultValue='', label='12_Flag if use offline campaign txn as initial (0 or 1) :')

# dbutils.widgets.text('media_mechanic', defaultValue='', label='25_media_mechanic value = :')
# dbutils.widgets.text('media_loc', defaultValue='', label='26_media_loc value = :')
# dbutils.widgets.text('mechanic_count', defaultValue='', label='27_mechanic_count value = :')
# dbutils.widgets.text('media_stores', defaultValue='', label='28_media_stores value = :')

# dbutils.widgets.text('dbfs_project_path', defaultValue='', label='29_dbfs_project_path_spark_api value = :')
# dbutils.widgets.text('input_path', defaultValue='', label='30_Input file path value = :')
# dbutils.widgets.text('sku_file', defaultValue='', label='31_SKU file path value = :')

# dbutils.widgets.text('target_file', defaultValue='', label='32_Target stores file value = :')
# dbutils.widgets.text('control_file', defaultValue='', label='33_Control stores file value = :')
# dbutils.widgets.text('svv_table', defaultValue='', label='34_survival_rate_table value = :')
# dbutils.widgets.text('pcyc_table', defaultValue='', label='35_purchase_cycle_table value = :')

# ## add week type to support both promo_wk and fis_wk  -- Pat 8 Sep 2022
# dbutils.widgets.text('wk_type', defaultValue='', label='36_Week Type of campaign (fis_wk, promo_wk) value = :')

# # campaign file api path - Danny 13 Jul 23
# dbutils.widgets.text('campaign_file_api', defaultValue='', label='37_Campaign file api for code v2')

# # campaign row number in campaign list file for code v2 - Pat 22 Jul 23
# dbutils.widgets.text('cmp_row', defaultValue='', label='38_Campaign row number')

## get value from widgets to variable
cmp_id_is          = dbutils.widgets.get('cmp_id_is').strip()
cmp_id_online      = dbutils.widgets.get('cmp_id_online').strip()
cmp_id_dgs         = dbutils.widgets.get('cmp_id_dgs').strip()
##
cmp_id             = dbutils.widgets.get('cmp_id').strip()
#cmp_id            = f"{cmp_id_is}_{cmp_id_online}_{cmp_id_dgs}"

cmp_nm             = dbutils.widgets.get('cmp_nm').strip()
eval_type          = dbutils.widgets.get('eval_type').strip().lower()
cmp_start          = dbutils.widgets.get('cmp_start')
cmp_end            = dbutils.widgets.get('cmp_end')
cmp_month          = dbutils.widgets.get('cmp_month')
gap_start_date     = dbutils.widgets.get('gap_start_date')
gap_end_date       = dbutils.widgets.get('gap_end_date')

store_fmt          = dbutils.widgets.get('store_fmt').strip().lower()
cmp_objective_offline = dbutils.widgets.get('cmp_objective_offline').strip()
cmp_objective_online  = dbutils.widgets.get('cmp_objective_online').strip()
media_fee_offline     = dbutils.widgets.get('media_fee_offline')
media_fee_ooh         = dbutils.widgets.get('media_fee_ooh')
media_fee_online      = dbutils.widgets.get('media_fee_online')

## Online info
reach_online  = dbutils.widgets.get('reach_online').strip()
match_rate    = dbutils.widgets.get('match_rate').strip()
## Pat add 25 Jan 2024  >> to check if include Online campaign
ca_flag       =  dbutils.widgets.get('ca_flag').strip()
use_ca_tab    =  dbutils.widgets.get('use_ca_tab').strip()

cate_lvl      = dbutils.widgets.get('cate_lvl').strip().lower()

#exp_sto_flg           = dbutils.widgets.get('exp_sto_flg').strip().lower()

adjacency_file = dbutils.widgets.get('adjacency_file').strip()
x_cate_flag    = dbutils.widgets.get('x_cate_flag').strip().lower()
x_cate_cd      = dbutils.widgets.get('x_cate_cd').strip()

#use_txn_ofn    = dbutils.widgets.get('use_txn_ofn').strip().lower()

use_reserved_store = dbutils.widgets.get('use_reserved_store')
resrv_store_class  = dbutils.widgets.get('resrv_store_class').strip()
hv_ctrl_store      = dbutils.widgets.get('hv_ctrl_store')
resrv_store_file   = dbutils.widgets.get('resrv_store_file').strip()


media_mechanic     = dbutils.widgets.get('media_mechanic').strip()
media_loc          = dbutils.widgets.get('media_loc').strip()
mechanic_count     = dbutils.widgets.get('mechanic_count')
media_stores       = dbutils.widgets.get('media_stores')
dbfs_project_path  = dbutils.widgets.get('dbfs_project_path')
input_path         = dbutils.widgets.get('input_path')
sku_file           = dbutils.widgets.get('sku_file').strip()
target_file        = dbutils.widgets.get('target_file')
control_file       = dbutils.widgets.get('control_file')

svv_table          = dbutils.widgets.get('svv_table')
pcyc_table         = dbutils.widgets.get('pcyc_table')

## add week type to support both promo_wk and fis_wk  -- Pat 8 Sep 2022
wk_type            = dbutils.widgets.get('wk_type')

# campaign file api path - Danny 13 Jul 23
campaign_file_api  = dbutils.widgets.get('campaign_file_api')

# campaign row number -- Pat 22 Jul 2023

cmp_row            = dbutils.widgets.get('cmp_row')

# COMMAND ----------

#print( ' cmp_id_is value     = : '  +  cmp_id + '\n')
print( ' cmp_id_is value     = : '  +  cmp_id_is + '\n')
print( ' cmp_id_online value = : '  +  cmp_id_online + '\n')
print( ' cmp_id_dgs value    = : '  +  cmp_id_dgs + '\n')
print( ' cmp_id_o3 value     = : '  +  cmp_id + '\n')
print( ' cmp_nm value        = : '  +  cmp_nm + '\n')
print( ' eval_type value     = : '  +  eval_type + '\n')
print( ' cmp_start value     = : '  +  cmp_start + '\n')
print( ' cmp_end value       = : '  +  cmp_end + '\n')
print( ' cmp_month value     = : '  +  cmp_month + '\n')
print( ' store_fmt value     = : '  +  store_fmt + '\n')
print( ' cmp_objective value = : '  +  cmp_objective_offline + '\n')
print( ' media_fee_offline value  = : '  +  media_fee_offline + '\n')
print( ' media_fee_online value   = : '  +  media_fee_online + '\n')
print( ' cate_lvl value           = : '  +  cate_lvl + '\n')
print( ' use_reserved_store value = : '  +  use_reserved_store + '\n')
print( ' resrv_store_class value  = : '  +  resrv_store_class + '\n')
print( ' x_cate_flag value = : '  +  x_cate_flag + '\n')
print( ' x_cate_cd value = : '  +  x_cate_cd + '\n')
print( ' media_mechanic value = : '  +  media_mechanic + '\n')
print( ' media_loc value = : '  +  media_loc + '\n')
print( ' mechanic_count value = : '  +  mechanic_count + '\n')
print( ' media_stores value = : '  +  media_stores + '\n')
print( ' hv_ctrl_store value = : '  +  hv_ctrl_store + '\n')
print( ' resrv_store_file value = : '  +  resrv_store_file + '\n')
print( ' adjacency_file value = : '  +  adjacency_file + '\n')
print( ' dbfs_project_path value = : '  +  dbfs_project_path + '\n')
print( ' input_path value = : '  +  input_path + '\n')
print( ' sku_file value = : '  +  sku_file + '\n')
print( ' target_file value = : '  +  target_file + '\n')
print( ' control_file value = : '  +  control_file + '\n')
print( ' gap start date value = : '  +  gap_start_date + '\n')
print( ' gap end date value = : '  +  gap_end_date + '\n')
print( ' survival rate table value = : ' + svv_table + '\n')
print( ' purchase_cycle table value = : ' + pcyc_table + '\n')

## add week type to support both promo_wk and fis_wk  -- Pat 8 Sep 2022
print( ' Campaign week type = : ' + wk_type + '\n')

# campaign file api path - Danny 13 Jul 23
print( ' Campaign file api = : ' + campaign_file_api + '\n')

# campaign row number - Pat 22 Jul 2023
print( ' Campaign row number in campaign list (config file) = : ' + cmp_row + '\n')

print('=' *80)
print('Online evaluation parameter ')
print('=' *80)
print(' Flag having online camapign     :' + str(ca_flag))
print(' Flag using consolidate CA table :' + str(use_ca_tab))
print(' Online Reach customers          :' + str(reach_online))
print(' % CA Match Rate                 :' + str(match_rate))
print('=' *80)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup date period parameter

# COMMAND ----------

## cmp_start = campaign start date in format yyyy-mm-dd
## cmp_end   = campaign end date in format yyyy-mm-dd

## get date and convert back to string
## fis_week
cmp_st_wk   = wk_of_year_ls(cmp_start)
cmp_en_wk   = wk_of_year_ls(cmp_end)

## promo_wk
cmp_st_promo_wk   = wk_of_year_promo_ls(cmp_start)
cmp_en_promo_wk   = wk_of_year_promo_ls(cmp_end)

## Gap Week (fis_wk)
if ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == '')) & ((str(gap_end_date).lower == 'nan') | (str(gap_end_date).strip() == '')):
    print('No Gap Week for campaign :' + str(cmp_nm))
    gap_flag    = False
    chk_pre_wk  = cmp_st_wk
    chk_pre_dt  = cmp_start
elif( (not ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == ''))) & 
      (not ((str(gap_end_date).lower() == 'nan')   | (str(gap_end_date).strip() == ''))) ):    
    print('\n Campaign ' + str(cmp_nm) + ' has gap period between : ' + str(gap_start_date) + ' and ' + str(gap_end_date) + '\n')
    ## fis_week
    gap_st_wk   = wk_of_year_ls(gap_start_date)
    gap_en_wk   = wk_of_year_ls(gap_end_date)
    
    ## promo
    gap_st_promo_wk  = wk_of_year_promo_ls(gap_start_date)
    gap_en_promo_wk  = wk_of_year_promo_ls(gap_end_date)
    
    gap_flag         = True    
    
    chk_pre_dt       = gap_start_date
    chk_pre_wk       = gap_st_wk
    chk_pre_promo_wk = gap_st_promo_wk
    
else:
    print(' Incorrect gap period. Please recheck - Code will skip !! \n')
    print(' Received Gap = ' + str(gap_start_date) + " and " + str(gap_end_date))
    
    raise Exception("Incorrect Gap period value please recheck !!")
## end if   

pre_en_date = (datetime.strptime(chk_pre_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
pre_en_wk   = wk_of_year_ls(pre_en_date)
pre_st_wk   = week_cal(pre_en_wk, -12)                       ## get 12 week away from end week -> inclusive pre_en_wk = 13 weeks
pre_st_date = f_date_of_wk(pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data
## promo week
pre_en_promo_wk = wk_of_year_promo_ls(pre_en_date)
pre_st_promo_wk = promo_week_cal(pre_en_promo_wk, -12)   

ppp_en_wk       = week_cal(pre_st_wk, -1)
ppp_st_wk       = week_cal(ppp_en_wk, -12)
##promo week
ppp_en_promo_wk = promo_week_cal(pre_st_promo_wk, -1)
ppp_st_promo_wk = promo_week_cal(ppp_en_promo_wk, -12)

ppp_st_date = f_date_of_wk(ppp_en_wk).strftime('%Y-%m-%d')
ppp_en_date = f_date_of_wk(ppp_st_wk).strftime('%Y-%m-%d')

## Add setup week type parameter

if wk_type == 'fis_wk':
    wk_tp     = 'fiswk'
    week_type = 'fis_week'    
elif wk_type == 'promo_wk':
    wk_tp     = 'promowk'
    week_type = 'promo_week'    
## end if


print('\n' + '-'*80 + '\n Date parameter for campaign ' + str(cmp_nm) + ' shown below \n' + '-'*80 )
print('Campaign period between ' + str(cmp_start) + ' and ' + str(cmp_end) + '\n')
print('Campaign is during Promo week ' + str(cmp_st_promo_wk) + ' to ' + str(cmp_en_promo_wk) + '\n')
print('Campaign pre-period (13 weeks) between week ' + str(pre_st_wk) + ' and week ' + str(pre_en_wk) + ' \n')
print('Campaign pre-period (13 weeks) between promo week ' + str(pre_st_promo_wk) + ' and week ' + str(pre_en_promo_wk) + ' \n')
print('Campaign pre-period (13 weeks) between date ' + str(pre_st_date) + ' and week ' + str(pre_en_date) + ' \n')

print('Campaign prior period (13+13 weeks) between week ' + str(ppp_st_wk) + ' and week ' + str(ppp_en_wk) + ' \n')
print('Campaign prior period (13+13 weeks) between promo week ' + str(ppp_st_promo_wk) + ' and week ' + str(ppp_en_promo_wk) + ' \n')
print('Campaign prior period (13+13 weeks) between date ' + str(ppp_st_date) + ' and week ' + str(ppp_en_date) + ' \n')

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup data path to run

# COMMAND ----------

## ----------------------------------------------
## setup file path Spark API
## ----------------------------------------------

mpath      = 'dbfs:/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/'
stdin_path = mpath + '00_std_inputs/'
eval_path  = mpath + '05_O3/'
## input path for evaluaton
incmp_path   = eval_path + '00_cmp_inputs/'
input_path = incmp_path + 'input_files/'

cmp_out_path = eval_path + cmp_month + '/' + str(cmp_id) + '/'
print('cmp_out_path = ' + str(cmp_out_path))

## ----------------------------------------------
## setup file path File API
## ----------------------------------------------

mpath_fl     = '/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/'
stdin_path_fl = mpath_fl + '00_std_inputs/'
eval_path_fl  = mpath_fl + '05_O3/'
## input path for evaluaton
incmp_path_fl = eval_path_fl + '00_cmp_inputs/'
input_path_fl = incmp_path_fl + 'input_files/'
cmp_out_path_fl = eval_path_fl + cmp_month + '/' + str(cmp_nm) + '/'
print('cmp_out_path_fl = ' + str(cmp_out_path_fl))
print('dbfs_project_path = ' + dbfs_project_path )

## ----------------------------------------------
## setup path for noteboook to run
## ----------------------------------------------
#dbs_nb_path = '/EDM_Share/EDM_Media/Campaign_Evaluation/Instore/hde/'



# COMMAND ----------

# MAGIC %md # Step get Campaign object for calling new code from p'Danny -- 22 Jul 2023

# COMMAND ----------

conf     = CampaignConfigFile(campaign_file_api)

cmp_rowi = int(cmp_row)

cmp      = CampaignEvalO3(conf, cmp_row_no=cmp_rowi)

cmp.display_details()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all input file name

# COMMAND ----------

print('\n' + '-'*80 + '\n' )
in_sku_file = input_path_fl + sku_file
in_trg_file = input_path + target_file
##
in_ai_file  = stdin_path + adjacency_file
##

print(' Input sku file = ' + in_sku_file )
print(' Input target file = ' + in_trg_file )
print(' Input Product Adjacency file = ' + in_ai_file )

## control need check
if (eval_type == 'full') & (hv_ctrl_store == 'true'):
    in_ctl_file = input_path + control_file    
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Control store file for campaign ' + str(cmp_nm) + ' : ' + in_ctl_file + '\n')
elif (eval_type == 'full') & (hv_ctrl_store != 'true') & (use_reserved_store == 'true'):    
    in_ctl_file = stdin_path + resrv_store_file    
    flg_use_oth = False
    flg_use_rsv = True
    print('\n Campaign will use standard reserved store . \n ')
elif (eval_type == 'full') & (hv_ctrl_store  != 'true') & (use_reserved_store != 'true'):
    flg_use_oth = True
    flg_use_rsv = False
    print('\n Campaign will use the rest store for matching !! . \n ')
elif (eval_type == 'std'):
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Campaign ' + str(cmp_nm) + ' do not need control store. \n')
else:
    flg_use_oth = False
    flg_use_rsv = False
    print('\n Campaign ' + str(cmp_nm) + ' do not need control store. \n')
## end if


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prep feature product, brand, class, subclass, aisle_use

# COMMAND ----------

#Test function get product info
##----------------------------------------------
## read sku file to list
##----------------------------------------------
feat_pd = pd.read_csv(in_sku_file)
#feat_pd.display()
feat_list = feat_pd['feature'].drop_duplicates().to_list()
print('-'*80 + '\n List of feature SKU show below : \n ' + '-'*80)
print(feat_list)

## Import Adjacency file
std_ai_df    = spark.read.csv(in_ai_file, header="true", inferSchema="true")

# function get product info

feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list, use_ai_group_list, use_ai_sec_list = _get_prod_df( feat_list
                                                                                                                                                                                                                                              ,cate_lvl
                                                                                                                                                                                                                                              ,std_ai_df
                                                                                                                                                                                                                                              ,x_cate_flag
                                                                                                                                                                                                                                              ,x_cate_cd)

#use_ai_df.display(10)

# COMMAND ----------

#feat_df.display(5)
feat_detail = feat_df.select( lit(str(use_ai_group_list)).alias('ai_group_list')
                             ,feat_df.div_nm.alias('division_name')
                             ,feat_df.dept_nm.alias('department_name')
                             ,feat_df.sec_nm.alias('section_name')
                             ,feat_df.class_nm.alias('class_name')
                             ,feat_df.sclass_nm.alias('subclass_name')
                             ,feat_df.brand_nm.alias('brand_name')
                             ,feat_df.upc_id
                             ,feat_df.prod_en_desc
                             ,lit(str(use_ai_sec_list)).alias('ai_sec_list')
                             ,feat_df.mfr_name.alias('manufactor_name')
                            )

## add display brand
print('-'*80)
print('\n Check display brand ')
print('-'*80)
brand_df.limit(10).display()

# COMMAND ----------

# MAGIC %md ##Prep input files

# COMMAND ----------

#trg_str_df.printSchema()
# store_dim = sqlContext.table('tdm.v_store_dim')
# store_dim.printSchema()

# COMMAND ----------

##---------------------
## Prep store dim
##---------------------
## for HDE only, for gofresh need o change store filer to 5 and combine region (central + west)

if store_fmt == 'hde':
    print('Prepare store dim for HDE')
    store_dim = sqlContext.table('tdm.v_store_dim').where(F.col('format_id').isin(1,2,3))\
                                                   .select( F.col('store_id')
                                                           ,F.col('format_id')
                                                           ,F.col('date_opened')
                                                           ,F.col('date_closed')
                                                           ,lower(F.col('region')).alias('store_region') 
                                                           ,lower(F.col('region')).alias('store_region_orig'))

elif store_fmt == 'gofresh' :
    print('Prepare store dim for Go Fresh')
    store_dim = sqlContext.table('tdm.v_store_dim').where(F.col('format_id').isin(5))\
                                                   .select( F.col('store_id')
                                                           ,F.col('format_id')
                                                           ,F.col('date_opened')
                                                           ,F.col('date_closed')
                                                           ,F.when(lower(F.col('region')) == 'west', lit('central'))
                                                             .otherwise(lower(F.col('region')))
                                                             .alias('store_region')
                                                           ,lower(F.col('region')).alias('store_region_orig'))
## Added 12 Dec 2023  -- Pat
elif store_fmt.lower() == 'talad' :
    print('Prepare store dim for Talad')
    store_dim = sqlContext.table('tdm.v_store_dim').where(F.col('format_id').isin(4))\
                                                   .select( F.col('store_id')
                                                           ,F.col('format_id')
                                                           ,F.col('date_opened')
                                                           ,F.col('date_closed')
                                                           ,lower(F.col('region')).alias('store_region') 
                                                           ,lower(F.col('region')).alias('store_region_orig'))
                                                    
else :
    print('Store Format is not correct code will skip evaluation for campaign ' + str(cmp_nm) + ' !!\n')

    cleanup.clear_attr_and_temp_tbl(cmp)  ## Add step call to cleanup object attribute -- Pat 4 Aug 2023

    raise Exception("Incorrect store format value !!")
## end if

## Import target file
in_trg_df = spark.read.csv(in_trg_file, header="true", inferSchema="true")

## end if    

## Import control file - if full evaluation
##===================================================================================
## 4 Jan 2023 - Pat
## Add check duplicate between target & control stores
## Throw error exception in case of duplicate between 2 set
##===================================================================================

if (eval_type == 'full') & (hv_ctrl_store == 'true'):
    ## use self control
    in_ctl_str_df = spark.read.csv(in_ctl_file, header="true", inferSchema="true")
    
    ## call function to check dup -- Pat 4 Jan 2023
    print('='*80)
    print(' \n Check duplicate between Target & control stores (hv_ctl_store == True). \n ')
    
    n_store_dup, dup_str_list =  get_target_control_store_dup(in_trg_df, in_ctl_str_df)
    
    if (n_store_dup > 1):
        except_txt       = ' Target stores is duplicate with control stores list !! \n number of duplicate = ' + str(n_store_dup) + ' stores \n List of store => ' + str(dup_str_list)

        cleanup.clear_attr_and_temp_tbl(cmp)  ## Add step call to cleanup object attribute -- Pat 4 Aug 2023
        
        raise Exception(except_txt)
    
elif (eval_type == 'full') & (flg_use_rsv) :
    
    ## use reserved will need to filter category reserved
    all_rsv_df    = spark.read.csv(in_ctl_file, header="true", inferSchema="true")
    in_ctl_str_df = all_rsv_df.where((all_rsv_df.class_code == resrv_store_class.upper()) & 
                                  (all_rsv_df.rs_flag == 'reserved')
                                 )\
                              .select(all_rsv_df.store_id)
    
    ## call function to check dup -- Pat 4 Jan 2023
    print('='*80)
    print(' \n Check duplicate between Target & Reserved control stores. \n ')
    
    #chk_store_dup_df = in_ctl_str_df.join(in_trg_df, [in_ctl_str_df.store_id == in_trg_df.store_id], 'left_semi')    
    #n_store_dup      = chk_store_dup_df.agg(sum(lit(1)).alias('n_store_dup')).collect()[0].n_store_dup
    
    n_store_dup, dup_str_list =  get_target_control_store_dup(in_trg_df, in_ctl_str_df)
    
    if (n_store_dup > 1):
        #dup_str_list     = chk_store_dup_df.toPandas()['store_id'].drop_duplicates().to_list()
        #print('='*80)
        #print(' List of duplicate between target & control => ' + str(dup_str_list))
        #print('='*80)
        except_txt       = ' Target stores is duplicate with reserved store !! \n number of duplicate = ' + str(n_store_dup) + ' stores \n List of store => ' + str(dup_str_list)
        raise Exception(except_txt)
        
##===================================================================================

elif (eval_type == 'full') & (flg_use_oth) :    
    in_ctl_str_df = store_dim.join  (in_trg_df, [store_dim.store_id == in_trg_df.store_id], 'left_anti')\
                             .select(store_dim.store_id)
    
## end if    


## get region for target & control store

# trg_str_df = in_trg_df.join  ( store_dim, [in_trg_df.store_id == store_dim.store_id], 'inner')\
#                       .select( in_trg_df.store_id
#                               ,store_dim.store_region_orig
#                               ,store_dim.store_region
#                               ,in_trg_df.c_start
#                               ,in_trg_df.c_end
#                               ,in_trg_df.mech_count
#                               ,in_trg_df.mech_name
#                               ,in_trg_df.media_fee_psto)  ## add column 'media_fee_psto' from input file : 24 Jul 2023  - Pat 

trg_str_df = in_trg_df.join  ( store_dim, [in_trg_df.store_id == store_dim.store_id], 'inner')\
                      .select( in_trg_df['*']
                              ,store_dim.store_region_orig
                              ,store_dim.store_region)  ## add column 'media_fee_psto' from input file : 24 Jul 2023  - Pat 
                      
## Add get distinct list of mechanics set + store count from input files & print out  -- Pat AUG 2022
trg_mech_set    = trg_str_df.groupBy(trg_str_df.mech_name)\
                            .agg    ( countDistinct(trg_str_df.store_id).alias('store_cnt')
                                     ,sum(trg_str_df.media_fee_psto).alias('media_fee')
                                    )\
                            .orderBy(trg_str_df.mech_name)\
                            .persist()

## Replace spaces and special characters with underscore in mech name, to match with Uplift by Mech -- Ta Nov 2022
trg_mech_set = trg_mech_set.withColumn('mech_name', F.regexp_replace(F.col('mech_name'), "[^a-zA-Z0-9]", "_"))

trg_mech_set_pd = trg_mech_set.toPandas()  ## for write out

print('-'*80)
print('Display Mechanic set')
print('-'*80)

trg_mech_set_pd.display()

if (eval_type == 'full'):
    u_ctl_str_df = in_ctl_str_df.join ( store_dim, [in_ctl_str_df.store_id == store_dim.store_id], 'inner')\
                                .select( in_ctl_str_df.store_id
                                        ,store_dim.store_region_orig
                                        ,store_dim.store_region)
    print('-'*80)
    print('Display Check - control store')
    print('-'*80)
    u_ctl_str_df.limit(10).display()
    
## end if

#dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0136_M02E.csv
#dbfs:/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0136_M02E.csv

# COMMAND ----------

## write out mechanics set to result folder for CRDM -- Pat Added 1 Aug 2022

pandas_to_csv_filestore(trg_mech_set_pd, 'mechanics_setup_details.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

## add check show control
#u_ctl_str_df.limit(10).display()

# COMMAND ----------

# MAGIC %md # Create transaction

# COMMAND ----------

#---- Get total data number of week, and each start - end period date
# tot_data_week_num, cmp_st_date, cmp_end_date, gap_st_date, gap_en_date, pre_st_date, pre_en_date, prior_st_date, prior_en_date = \
# get_total_data_num_week(c_st_date_txt=cmp_start, c_en_date_txt=cmp_end, gap_week_txt=gap_week)

txn_tab_nm = f'tdm_dev.media_campaign_eval_txn_data_{cmp_id}'

#---- Try loding existing data table, unless create new
try:
    # Test block
    # raise Exception('To skip try block') 
    txn_all = spark.table(f'tdm_dev.media_campaign_eval_txn_data_{cmp_id}')
    print(f'Load data table for period : Ppp - Pre - Gap - Cmp, All store All format \n from : tdm_dev.media_campaign_eval_txn_data_{cmp_id}')

except:
    print(f'Create intermediate transaction table for period Prior - Pre - Dur , all store format : tdm_dev.media_campaign_eval_txn_data_{cmp_id}')
    txn_all = get_trans_itm_wkly(start_week_id=ppp_st_wk, end_week_id=cmp_en_wk, store_format=[1,2,3,4,5], 
                                  prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
                                                   'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                                   'department_code', 'section_code', 'class_code', 'subclass_code'])
    # Combine feature brand - Danny
    brand_list = brand_df.select("brand_nm").drop_duplicates().toPandas()["brand_nm"].tolist()
    brand_list.sort()
    if len(brand_list) > 1:
        txn_all = txn_all.withColumn("brand_name", F.when(F.col("brand_name").isin(brand_list), F.lit(brand_list[0])).otherwise(F.col("brand_name")))
    
    #---- Add period column
    if gap_flag:
        print('Data with gap week')
        txn_all = (txn_all.withColumn('period_fis_wk', 
                                      F.when(F.col('week_id').between(cmp_st_wk, cmp_en_wk), F.lit('cmp'))
                                       .when(F.col('week_id').between(gap_st_wk, gap_en_wk), F.lit('gap'))
                                       .when(F.col('week_id').between(pre_st_wk, pre_en_wk), F.lit('pre'))
                                       .when(F.col('week_id').between(ppp_st_wk, ppp_en_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                          .withColumn('period_promo_wk', 
                                      F.when(F.col('promoweek_id').between(cmp_st_promo_wk, cmp_en_promo_wk), F.lit('cmp'))
                                       .when(F.col('promoweek_id').between(gap_st_promo_wk, gap_en_promo_wk), F.lit('gap'))
                                       .when(F.col('promoweek_id').between(pre_st_promo_wk, pre_en_promo_wk), F.lit('pre'))
                                       .when(F.col('promoweek_id').between(ppp_st_promo_wk, ppp_en_promo_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                  )
    else:
        txn_all = (txn_all.withColumn('period_fis_wk', 
                                      F.when(F.col('week_id').between(cmp_st_wk, cmp_en_wk), F.lit('cmp'))
                                       .when(F.col('week_id').between(pre_st_wk, pre_en_wk), F.lit('pre'))
                                       .when(F.col('week_id').between(ppp_st_wk, ppp_en_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                          .withColumn('period_promo_wk', 
                                      F.when(F.col('promoweek_id').between(cmp_st_promo_wk, cmp_en_promo_wk), F.lit('cmp'))
                                       .when(F.col('promoweek_id').between(pre_st_promo_wk, pre_en_promo_wk), F.lit('pre'))
                                       .when(F.col('promoweek_id').between(ppp_st_promo_wk, ppp_en_promo_wk), F.lit('ppp'))
                                       .otherwise(F.lit('NA')))
                  )        

    txn_all.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(txn_tab_nm)
    ## Pat add, delete dataframe before re-read
    del txn_all
    ## Re-read from table
    txn_all = spark.table(txn_tab_nm)

# COMMAND ----------

txn_all.printSchema()

# Special filter for incorrect data in txn item and txn head -- Pat  14 Dec 2022
# txn_all = txn_all. where( ~(((txn_all.upc_id == 52012742) & ( txn_all.transaction_uid == 162572146008 ) & (txn_all.store_id == 2742 ) & ( txn_all.date_id == '2022-11-30' ))  |
#                             ((txn_all.upc_id == 51630714) & ( txn_all.transaction_uid == 172196252055 ) & (txn_all.store_id == 5140 ) & ( txn_all.date_id == '2022-12-02' ))  |
#                             ((txn_all.upc_id == 51223004) & ( txn_all.transaction_uid == 51975905005 ) & (txn_all.store_id == 3527 ) & ( txn_all.date_id == '2022-11-10' ))  |
#                             ((txn_all.upc_id == 74531077) & ( txn_all.transaction_uid == 172196262075 ) & (txn_all.store_id == 5162 ) & ( txn_all.date_id == '2022-12-02' ))  |
#                             ((txn_all.upc_id == 51885470) & ( txn_all.transaction_uid == 172196255038 ) & (txn_all.store_id == 6470 ) & ( txn_all.date_id == '2022-12-02' ))  |
#                             ((txn_all.upc_id == 52169337) & ( txn_all.transaction_uid == 172196258001 ) & (txn_all.store_id == 5134 ) & ( txn_all.date_id == '2022-12-02' ))  |
#                             ((txn_all.upc_id == 51929736) & ( txn_all.transaction_uid == 172196262029 ) & (txn_all.store_id == 5162 ) & ( txn_all.date_id == '2022-12-02' )))
#                          )

# COMMAND ----------

# MAGIC %md ## Assign TXN to campaign object

# COMMAND ----------

cmp.txn = txn_all.replace({"cmp":"dur"}).withColumn("unit", F.col("pkg_weight_unit"))

# COMMAND ----------

# MAGIC %md #Check region - test store, Combine 'store_region' if GoFresh

# COMMAND ----------

test_store_sf = spark.read.csv(in_trg_file , header=True, inferSchema=True)
test_vs_all_store_count, txn_all = check_combine_region(store_format_group=store_fmt, test_store_sf=test_store_sf, txn = txn_all)
test_vs_all_store_count.display()
test_vs_all_store_count_df = to_pandas(test_vs_all_store_count)
pandas_to_csv_filestore(test_vs_all_store_count_df, f'test_vs_all_store_count.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##1. Check feature SKU details  
# MAGIC ##2. Create adjacency product group / adjacency upc_id  

# COMMAND ----------

# # path of adjacency mapping file
# adjacency_file_path = os.path.join(stdin_path, adjacency_file)

# adj_prod_sf, adj_prod_group_name_sf, featues_product_and_exposure_sf, mfr_promoted_product_str = \
# get_adjacency_product_id(promoted_upc_id=feat_list , adjacecy_file_path=adjacency_file_path)

# # Save adjacency product 
# adj_prod_df = to_pandas(adj_prod_sf)
# pandas_to_csv_filestore(adj_prod_df, 'adj_prod_id.csv', prefix=os.path.join(cmp_out_path, 'output'))

# # Save adjacency group name
# adj_prod_group_name_df = to_pandas(adj_prod_group_name_sf)
# pandas_to_csv_filestore(adj_prod_group_name_df, 'adj_group_name.csv', prefix=os.path.join(cmp_out_path, 'result'))

# # Detail of feature products + exposure
# featues_product_and_exposure_df = to_pandas(featues_product_and_exposure_sf)
# pandas_to_csv_filestore(featues_product_and_exposure_df, 'feature_product_and_exposure_details.csv', 
#                         prefix=os.path.join(cmp_out_path, 'result'))

# COMMAND ----------

# path of adjacency mapping file
#adjacency_file_path = os.path.join(stdin_path, adjacency_file)

#adj_prod_sf, adj_prod_group_name_sf, featues_product_and_exposure_sf, mfr_promoted_product_str = \
#get_adjacency_product_id(promoted_upc_id=feat_list , adjacecy_file_path=adjacency_file_path)

# Save adjacency product 
adj_prod_sf = use_ai_df
adj_prod_df = to_pandas(use_ai_df)
pandas_to_csv_filestore(adj_prod_df, 'adj_prod_id.csv', prefix=os.path.join(cmp_out_path_fl, 'output'))

# Save adjacency group name
#adj_prod_group_name_df = to_pandas(adj_prod_group_name_sf)
#pandas_to_csv_filestore(adj_prod_group_name_df, 'adj_group_name.csv', prefix=os.path.join(cmp_out_path, 'result'))

# Detail of feature products + exposure
featues_product_and_exposure_df = to_pandas(feat_detail)
pandas_to_csv_filestore(featues_product_and_exposure_df, 'feature_product_and_exposure_details.csv', 
                        prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Report : Awareness

# COMMAND ----------

# cmp_st_date = datetime.strptime(cmp_start, '%Y-%m-%d')
# cmp_end_date = datetime.strptime(cmp_end, '%Y-%m-%d')
# exposure_all, exposure_region = get_awareness(txn_all, cp_start_date=cmp_st_date, cp_end_date=cmp_end_date,
#                                               store_fmt=store_fmt, test_store_sf=test_store_sf, adj_prod_sf=adj_prod_sf,
#                                               media_spend=float(media_fee))
# exposure_all_df = to_pandas(exposure_all)
# pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(cmp_out_path, 'result'))
# exposure_region_df = to_pandas(exposure_region)
# pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(cmp_out_path, 'result'))

# COMMAND ----------

spark.read.csv("dbfs:/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/exposure_group/eyeball_dgs.csv",inferSchema=True, header=True).display()

# COMMAND ----------

def exp_dgs(test_store_sf, txn):
    # load the eyeball_dgs csv into a a Spark DataFrame
    eye_ball_dgs = spark.read.csv("dbfs:/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/exposure_group/eyeball_dgs.csv",inferSchema=True, header=True)
    # display the eye_ball_dgs DataFrame
    # eye_ball_dgs.display()
    
    # filter the test_store_df to remove null values and drop duplicates, and combine with eye_ball_dgs and txn DataFrames
    test_dgs = test_store_sf.where(F.col("dgs_min_p_hr").isNotNull())\
                            .drop_duplicates()\
                            .join(eye_ball_dgs, "store_id", "inner")\
                            .join(txn.select("store_id","store_region").dropDuplicates(), "store_id", "inner")\
                            .select("store_id","c_start","c_end","mech_name","mech_count","store_region","eyeball_per_min_day","dgs_min_p_hr")\
                            .drop_duplicates()\
                            .withColumn("no_date",F.expr("datediff(c_end, c_start) + 1"))\
                            .na.fill(0,subset=["eyeball_per_min_day"])\
                            .withColumn("eb_media",F.col("eyeball_per_min_day")* F.col("dgs_min_p_hr")*F.col("no_date") * F.col("mech_count"))\
                            .na.fill(0,subset=["eb_media"])
    # return the test_dgs DataFrame
    return test_dgs

# COMMAND ----------

## Change to use new code from p'Danny for exposure -- Pat 22 Jul 2023

#from exposure import exposed

cmp.txn = cmp.txn.withColumnRenamed("store_format_group", "store_format_name")

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

test_df = exp_dgs(in_trg_df,cmp.txn)

exp_dgs_all_df = test_df.agg(F.sum("eb_media").alias("epos_impression_dgs"))
exposure_all_df = to_pandas(exposure_all.join(exp_dgs_all_df).withColumn("epos_impression", F.col("epos_impression").cast("int")+F.col("epos_impression_dgs")))
pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))


exp_dgs_region_df = test_df.groupBy("store_region").agg(F.sum("eb_media").alias("epos_impression_dgs"))
exposure_region_df = to_pandas(exposure_reg.join(exp_dgs_region_df, on="store_region", how="left")\
                                            .na.fill(0,subset=['epos_impression_dgs'])\
                                           .withColumn("epos_impression", F.col("epos_impression").cast("int")+F.col("epos_impression_dgs")))
pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

exp_dgs_mech_df = test_df.groupBy("mech_name").agg(F.sum("eb_media").alias("epos_impression_dgs"))
exposure_mech_df = to_pandas(exposure_mech.join(exp_dgs_mech_df, on="mech_name", how="left")\
                                          .na.fill(0,subset=['epos_impression_dgs'])\
                                          .withColumn("epos_impression", F.col("epos_impression").cast("int")+F.col("epos_impression_dgs")))
pandas_to_csv_filestore(exposure_mech_df, 'exposure_mech.csv', prefix=os.path.join(eval_path_fl, cmp_month, cmp_nm, 'result'))

# COMMAND ----------

exposure_all_df

# COMMAND ----------

exposure_region_df

# COMMAND ----------

exposure_mech_df

# COMMAND ----------

## from utils import load_txn
## load_txn.load_txn(cmp)
#
### Change to use new code from p'Danny for exposure -- Pat 22 Jul 2023
#
##from exposure import exposed
#
#cmp.txn = cmp.txn.withColumnRenamed("store_format_group", "store_format_name")
#
#exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)
#
#exposure_all_df = to_pandas(exposure_all)
#pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(cmp_out_path, 'result'))
#
#exposure_region_df = to_pandas(exposure_reg)
#pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(cmp_out_path, 'result'))
#
#exposure_mech_df = to_pandas(exposure_mech)
#
#pandas_to_csv_filestore(exposure_mech_df, 'exposure_mech.csv', prefix=os.path.join(cmp_out_path, 'result'))

# COMMAND ----------

# ## Exposure using filter by date already - No need to check for week type 
# ## -- Pat Check code 8 Sep 2022

# cmp_st_date                   = datetime.strptime(cmp_start, '%Y-%m-%d')
# cmp_end_date                  = datetime.strptime(cmp_end, '%Y-%m-%d')
# exposure_all, exposure_region = get_awareness(txn_all, cp_start_date=cmp_st_date, cp_end_date=cmp_end_date,
#                                               store_fmt=store_fmt, test_store_sf=test_store_sf, adj_prod_sf=use_ai_df,
#                                               media_spend=float(media_fee))

# ## Pat add to handle "store_region === '' "
# exposure_region = exposure_region.select( F.when(trim(exposure_region.store_region) == '', lit('Unidentified'))
#                                            .otherwise(exposure_region.store_region)
#                                            .alias('store_region')
#                                          , exposure_region.epos_visits
#                                          , exposure_region.carded_visits
#                                          , exposure_region.non_carded_visits
#                                          , exposure_region.epos_impression
#                                          , exposure_region.carded_impression
#                                          , exposure_region.non_carded_impression
#                                          , exposure_region.carded_customers
#                                          , exposure_region.num_test_store
#                                          , exposure_region.num_all_test_stores
#                                          , exposure_region.all_media_spend
#                                          , exposure_region.region_media_spend
#                                          , exposure_region.carded_reach
#                                          , exposure_region.avg_carded_freq
#                                          , exposure_region.est_non_carded_reach
#                                          , exposure_region.total_reach
#                                          , exposure_region.CPM )

# exposure_all_df = to_pandas(exposure_all)
# pandas_to_csv_filestore(exposure_all_df, 'exposure_all.csv', prefix=os.path.join(cmp_out_path, 'result'))
# exposure_region_df = to_pandas(exposure_region)
# pandas_to_csv_filestore(exposure_region_df, 'exposure_region.csv', prefix=os.path.join(cmp_out_path, 'result'))
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Report : Customer Activated & Movement & Switching

# COMMAND ----------

#---- Customer Activated : Danny 1 Aug 2022
#---- Customer Activated : Pat update add spend of activated cust 3 Nov 2022

brand_activated, sku_activated, brand_activated_sales_df, sku_activated_sales_df  = get_cust_activated(txn_all, 
                                                                                                       cmp_start, 
                                                                                                       cmp_end,
                                                                                                       week_type, 
                                                                                                       test_store_sf, 
                                                                                                       adj_prod_sf,
                                                                                                       brand_df, 
                                                                                                       feat_df)

sku_activated.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'tdm_dev.media_camp_eval_{cmp_id}_cust_sku_activated')
brand_activated.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'tdm_dev.media_camp_eval_{cmp_id}_cust_brand_activated')

sku_activated = spark.table(f"tdm_dev.media_camp_eval_{cmp_id}_cust_sku_activated")
brand_activated = spark.table(f"tdm_dev.media_camp_eval_{cmp_id}_cust_brand_activated")

# n_brand_activated = brand_activated.count()
# n_sku_activated= sku_activated.count()

# activated_df = pd.DataFrame({'customer_exposed_brand_activated':[n_brand_activated], 'customer_exposed_sku_activated':[n_sku_activated]})

##-------------------------------------------------------------------------------
## change to export from output dataframe will all value separated brand and SKU
##-------------------------------------------------------------------------------

## Brand
brand_activated_info_pd = brand_activated_sales_df.toPandas()

pandas_to_csv_filestore(brand_activated_info_pd, 'customer_exposed_activate_brand.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

## SKU
sku_activated_info_pd = sku_activated_sales_df.toPandas()

pandas_to_csv_filestore(sku_activated_info_pd, 'customer_exposed_activate_sku.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

##---------------------------------
## Add Old format file (brand & feature in the same file )
##---------------------------------
## Initial Dataframe
activated_df                      = pd.DataFrame()

activated_df['customer_exposed_brand_activated'] = brand_activated_info_pd['brand_activated_cust_cnt']
activated_df['customer_exposed_sku_activated']   = sku_activated_info_pd['sku_activated_cust_cnt']

pandas_to_csv_filestore(activated_df, 'customer_exposed_activate.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

print('#'*80)
print('# Activated customer at Brand and SKU level Show below. ')
print('#'*80)
print(activated_df)
print('#'*80 + '\n')


# COMMAND ----------

## REmove parameter here as move to setup in date parameter setup in box above already - Pat 20 Sep 2022

# if wk_type == 'fis_wk':
#     week_type = 'fis_week'
# elif wk_type == 'promo_wk':
#     week_type = 'promo_week'
# ## end if

# # #---- Customer Activated : Danny 1 Aug 2022

# brand_activated, sku_activated = get_cust_activated(txn_all, 
#                                                     cmp_start, 
#                                                     cmp_end,
#                                                     week_type, 
#                                                     test_store_sf, 
#                                                     adj_prod_sf,
#                                                     brand_df, 
#                                                     feat_df)

# sku_activated.write.format('parquet').mode('overwrite').saveAsTable(f'tdm_seg.media_camp_eval_{cmp_id}_cust_sku_activated')
# brand_activated.write.format('parquet').mode('overwrite').saveAsTable(f'tdm_seg.media_camp_eval_{cmp_id}_cust_brand_activated')

# sku_activated = spark.table(f"tdm_seg.media_camp_eval_{cmp_id}_cust_sku_activated")
# brand_activated = spark.table(f"tdm_seg.media_camp_eval_{cmp_id}_cust_brand_activated")

# n_brand_activated = brand_activated.count()
# n_sku_activated= sku_activated.count()

# activated_df = pd.DataFrame({'customer_exposed_brand_activated':[n_brand_activated], 'customer_exposed_sku_activated':[n_sku_activated]})

# pandas_to_csv_filestore(activated_df, 'customer_exposed_activate.csv', prefix=os.path.join(cmp_out_path, 'result'))

#---- Customer Activated by Mechanic : Ta 21 Sep 2022

# brand_activated_by_mech, sku_activated_by_mech, agg_numbers_by_mech = get_cust_activated_by_mech(txn=txn_all, 
#                                                                                                  cp_start_date=cmp_start, 
#                                                                                                  cp_end_date=cmp_end,
#                                                                                                  wk_type=wk_type, 
#                                                                                                  test_store_sf=test_store_sf, 
#                                                                                                  adj_prod_sf=use_ai_df,
#                                                                                                  brand_sf=brand_df, 
#                                                                                                  feat_sf=feat_df)

# sku_activated_by_mech.write.format('parquet').mode('overwrite').saveAsTable(f'tdm_seg.media_camp_eval_{cmp_id}_cust_sku_activated_by_mech')
# brand_activated_by_mech.write.format('parquet').mode('overwrite').saveAsTable(f'tdm_seg.media_camp_eval_{cmp_id}_cust_brand_activated_by_mech')

# sku_activated_by_mech = spark.table(f"tdm_seg.media_camp_eval_{cmp_id}_cust_sku_activated_by_mech")
# brand_activated_by_mech = spark.table(f"tdm_seg.media_camp_eval_{cmp_id}_cust_brand_activated_by_mech")

# agg_numbers_by_mech_pd = agg_numbers_by_mech.toPandas()

# pandas_to_csv_filestore(agg_numbers_by_mech_pd, 'customer_exposed_activate_by_mech.csv', prefix=os.path.join(cmp_out_path, 'result'))

#---- Customer switching : Danny 1 Aug 2022
cust_mv, new_sku = get_cust_movement(txn=txn_all,
                                     wk_type=week_type,
                                     feat_sf=feat_df,
                                     sku_activated=sku_activated,
                                     class_df=class_df,
                                     sclass_df=sclass_df,
                                     brand_df=brand_df,
                                     switching_lv=cate_lvl)

cust_mv.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'tdm_dev.media_camp_eval_{cmp_id}_cust_mv')

## Save customer movement
cust_mv_count = cust_mv.groupBy('customer_macro_flag', 'customer_micro_flag').count().orderBy('customer_macro_flag', 'customer_micro_flag')
cust_mv_count_df = to_pandas(cust_mv_count)
pandas_to_csv_filestore(cust_mv_count_df, 'customer_movement.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

#----- Customer brand switching & brand penetration : Danny 1 Aug 2022
cust_mv = spark.table(f'tdm_dev.media_camp_eval_{cmp_id}_cust_mv')
cust_brand_switching, cust_brand_penetration, cust_brand_switching_and_pen = \
get_cust_brand_switching_and_penetration(
    txn=txn_all,
    switching_lv=cate_lvl, 
    brand_df=brand_df,
    class_df=class_df,
    sclass_df=sclass_df,
    cust_movement_sf=cust_mv,
    wk_type=week_type)
cust_brand_switching_and_pen_df = to_pandas(cust_brand_switching_and_pen)
pandas_to_csv_filestore(cust_brand_switching_and_pen_df, 'customer_brand_switching_penetration.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

#---- Customer brand switching & penetration : danny 20 Sep 2022
cust_brand_switching_and_pen_muli = \
get_cust_brand_switching_and_penetration_multi(
    txn=txn_all,
    switching_lv=cate_lvl,
    brand_df=brand_df,
    class_df=class_df,
    sclass_df=sclass_df,
    cate_df=cate_df,
    cust_movement_sf=cust_mv,
    wk_type=week_type)
cust_brand_switching_and_pen_muli_df = to_pandas(cust_brand_switching_and_pen_muli)
pandas_to_csv_filestore(cust_brand_switching_and_pen_muli_df, 'customer_brand_switching_penetration_multi.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

#---- Customer SKU switching : Danny 1 Aug 2022
sku_switcher = get_cust_sku_switching(txn=txn_all, 
                                      switching_lv=cate_lvl, 
                                      sku_activated=sku_activated,
                                      feat_list=feat_list,
                                      class_df=class_df,
                                      sclass_df=sclass_df,
                                      wk_type=week_type)

cust_sku_switching_df = to_pandas(sku_switcher)
pandas_to_csv_filestore(cust_sku_switching_df, 'customer_sku_switching.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md ## Add detail of "New to category" customers to solution  ## 20 Jun 2022 - Pat

# COMMAND ----------

## --------------------------------------------
## call function "get_new_to_cate" in util-1
## --------------------------------------------

cate_info_df, cate_brand_info = get_new_to_cate(txn_all,cust_mv, wk_type )

## Export to file in output path (not result path)

cate_info_pd = cate_info_df.toPandas()

pandas_to_csv_filestore(cate_info_pd, 'category_info_from_new_to_category_customers.csv', 
                        prefix=os.path.join(cmp_out_path_fl, 'result'))

del cate_info_df
del cate_info_pd    

##------------------
cate_brand_info_pd = cate_brand_info.toPandas()

pandas_to_csv_filestore(cate_brand_info_pd, 'brand_info_from_top5cate_new_to_cate_customers.csv', 
                        prefix=os.path.join(cmp_out_path_fl, 'output'))



del cate_brand_info
del cate_brand_info_pd

# COMMAND ----------

# MAGIC %md ## Additional Customer profile

# COMMAND ----------

# MAGIC %md ### Customer region from customer prefer stores

# COMMAND ----------

## Initial table 

sku_atv_cst   = spark.table(f'tdm_dev.media_camp_eval_{cmp_id}_cust_sku_activated')
brand_atv_cst = spark.table(f'tdm_dev.media_camp_eval_{cmp_id}_cust_brand_activated')
cst_pfr_seg   = spark.table('tdm.srai_prefstore_full_history')

## Get max period of customer segment
mx_period     = cst_pfr_seg.agg(max(cst_pfr_seg.period_id).alias('mxp')).collect()[0].mxp

cst_pfr_seg_c = cst_pfr_seg.where(cst_pfr_seg.period_id == mx_period).persist()


# COMMAND ----------

## call function to get output pandas df

## SKU Level

grp_str_reg_sku_pd, grp_reg_sku_pd = get_atv_cust_region(sku_atv_cst, cst_pfr_seg_c, 'sku')

## Export output

pandas_to_csv_filestore(grp_reg_sku_pd, 'atv_sku_cust_region.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
pandas_to_csv_filestore(grp_str_reg_sku_pd, 'atv_sku_cust_by_format_region.csv', prefix=os.path.join(cmp_out_path_fl, 'output'))

del grp_reg_sku_pd, grp_str_reg_sku_pd

## Brand Level

grp_str_reg_brand_pd, grp_reg_brand_pd = get_atv_cust_region(brand_atv_cst, cst_pfr_seg_c, 'brand')

pandas_to_csv_filestore(grp_reg_brand_pd, 'atv_brand_cust_region.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
pandas_to_csv_filestore(grp_str_reg_brand_pd, 'atv_brand_cust_by_format_region.csv', prefix=os.path.join(cmp_out_path_fl, 'output'))

del grp_reg_brand_pd, grp_str_reg_brand_pd


# COMMAND ----------

#%md ## Customer Share - Pre/during of Test Store >> feature & brand 
#-- add back 21 Jul 2022 from AE request, to see if there any metric to see from standard eval.  DS need consider to show result.

# COMMAND ----------

# kpi_spdf, kpi_pd, cust_share_pd = cust_kpi_noctrl(txn_all ,store_fmt , trg_str_df, feat_list, brand_df, cate_df)

# kpi_pd.display()

# cust_share_pd.display()

# ## export File cust share & KPI

# pandas_to_csv_filestore(kpi_pd, f'all_kpi_in_category_no_control.csv', prefix=os.path.join(dbfs_project_path, 'result'))
# pandas_to_csv_filestore(cust_share_pd, f'cust_share_target_promo.csv', prefix=os.path.join(dbfs_project_path, 'result'))

# COMMAND ----------

# MAGIC %md ### Profile TruPrice

# COMMAND ----------

truprice_profile = get_profile_truprice(txn=txn_all, 
                                        store_fmt=store_fmt,
                                        cp_end_date=cmp_end,
                                        wk_type=week_type,
                                        sku_activated=sku_activated,
                                        switching_lv=cate_lvl,
                                        class_df=class_df,
                                        sclass_df=sclass_df,
)
truprice_profile_df = to_pandas(truprice_profile)
pandas_to_csv_filestore(truprice_profile_df, 'profile_sku_activated_truprice.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md ## Sales growth & Marketshare growth (Pre/During) for all eval to see if there is a better result
# MAGIC -- Added back 21 Jul 2022 from AE request but need DS consider to show result (Pat)

# COMMAND ----------

## Do this for standard evaluation, if full will do with control store
## Brand at class

sales_brand_class_fiswk   = get_sales_mkt_growth_noctrl( txn_all
                                                           ,brand_df
                                                           ,class_df
                                                           ,'brand'
                                                           ,'class'
                                                           ,wk_type
                                                           ,store_fmt
                                                           ,trg_str_df
                                                          )
   
## brand at sublcass
sales_brand_subclass_fiswk = get_sales_mkt_growth_noctrl( txn_all
                                                           ,brand_df
                                                           ,sclass_df
                                                           ,'brand'
                                                           ,'subclass'
                                                           ,wk_type
                                                           ,store_fmt
                                                           ,trg_str_df
                                                          )

## feature at class
sales_sku_class_fiswk      = get_sales_mkt_growth_noctrl( txn_all
                                                          ,feat_df
                                                          ,class_df
                                                          ,'sku'
                                                          ,'class'
                                                          ,wk_type
                                                          ,store_fmt
                                                          ,trg_str_df
                                                         )
## feature at subclass
sales_sku_subclass_fiswk    = get_sales_mkt_growth_noctrl( txn_all
                                                            ,feat_df
                                                            ,sclass_df
                                                            ,'sku'
                                                            ,'subclass'
                                                            ,wk_type
                                                            ,store_fmt
                                                            ,trg_str_df
                                                           )

## Export File sales market share growth
## Flook code 
mech_growth = get_sales_mkt_growth_per_mech(
    txn_all, wk_type, store_fmt, trg_str_df, feat_list, brand_df, cate_df, cate_lvl
)

#wk_tp = wk_type.replace('_', '## setup in box date parameter already  -- Pat 20 Sep 2022
mech_growth_pd = mech_growth.toPandas()

pandas_to_csv_filestore(mech_growth_pd, 'growth_by_mech.csv', prefix=os.path.join(dbfs_project_path, 'result'))

pandas_to_csv_filestore(sales_brand_class_fiswk, 'sales_brand_class_growth_target_'+ wk_tp + '.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_brand_subclass_fiswk, 'sales_brand_subclass_growth_target_'+ wk_tp + '.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_sku_class_fiswk, 'sales_sku_class_growth_target_'+ wk_tp + '.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(sales_sku_subclass_fiswk, 'sales_sku_subclass_growth_target_'+ wk_tp + '.csv', prefix=os.path.join(dbfs_project_path, 'result'))


# COMMAND ----------

# MAGIC %md ## Target store weekly sales trend

# COMMAND ----------

# get_sales_trend_trg(intxn
#                        ,trg_store_df
#                        ,prod_df
#                        ,prod_lvl
#                        ,week_type
#                        ,period_col
#                        ):

## SKU Sales trend in spark df format 

sku_trend_trg   = get_sales_trend_trg(txn_all, trg_str_df, feat_df, 'SKU', wk_type, 'period_' + wk_type)

## Brand sales trend
brand_trend_trg = get_sales_trend_trg(txn_all, trg_str_df, brand_df, 'Brand', wk_type, 'period_' + wk_type)

## Category sales trend
cate_trend_trg  = get_sales_trend_trg(txn_all, trg_str_df, cate_df, 'Category', wk_type, 'period_' + wk_type)


# COMMAND ----------

## Convert to pandas and write out

pd_sku_trend_trg   = sku_trend_trg.toPandas()
pd_brand_trend_trg = brand_trend_trg.toPandas()
pd_cate_trend_trg  = cate_trend_trg.toPandas()

## cmp_out_path_fl

# sku_file           = cmp_out_path_fl + 'weekly_sales_trend_promowk_sku.csv'
# brand_file         = cmp_out_path_fl + 'weekly_sales_trend_promowk_brand.csv'
# cate_file          = cmp_out_path_fl + 'weekly_sales_trend_promowk_cate.csv'


pandas_to_csv_filestore(pd_sku_trend_trg, 'weekly_sales_trend_' + wk_tp + '_sku.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(pd_brand_trend_trg, 'weekly_sales_trend_' + wk_tp + '_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(pd_cate_trend_trg, 'weekly_sales_trend_' + wk_tp + '_cate.csv', prefix=os.path.join(dbfs_project_path, 'result'))

# COMMAND ----------

# MAGIC %md ## Customer KPI No Control -- #weeks in pre/during is not the same, result will not be trustable

# COMMAND ----------

# def cust_kpi_noctrl(txn
#                     ,store_fmt
#                     ,test_store_sf
#                     ,feat_list
#                     ,brand_df
#                     ,cate_df
#                     ):
#     """Promo-eval : customer KPIs Pre-Dur for test store
#     - Features SKUs
#     - Feature Brand in subclass
#     - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
#     - Category dataframe (all SKU in category at defined switching level)
#     - Return 
#       >> combined_kpi : spark dataframe with all combine KPI
#       >> kpi_df : combined_kpi in pandas
#       >> df_pv : Pivot format of kpi_df
#     """ 
## use based on p'Danny function & Pat Adjust  -- Pat 17 Jun 2022

## Pat Add for GO Fresh Evaluation 24 AUg 2022
### 
## Enable - kpi no control for campaign evaluation type 
##if eval_type == 'std':

### Scent add 2024-01-20 utils 3 
if wk_type == 'fis_wk' :
    kpi_spdf, kpi_pd, cust_share_pd = cust_kpi_noctrl_fiswk_dev(txn_all 
                                                           ,store_fmt 
                                                           ,trg_str_df
                                                           ,feat_list
                                                           ,brand_df
                                                           ,cate_df)
    kpi_pd.display()
    cust_share_pd.display()
elif wk_type == 'promo_wk':
    kpi_spdf, kpi_pd, cust_share_pd = cust_kpi_noctrl_pm_dev( txn_all 
                                                      ,store_fmt 
                                                      ,trg_str_df
                                                      ,feat_list
                                                      ,brand_df
                                                      ,cate_df)
 
    kpi_pd.display() 
    cust_share_pd.display()
## end if

## export File cust share & KPI

pandas_to_csv_filestore(kpi_pd, f'all_kpi_in_category_no_control.csv', prefix=os.path.join(dbfs_project_path, 'result'))
pandas_to_csv_filestore(cust_share_pd, f'cust_share_target.csv', prefix=os.path.join(dbfs_project_path, 'result'))
## end if

# COMMAND ----------

# MAGIC %md # Step for Ozone Cust group - combine code

# COMMAND ----------

# MAGIC %md ## Recheck transaction & prep table name 

# COMMAND ----------

## txn_tab_nm (add another table partition by 'hh_id') 
#txn_o3_tab    = 'tdm_dev.media_campaign_eval_txn_data_o3_' + cmp_id.lower()  
## table for customer table grouping
cst_ishm_tab   = 'tdm_dev.media_campaign_eval_cust_ishm_data_o3_' + cmp_id.lower()
cst_isstr_tab  = 'tdm_dev.media_campaign_eval_cust_isstr_data_o3_' + cmp_id.lower()
cst_isall_tab  = 'tdm_dev.media_campaign_eval_cust_isall_data_o3_' + cmp_id.lower()
cst_dgs_tab    = 'tdm_dev.media_campaign_eval_cust_dgs_data_o3_' + cmp_id.lower()
cst_isdgs_tab  = 'tdm_dev.media_campaign_eval_cust_isdgs_data_o3_' + cmp_id.lower()
cst_ca_all_tab = 'tdm_dev.media_campaign_eval_cust_ca_all_data_o3_' + cmp_id.lower()
## cust target all
cst_trg_tab   = 'tdm_dev.media_campaign_eval_cust_target_all_data_o3_' + cmp_id.lower()

## control - set 1
cst_nis_tab      = 'tdm_dev.media_campaign_eval_cust_nis_data_o3_' + cmp_id.lower()
cst_ntrg_hde_tab = 'tdm_dev.media_campaign_eval_cust_ntrg_hde_data_o3_' + cmp_id.lower()
cst_ntrg_oth_tab = 'tdm_dev.media_campaign_eval_cust_ntrg_oth_data_o3_' + cmp_id.lower()
## cust control all
cst_ctl_tab   = 'tdm_dev.media_campaign_eval_cust_control_all_data_o3_' + cmp_id.lower()



# COMMAND ----------

print('=' * 80)
print('Initial Transaction table = ' + str(txn_tab_nm))
print('=' * 80)

try :
  ctxn     = spark.table(txn_tab_nm)
  print(' Default campaign transaction table from O3 exist process will use initial data from : ' + txn_tab_nm + ' \n')
except:
  print(' Default campaign transaction table from instore and O3 does not exist!! \n')
  print(' Process will create o3 transaction table ' + txn_tab + '\n')
  ## call function to create table
  if wk_type == 'fis_wk':
    txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_wk, end_week_id=cmp_en_wk, store_format=[1,2,3,4,5], 
                                  prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
                                                     'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                                     'department_code', 'section_code', 'class_code', 'subclass_code'])
  elif wk_type == 'promo_wk':
    txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_promo_wk, end_week_id=cmp_en_promo_wk, store_format=[1,2,3,4,5], 
                                  prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
                                                     'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                                     'department_code', 'section_code', 'class_code', 'subclass_code'])
  ## end if
## end except

## additional column based on household_id not cc_flag

ctxn    = txn_all.withColumn('hh_flag',F.when(txn_all.household_id.isNull(), lit(0))
                                        .otherwise(lit(1)))

# ## save table
# sqltxt = 'drop table if exists ' + txn_o3_tab
# spark.sql(sqltxt)

# ctxn.write.mode('overwrite').partitionBy('hh_flag').saveAsTable(txn_o3_tab)
# ## Pat add, delete dataframe before re-read
# print('Save txn table partition by hh_flag on table "' : + str(txn_o3_tab) + '" \n')
# del ctxn
# ## Re-read from table
# ctxn = spark.table(txn_o3_tab)

# COMMAND ----------

# MAGIC %md ## Get "during" period transaction table

# COMMAND ----------

trg_str_ddup   = trg_str_df.select('store_id').dropDuplicates()

## join target store to get store_flag, all format, all channel 
ctxnp          = ctxn.join  (trg_str_ddup, ctxn.store_id == trg_str_ddup.store_id, 'left')\
                     .select( ctxn['*']
                             ,F.when(trg_str_ddup.store_id.isNotNull(), lit(1))
                               .otherwise(lit(0))
                               .alias('trg_str_flag')
                             )
## get cc_txn during & pre period

cctxn_dur     = ctxnp.where((ctxnp.hh_flag == 1)  & (ctxn.date_id.between(cmp_start, cmp_end)))

# COMMAND ----------

# MAGIC %md ### Function to get transaction at product level

# COMMAND ----------

def get_prod_txn(intxn, prd_df ):
  ## having 2 inputs parameter
  ##  > txn   : spark Dataframe : transaction table 
  ##  > prd_df : spark Dataframe : contain upc_id (product)
  ##
  ## Return a spark data frame contain transaction of all product in prd_df
  
  otxn = intxn.join(prd_df, intxn.upc_id == prd_df.upc_id, 'left_semi')
  
  return otxn

## end def

# COMMAND ----------

# MAGIC %md ### Function to get household_id customers

# COMMAND ----------

def get_cdf (intxn, prd_lvl):
  ## have 2 input parameters
  ## intxn : sparkDataframe : cc transaction table
  ## prd_lvl : string: product level for labet customers column
  ##
  ## return 1 output dataframe 
  ## c_df   : sparkDataframe contain distinct customers of cc transaction
  
  c_df = intxn.where(intxn.hh_flag == 1)\
              .select( intxn.household_id.alias(prd_lvl+'_h_id'))\
              .dropDuplicates()
  
  return c_df
## end def

# COMMAND ----------

# MAGIC %md ## Get Transaction at product Level

# COMMAND ----------

## get brand 
bnd_txn_dur   = get_prod_txn(cctxn_dur, brand_df)

## get feature sku 
sku_txn_dur   = get_prod_txn(cctxn_dur, feat_df)


# COMMAND ----------

# MAGIC %md ## Check if Having online media

# COMMAND ----------

## This block for CA data read will skip if no online campaign --> ca_flag <> 1


if int(ca_flag) == 1 :
    
    print('=' *80)
    print('This campaign has "online" campaign, will evaluate with CA customer group')
    print('=' *80)

    use_ca_tab_flg = float(use_ca_tab)
    
    if use_ca_tab_flg == 1 : 
    
        mca     = spark.table('tdm_dev.consolidate_ca_master').where(F.col('cmp_id') == cmp_id_online)
        ca_grp  = spark.table('tdm_dev.consolidate_ca_group').where(F.col('cmp_id') == cmp_id_online)
        #cmp_inf = spark.table('tdm_dev.consolidate_ca_campaign')
        
        ## get target & control ca
        
        ## Add check if can find CA from consol CA table.
        mca_chk = mca.limit(1).count()
        #print(mca_chk)
        
        if mca_chk == 1:
            print(' Online Campaign ' + str(c_id_oln) + ' having CA from Consolidate CA table. \n')
        else :  
            raise Exception('Online Campaign ' + str(c_id_oln) + ' Do not found CA in Consolidate CA table. \n' '!! Code will skip.')
        ## end if
        
        mca_grp = mca.join  (ca_grp, [ (mca.cmp_id == ca_grp.cmp_id) & (mca.group == ca_grp.group)], 'inner' )\
                     .select( mca.cmp_id
                             ,mca.golden_record_external_id_hash.alias('crm_id')
                             ,mca.group.alias('ca_grp_id')
                             ,concat_ws('_',mca.group,ca_grp.group_name).alias('ca_group')
                             ,mca.flag_type.alias('c_type')                    
                            )
        
        ## Separate Test/Control & drop duplicate within group 
        trg_df  = mca_grp.where(mca_grp.c_type == 'test').dropDuplicates()  ## Drop duplicate within group
        ctl_df   = mca_grp.where(mca_grp.c_type == 'con').dropDuplicates()
        
        #trg_df.limit(20).display()
        
        ## check if customer duplicate between group
        
        cnt_df   = trg_df.groupBy(trg_df.crm_id)\
                         .agg    (count(trg_df.ca_group).alias('cnt_grp'))
        
        ## get hh_id which has duplicate between group
        dup_df   = cnt_df.where(cnt_df.cnt_grp > 1)\
                         .select( cnt_df.crm_id)
        
        #dup_df.limit(10).display()
        
        ## join back to get customer group and export for checking later
        
        dup_trg    = trg_df.join(dup_df, 'crm_id', 'left_semi')
        
        dup_trg_pd = dup_trg.toPandas()
        
        ## Export product info until brand level to file
        
        file_nm   = cmp_nm + '_target_CA_X_group_duplicate.csv'
        full_nm   = cmp_out_path_fl + file_nm
        
        #dup_trg_pd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')
        pandas_to_csv_filestore(dup_trg_pd, file_nm, prefix=os.path.join(cmp_out_path_fl, 'output'))

        ## get number of duplicate customer to show
        cc_dup = dup_df.agg (sum(lit(1)).alias('hh_cnt')).collect()[0].hh_cnt 
        
        if cc_dup is None :
          cc_dup = 0
        ## end if
        
        print('='*80)
        print('\n Number of target CA duplicate between group = ' + str(cc_dup) + ' household_id. \n ')
        print('='*80)
        
        print('Export Duplicate Target CA to : ' + str(full_nm))
        
        ## 12 Jul 2023 Change to select group which come first as group of CA in case of dupliate  -- Pat 
        
        ## use sorting & group by
        
        trg_df = trg_df.groupBy(trg_df.crm_id)\
                       .agg    ( max(trg_df.cmp_id).alias('cmp_id')
                                ,min(trg_df.ca_group).alias('ca_group')
                                ,lit('trg').alias('c_type')
                               )
        
        ## also de-dup control customer but don't export duplicate to show out  -- Pat 12 Jul 2023
        
        ctl_df = ctl_df.groupBy(ctl_df.crm_id)\
                       .agg    ( max(ctl_df.cmp_id).alias('cmp_id')
                                ,min(ctl_df.ca_group).alias('ca_group')
                                ,lit('ctl').alias('c_type')
                               )
        
        # trg_df.limit(10).display()
        # ctl_df.limit(10).display()
    
    else :  ## use previous solution, read from file (Use txn flag == 0 or specific others beside 1 )
       
        ##---------------------
        ## Use Target CA File
        ##---------------------
        
        files      = dbutils.fs.ls(input_path + 'target_ca/')  ## get path in spartk API
        print(' Look for file in path ' + input_path + 'target_ca/' + ' \n')
        
        ## setup file filter
        txt_begin  = cmp_id_online + '_target_ca_'
        print(' Filter "Targeting CA file " only file begin with : ' + txt_begin + '\n')
        
        files_list = [f.path for f in files]
        #print(files_list)
        use_file   = [uf for uf in files_list if txt_begin in uf ]
        #print(use_file)
        
        nfiles     = len(use_file)
        
        if nfiles == 0 :
          print(' No "Target CA" file for campaign ' + cmp_nm + '!! Code will skip.')
          raise Exception('No "Target CA" file for campaign ' + cmp_nm + '!! Code will skip.')
        ## end if
        
        print(' Number of Target CA file = ' + str(nfiles) + ' files.')
        
        trg_df     = spark.createDataFrame([], StructType())
        
        ## read & combine data
        for cfile in use_file: 
          
          print(cfile)  
          
          ## add step get CA group name from file name
          
          f_ln   = len(cfile)
          kywd   = '_target_ca_'
          idx    = cfile.find(kywd)
          st_idx = idx + len(kywd)
          get_wd = cfile[st_idx : -4 ] ## remove .csv
          grp_nm = ''.join(t for t in get_wd if(t.isalnum() or t == '_'))
          
          ##------------------------------------------
          
          fr_df  = spark.read.csv(cfile, header=True, inferSchema=True)
          fr_dfe = fr_df.select    ( fr_df.golden_record_external_id_hash.alias('crm_id')
                                    ,lit('trg').alias('c_type')
                                    ,lit(grp_nm).alias('ca_group')
                                   )
          
          trg_df = trg_df.unionByName(fr_dfe, allowMissingColumns=True)
        
        ## end for
        
        ## drop duplicates (within group)
        trg_df   = trg_df.dropDuplicates()
        
        ## check if customer duplicate between group
        
        cnt_df   = trg_df.groupBy(trg_df.crm_id)\
                         .agg    (count(trg_df.ca_group).alias('cnt_grp'))
        
        ## get hh_id which has duplicate between group
        dup_df   = cnt_df.where(cnt_df.cnt_grp > 1)\
                         .select(cnt_df.crm_id)
        
        ## join back to get customer group and export for checking later
        
        dup_trg    = trg_df.join(dup_df, 'crm_id', 'left_semi')
        
        dup_trg_pd = dup_trg.toPandas()
        
        ## Export product info until brand level to file
        
        file_nm   = cmp_nm + '_target_CA_X_group_duplicate.csv'
        full_nm   = cmp_out_path_fl + file_nm
        
        #dup_trg_pd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')
        pandas_to_csv_filestore(dup_trg_pd, file_nm, prefix=os.path.join(cmp_out_path_fl, 'output'))
        
        ## get number of duplicate customer to show
        cc_dup = dup_df.agg (sum(lit(1)).alias('hh_cnt')).collect()[0].hh_cnt 
        
        if cc_dup is None :
          cc_dup = 0
        ## end if
        
        print('='*80)
        print('\n Number of target CA duplicate between group = ' + str(cc_dup) + ' household_id. \n ')
        print('='*80)
        
        print('Export Duplicate Target CA to : ' + str(full_nm))
        
        ## -----------------------------------------------------
        ## 7 Apr 2023  - Add remove customers which has duplicated across CA group from analysis first. -- Pat
        ## -----------------------------------------------------
        
        trg_df = trg_df.join(dup_df, 'crm_id', 'left_anti')  ## not included duplicates customers
        
        #trg_df.display()
        ##  
        
        ##---------------------
        ## Control CA
        ##---------------------
        files      = dbutils.fs.ls(input_path + 'control_ca/')  ## get path in spartk API
        
        print(' Look for file in path ' + input_path + 'control_ca/' + ' \n')
        
        ## setup file filter
        txt_begin  = cmp_id_online + '_control_ca_'
        print(' Filter "Control CA file " only file begin with : ' + txt_begin + '\n')
        
        files_list = [f.path for f in files]
        use_file   = [uf for uf in files_list if txt_begin in uf ]
        #print(use_file)
        
        nfiles     = len(use_file)
        
        if nfiles == 0 :
          print(' No "Control CA" file for campaign ' + cmp_nm + '!! Code will skip.')
          raise Exception('No "Control CA" file for campaign ' + cmp_nm + '!! Code will skip.')
        ## end if
        
        print(' Number of Control CA file = ' + str(nfiles) + ' files.')
        
        ctl_df     = spark.createDataFrame([], StructType())
        
        ## read & combine data
        for cfile in use_file: 
          
          print(cfile)  
          
          fr_df  = spark.read.csv(cfile, header=True, inferSchema=True)
          fr_dfe = fr_df.select    ( fr_df.golden_record_external_id_hash.alias('crm_id')
                                    ,lit('ctl').alias('c_type')
                                   )
          
          ctl_df = ctl_df.unionByName(fr_dfe, allowMissingColumns=True)
        
        ## end for
        ##-----------------------------------
        ## drop duplicate
        ##-----------------------------------
        
        ## As of 5 Apr 2023 : Pat still not separated CA for each target CA group
        
        ctl_df = ctl_df.dropDuplicates()
        
        ## Add step to remove control CA which duplicate with Target CA out from group -- 25 May 2023 (Pat)
        
        ctl_df = ctl_df.join(trg_df, [ctl_df.crm_id == trg_df.crm_id], 'left_anti')
        
        #ctl_df.display()
        
        ##---------------------
        ## Offline Conversion CA
        ##---------------------
        files      = dbutils.fs.ls(input_path + 'offline_cvn/')  ## get path in spartk API
        
        print(' Look for file in path ' + input_path + 'offline_cvn/' + ' \n')
        
        ## setup file filter
        txt_begin  = cmp_id_online + '_offline_cvn_'
        
        print(' Filter "Offline conversion file " only file begin with : ' + txt_begin + '\n')
        
        files_list = [f.path for f in files]
        use_file   = [uf for uf in files_list if txt_begin in uf ]
        #print(use_file)
        
        nfiles     = len(use_file)
        
        if nfiles == 0 :
          print(' No "Offline Conversion" file for campaign ' + c_nm + '!! Code will skip.')
          raise Exception('No "Offline Conversion" file for campaign ' + c_nm + '!! Code will skip.')
        ## end if
        
        print(' Number of Offline Conversion File = ' + str(nfiles) + ' files.')
        
        cvn_df     = spark.createDataFrame([], StructType())
        
        ## read & combine data
        for cfile in use_file: 
          
          print(cfile)  
          
          fr_df  = spark.read.csv(cfile, header=True, inferSchema=True)
          fr_dfe = fr_df.select    ( fr_df.golden_record_external_id_hash.alias('crm_id'))
          
          cvn_df = cvn_df.unionByName(fr_dfe, allowMissingColumns=True)
        
        ## end for
        
        ## Drop duplicate from transaction , get only distinct CRM_ID

        cvn_df = cvn_df.dropDuplicates()

        ##---------------------
        ## get customer dim for household_id
        ##-----------------------

        cst_dim    = spark.table('tdm.v_customer_dim')

        ##---------------------
        
        cvn_df = cvn_df.join  (cst_dim, cvn_df.crm_id == cst_dim.golden_record_external_id_hash, 'inner')\
                       .select( cst_dim.household_id.alias('hh_id')
                               ,cvn_df.crm_id
                              )\
                       .dropDuplicates()

        ##---------------------
        ## Merge Target & control CA to join get customer id from cust dim
        ##---------------------
        
        ca_df      = trg_df.unionByName(ctl_df, allowMissingColumns=True)        

        ## join get customer id

        ca_df  = ca_df.join  (cst_dim, ca_df.crm_id == cst_dim.golden_record_external_id_hash, 'inner')\
                      .select( cst_dim.household_id.alias('hh_id')
                              ,ca_df.crm_id
                              ,ca_df.c_type
                              ,ca_df.ca_group
                             )\
                      .dropDuplicates()

    ## End if check Use CA Table
else:
    print('=' *80)
    print('This campaign has no "online" campaign, will evaluate !without! CA customers')
    print('=' *80)
## end if check ca_flag    

# COMMAND ----------

# MAGIC %md ## Get random CA to "Reach" customers

# COMMAND ----------

## For campaign with CA
flag_ca = 0

if int(ca_flag) == 1 :
    
    flag_ca = 1
    
    print('=' *80)
    print('Doing Ramdom select CA = "Reach" customers ')
    print('=' *80)
    
    ##----------------------------------------------------------------------
    ## Prep input number of customers to get random
    ##----------------------------------------------------------------------

    ca_df_trg     = ca_df.where(ca_df.c_type == 'trg')
    ca_df_ctl     = ca_df.where(ca_df.c_type == 'ctl')

    ## get # total target CA
    ca_cnt        = ca_df_trg.count()
    reach_use     = float(reach_online)
    media_fee_oln = float(media_fee_online)

    print('Total target CA = ' + str(ca_cnt))
    print('Total Reach     = ' + str(reach_use))

    bnd_cst_d_df  = get_cdf(bnd_txn_dur, 'brand')

    bnd_atv_dur = ca_df_trg.join(bnd_cst_d_df, ca_df_trg.hh_id == bnd_cst_d_df.brand_h_id, 'left_semi')
    oth_ca      = ca_df_trg.join(bnd_cst_d_df, ca_df_trg.hh_id == bnd_cst_d_df.brand_h_id, 'left_anti')

    ## Brand offline conversion during
    bnd_atv_cnt = bnd_atv_dur.agg(sum(lit(1)).alias('cst_cnt')).collect()[0].cst_cnt

    ## Offline conversion from n'Nhung  -- Pat add 9 Apr 2023
    cvn_cnt     = cvn_df.agg(sum(lit(1)).alias('cst_cnt')).collect()[0].cst_cnt
    
    if bnd_atv_cnt < cvn_cnt:  
        bnd_atv_cnt = cvn_cnt
        bnd_atv_dur = ca_df_trg.join(cvn_df, ca_df_trg.hh_id == cvn_df.hh_id, 'left_semi')
        oth_ca      = ca_df_trg.join(cvn_df, ca_df_trg.hh_id == cvn_df.hh_id, 'left_anti')
    # end if
    
    pcst_cost_ca    = media_fee_oln/ca_cnt
    
    print('Per cust cost for online targeted customers = ' + str(pcst_cost_ca) + ' THB \n' )
    print('-'*80)
    ## ------------------------------------------------------
    ## Estimate % conversion customers which might not reach
    pct_reach   = (reach_use/ca_cnt) + 0.2
    bnd_act_rnd = math.ceil(bnd_atv_cnt * pct_reach)
    print('%Reach number customer + 0.2                                   = ' + str(pct_reach))
    print('Number of Total brand activate                                 = ' + str(bnd_atv_cnt))
    print('Number of Brand activation customers decrease to to get random = ' + str(bnd_act_rnd) )

    ## ------------------------------------------------------
    
    ## Get random brand activate to % brand activate conversion
    rnd_bnd_dur = bnd_atv_dur.orderBy(rand()).limit(int(bnd_act_rnd))
    ca_random   = float(reach_use - bnd_act_rnd)

    ## get percentage to put in select random

    ca_rnd_pct  = ca_random/reach_use
    print('-' * 80)
    print('Number of CA activated at brand level      = ' + str(bnd_atv_cnt))
    print('Number of CA activated at brand level rand = ' + str(int((bnd_act_rnd))))
    print('Number of online Reach customer            = ' + str(reach_use) + ' custs')
    print('Total CA to random select                  = ' + str(ca_random) + ' custs or equal to ' + str(ca_rnd_pct) + ' percent from reach.')
    print('-' * 80)

    rand_ca = oth_ca.orderBy(rand()).limit(int(ca_random))
    #rand_ca.limit(10).display()    
    ca_rdf  = rand_ca.union(rnd_bnd_dur)\
                     .union(ca_df_ctl)\
                     .distinct()
    
    ## save used to table
    sqltxt  = 'drop table if exists ' + cst_ca_all_tab

    spark.sql(sqltxt)

    ca_rdf.write.mode('overwrite').saveAsTable(cst_ca_all_tab)
    
    del ca_rdf

    ca_rdf = spark.table(cst_ca_all_tab)
## end if CA Flag == 1


# COMMAND ----------

# MAGIC %md ## Prep store info for customers group

# COMMAND ----------

# dgsis_str  = trg_str_df.where ((trg_str_df.is_flag == 'y') & (trg_str_df.dgs_flag == 'y'))\
#                        .select(trg_str_df.store_id)\
#                        .dropDuplicates()

# trg_dgsis  = trg_str_df.join   (dgsis_str, 'store_id', 'left_semi')
#                        .groupBy(trg_str_df.store_id)\
#                        .agg    (sum(trg_str_df.media_fee_psto).alias('media_fee'))
# trg_dgsis.display()                       
          

# COMMAND ----------

## Get Target store code only

# trg_is     = trg_str_df.select( trg_str_df.store_id)\
#                        .where ((trg_str_df.is_flag.isNotNull()) & (trg_str_df.dgs_flag.isNull()))\
#                        .dropDuplicates()

all_trg_df = trg_str_df.select('store_id').dropDuplicates()

trg_is_pv  = trg_str_df.where ((trg_str_df.is_flag.isNotNull()) & (trg_str_df.dgs_flag.isNull()))\
                       .groupBy(trg_str_df.store_id)\
                       .pivot  ('aisle_scope')\
                       .agg    (sum('mech_count').alias('mech_cnt')
                               ,sum('media_fee_psto').alias('media_fee')
                               )

trg_is     = trg_is_pv.select('store_id')

#trg_is_pv.limit(10).display()

## Check if dataset empty
if len(trg_is.head(1)) > 0 :
    flag_ism = 1
    print('Having store with "instore media" only (is)')
else :
    flag_ism = 0
    print('No store having "instore media" only (is)')
## end if

trg_dgs    = trg_str_df.where ((trg_str_df.is_flag.isNull()) & 
                               (trg_str_df.dgs_flag.isNotNull()) & 
                               (trg_str_df.dgs_min_p_hr > 0))\
                       .groupBy(trg_str_df.store_id)\
                       .agg    (sum(trg_str_df.media_fee_psto).alias('media_fee'))

## Check if dataset empty
if len(trg_dgs.head(1)) > 0 :
    flag_dgs = 1
    print('Having store with "digital screen" only (dgs)')
else :
    flag_dgs = 0
    print('No store having "digital screen" only (dgs)')
## end if

dgsis_str  = trg_str_df.where ((trg_str_df.is_flag == 'y') & (trg_str_df.dgs_flag == 'y'))\
                       .select(trg_str_df.store_id)\
                       .dropDuplicates()

trg_dgsis  = trg_str_df.join   (dgsis_str, 'store_id', 'left_semi')\
                       .groupBy(trg_str_df.store_id)\
                       .agg    (sum(trg_str_df.media_fee_psto).alias('media_fee'))
                       
#trg_dgsis.display()

## Check if dataset empty
if len(trg_dgsis.head(1)) > 0 :
    flag_dgsis = 1
    print('Having store with both digital screen and instore media (dgs_is)')
else :
    flag_dgsis = 0
    print('No store having both digital screen and instore media (dgs_is)')
## end if


# COMMAND ----------

# MAGIC %md ## Get customer group

# COMMAND ----------

# MAGIC %md ### Instore Customers (Offline)

# COMMAND ----------

## instore media need check aisle scope for each store
if flag_ism == 1 :
  all_pcst_cost_is = 0
  ## separate store using trg_is_pv
  
  col_list   = trg_is_pv.columns
  cst_is_all = spark.createDataFrame(data = [], schema= StructType([]))

  if ('homeshelf_mech_cnt' in col_list) & ('store_mech_cnt' in col_list) :

      hm_lvstr  = trg_is_pv.where((trg_is_pv.homeshelf_mech_cnt > 0) & (trg_is_pv.store_mech_cnt.isNull()))\
                           .select(trg_is_pv.store_id)
      
      str_lvstr = trg_is_pv.where(trg_is_pv.store_mech_cnt.isNotNull() )\
                           .select(trg_is_pv.store_id)
  # store_mech_cnt doesn't exists 
  elif ('homeshelf_mech_cnt' in col_list) : 
      hm_lvstr  = trg_is_pv.where((trg_is_pv.homeshelf_mech_cnt > 0))\
                           .select(trg_is_pv.store_id)
      str_lvstr = spark.createDataFrame(data = [], schema= StructType([]))                   
  # homeshelf_mech_cnt doesn't exists
  elif ('store_mech_cnt' in col_list): 
      str_lvstr = trg_is_pv.where(trg_is_pv.store_mech_cnt.isNotNull() )\
                           .select(trg_is_pv.store_id)
      hm_lvstr  = spark.createDataFrame(data = [], schema= StructType([]))

  ## start for instore-homeshelf

  if len(hm_lvstr.head(1)) > 0:
    txn_is_hm   = ctxn.where( (ctxn.offline_online_other_channel == 'OFFLINE') &
                              (ctxn.date_id.between(cmp_start, cmp_end))
                            )\
                      .join(hm_lvstr, ctxn.store_id == hm_lvstr.store_id, 'left_semi')\
                      .join(use_ai_df, ctxn.upc_id == use_ai_df.upc_id, 'left_semi')  ## need join aisle prod to scope exposure
    
    #txn_is_hm.limit(20).display()
    ## get visit pen
    hm_agg        = txn_is_hm.groupBy(txn_is_hm.hh_flag)\
                             .agg( countDistinct(txn_is_hm.transaction_uid).alias('visit_cnt')
                                  ,countDistinct(txn_is_hm.household_id).alias('cc_cnt')                            
                                 )
    ## get media fee
    if ('homeshelf_mech_cnt' in col_list) & ('store_mech_cnt' in col_list) :
        is_hm_fee     = trg_is_pv.where((trg_is_pv.homeshelf_mech_cnt > 0) & (trg_is_pv.store_mech_cnt.isNull()))\
                                 .agg  (sum(trg_is_pv.homeshelf_media_fee).alias('media_fee'))\
                                 .collect()[0].media_fee
    elif ('homeshelf_mech_cnt' in col_list) : 
        is_hm_fee     = trg_is_pv.where((trg_is_pv.homeshelf_mech_cnt > 0) )\
                                 .agg  (sum(trg_is_pv.homeshelf_media_fee).alias('media_fee'))\
                                 .collect()[0].media_fee
    else:
        is_hm_fee = 0
    ## end if

    all_vis_hm    = hm_agg.agg(sum(hm_agg.visit_cnt).alias('all_vis')).collect()[0].all_vis
    mylo_vis_hm   = hm_agg.where(hm_agg.hh_flag == 1).select(hm_agg.visit_cnt).collect()[0].visit_cnt
    mylo_cnt_hm   = hm_agg.where(hm_agg.hh_flag == 1).select(hm_agg.cc_cnt).collect()[0].cc_cnt

    mylo_vpen_hm  = mylo_vis_hm/all_vis_hm
    aprox_acc_hm  = mylo_cnt_hm / mylo_vpen_hm
    aprox_ncc_hm  = aprox_acc_hm - mylo_cnt_hm
    pcst_cost_hm  = is_hm_fee/aprox_acc_hm

    print('Customer and Visits at instore - homeshelf media')
    print('-' * 80)
    print('all_vis_hm = ' + str(all_vis_hm))
    print('mylo_vis   = ' + str(mylo_vis_hm))
    print('mylo_vpen  = ' + str(mylo_vpen_hm))
    print('Mylo Cust  = ' + str(mylo_cnt_hm))
    print('approx all cust = ' + str(aprox_acc_hm))
    print('approx non Mylo = ' + str(aprox_ncc_hm))
    print('Total Instore homeshelf media fee = ' + str(is_hm_fee))
    print('per cust cost for homeshelf media = ' + str(pcst_cost_hm))
    print('-' * 80)    
    ## get min visits aisle for each customers & save as 1 table
    cst_is_hm    = txn_is_hm.where  (txn_is_hm.hh_flag == 1)\
                            .groupBy(txn_is_hm.household_id)\
                            .agg    ( min(txn_is_hm.date_id).alias('first_vis')
                                    , max(txn_is_hm.date_id).alias('last_vis')
                                    ,lit('is').alias('cust_group')
                                    )
    cst_is_all       = cst_is_hm
    all_pcst_cost_is = pcst_cost_hm

    # ## save customer visits homeshelf in target store
    # print('='*80)
    # print('Save Instore media customer to table : ' + str(cst_ishm_tab))
    # print('='*80)
    # sqltxt = 'drop table if exists ' + cst_ishm_tab
    
    # sqlContext.sql(sqltxt)

    # cst_is_hm.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_ishm_tab)
    
    # ## delete df and re-read from table
    # del cst_is_hm
    # cst_is_hm = spark.table(cst_ishm_tab)
  
  ## end if  ==> for is_homeshelf
  
  ## Start for instore - store level (promo - etc)

  if len(str_lvstr.head(1)) > 0:
    txn_is_str   = ctxn.where( (ctxn.offline_online_other_channel == 'OFFLINE') &
                               (ctxn.date_id.between(cmp_start, cmp_end))
                             )\
                       .join(str_lvstr, ctxn.store_id == str_lvstr.store_id, 'left_semi')
    
    #txn_is_hm.limit(20).display()
    ## get visit pen
    str_agg      = txn_is_str.groupBy(txn_is_str.hh_flag)\
                             .agg( countDistinct(txn_is_str.transaction_uid).alias('visit_cnt')
                                  ,countDistinct(txn_is_str.household_id).alias('cc_cnt')                            
                                 )                             
    ## get media fee                        
    if ('homeshelf_media_fee' in col_list) & ('store_media_fee' in col_list) :
        is_str_fee   = trg_is_pv.where(trg_is_pv.store_mech_cnt.isNotNull() )\
                                 .agg  (sum((trg_is_pv.homeshelf_media_fee + trg_is_pv.store_media_fee)).alias('media_fee'))\
                                 .collect()[0].media_fee
    elif ('homeshelf_media_fee' in col_list) :
        is_str_fee   = trg_is_pv.where(trg_is_pv.store_mech_cnt.isNotNull() )\
                                 .agg  (sum((trg_is_pv.homeshelf_media_fee)).alias('media_fee'))\
                                 .collect()[0].media_fee
    elif ('store_media_fee' in col_list) :
        is_str_fee   = trg_is_pv.where(trg_is_pv.store_mech_cnt.isNotNull() )\
                                 .agg  (sum((trg_is_pv.store_media_fee)).alias('media_fee'))\
                                 .collect()[0].media_fee
    ## end if
                                
    all_vis_str  = str_agg.agg  (sum(str_agg.visit_cnt).alias('all_vis')).collect()[0].all_vis
    mylo_vis_str = str_agg.where(str_agg.hh_flag == 1).select(str_agg.visit_cnt).collect()[0].visit_cnt
    mylo_cnt_str = str_agg.where(str_agg.hh_flag == 1).select(str_agg.cc_cnt).collect()[0].cc_cnt

    mylo_vpen_str = mylo_vis_str/all_vis_str
    aprox_acc_str = mylo_cnt_str / mylo_vpen_str
    aprox_ncc_str = aprox_acc_str - mylo_cnt_str
    pcst_cost_str = is_str_fee/aprox_acc_str
    
    print('Customer and Visits at instore - store level media')
    print('-' * 80)
    print('all_vis_str = ' + str(all_vis_str))
    print('mylo_vis   = ' + str(mylo_vis_str))
    print('mylo_vpen  = ' + str(mylo_vpen_str))
    print('Mylo Cust  = ' + str(mylo_cnt_str))
    print('approx all cust = ' + str(aprox_acc_str))
    print('approx non Mylo = ' + str(aprox_ncc_str))
    print('Total Instore storelevel media fee = ' + str(is_str_fee))
    print('per cust cost for store media      = ' + str(pcst_cost_str))
    print('-' * 80)
    
    all_pcst_cost_is = all_pcst_cost_is + pcst_cost_str

    ## get min visits aisle for each customers & save as 1 table
    cst_is_str    = txn_is_str.where  (txn_is_str.hh_flag == 1)\
                            .groupBy(txn_is_str.household_id)\
                            .agg    ( min(txn_is_str.date_id).alias('first_vis')
                                    , max(txn_is_str.date_id).alias('last_vis')
                                    ,lit('is').alias('cust_group')
                                    )
    
    if len(hm_lvstr.head(1)) > 0:
        cst_is_all = cst_is_all.unionByName(cst_is_str, allowMissingColumns = True).dropDuplicates()
    else:
        cst_is_all = cst_is_str
    ## end if

    ## save customer visits homeshelf in target store
    print('='*80)
    print('Save Instore media store level customer to table : ' + str(cst_isall_tab))
    print('='*80)

    # sqltxt = 'drop table if exists ' + cst_isall_tab
    
    # sqlContext.sql(sqltxt)

    cst_is_all.write.mode("overwrite").option('overwriteSchema', 'True').saveAsTable(cst_isall_tab)

    ## delete df and re-read from table
    del cst_is_all
    cst_is_all = spark.table(cst_isall_tab)
    
    # del cst_is_str
    # del cst_is_hm

  ## end if  ==> for is_store level.
  print('per cust cost for all in-store media      = ' + str(all_pcst_cost_is))
  ##   
## end if 


# COMMAND ----------

#cst_is_all.limit(10).display()

# COMMAND ----------

# MAGIC %md ### dgs only customers

# COMMAND ----------

## use store level customers for Digital screen
if flag_dgs == 1:
    txn_dgs       = ctxn.where( (ctxn.offline_online_other_channel == 'OFFLINE') &
                                (ctxn.date_id.between(cmp_start, cmp_end))
                              )\
                        .join(trg_dgs  , ctxn.store_id == trg_dgs.store_id, 'left_semi')
    
    ## get media fee overall dgs

    dgs_mfee      = trg_dgs.agg(sum('media_fee').alias('media_fee')).collect()[0].media_fee

    ## get visit pen
    dgs_agg       = txn_dgs.groupBy(txn_dgs.hh_flag)\
                           .agg(countDistinct(txn_dgs.transaction_uid).alias('visit_cnt')
                               ,countDistinct(txn_dgs.household_id).alias('cc_cnt')                            
                               )
    all_vis_dgs   = dgs_agg.agg(sum(dgs_agg.visit_cnt).alias('all_vis')).collect()[0].all_vis
    mylo_vis_dgs  = dgs_agg.where(dgs_agg.hh_flag == 1).select(dgs_agg.visit_cnt).collect()[0].visit_cnt
    mylo_cnt_dgs  = dgs_agg.where(dgs_agg.hh_flag == 1).select(dgs_agg.cc_cnt).collect()[0].cc_cnt

    mylo_vpen_dgs = mylo_vis_dgs/all_vis_dgs
    aprox_acc_dgs = mylo_cnt_dgs / mylo_vpen_dgs
    aprox_ncc_dgs = aprox_acc_dgs - mylo_cnt_dgs
    pcst_cost_dgs = dgs_mfee/aprox_acc_dgs
    
    print('Customer and Visits at instore Level - DGS alone')
    print('-' * 80)
    print('all_vis_dgs = ' + str(all_vis_dgs))
    print('mylo_vis    = ' + str(mylo_vis_dgs))
    print('mylo_vpen   = ' + str(mylo_vpen_dgs))
    print('Mylo Cust   = ' + str(mylo_cnt_dgs))
    print('approx all cust = ' + str(aprox_acc_dgs))
    print('approx non Mylo = ' + str(aprox_ncc_dgs))
    print('Total Digital screen media fee         = ' + str(dgs_mfee))
    print('per cust cost for Digital Screen media = ' + str(pcst_cost_dgs))
    print('-' * 80)    
    ## get min visits aisle for each customers & save as 1 table
    cst_dgs   = txn_dgs.where  (txn_dgs.hh_flag == 1)\
                       .groupBy(txn_dgs.household_id)\
                       .agg    ( min(txn_dgs.date_id).alias('first_vis')
                               ,max(txn_dgs.date_id).alias('last_vis')
                               ,lit('dgs').alias('cust_group')
                               )
                             
    ## save customer visits homeshelf in target store
    print('='*80)
    print('Save Digital Screen media customer to table : ' + str(cst_dgs_tab))
    print('='*80)
    # sqltxt = 'drop table if exists ' + cst_dgs_tab
    
    # sqlContext.sql(sqltxt)

    cst_dgs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_dgs_tab)
    
    ## delete df and re-read from table
    del cst_dgs
    cst_dgs = spark.table(cst_dgs_tab)

## end if  ==> for digital screen only

# COMMAND ----------

# MAGIC %md ### dgs+is customers (1) from store with 2 media mechanics

# COMMAND ----------

## use store level customers for Digital screen
#flag_dgsis = 1
print(flag_dgsis)
if flag_dgsis == 1:
    txn_dgsis       = ctxn.where( (ctxn.offline_online_other_channel == 'OFFLINE') &
                                  (ctxn.date_id.between(cmp_start, cmp_end))
                                )\
                          .join (trg_dgsis  , ctxn.store_id == trg_dgsis.store_id, 'left_semi')  
    
    ## get media fee overall dgs
    dgsis_mfee      = trg_dgsis.agg(sum('media_fee').alias('media_fee')).collect()[0].media_fee
    
    #trg_dgsis.limit(5).display()
    
    print('dgsis_mfee = ' + str(dgsis_mfee))
    
    ## get visit pen
    dgsis_agg       = txn_dgsis.groupBy(txn_dgsis.hh_flag)\
                               .agg(countDistinct(txn_dgsis.transaction_uid).alias('visit_cnt')
                                   ,countDistinct(txn_dgsis.household_id).alias('cc_cnt')                            
                                   )
    all_vis_dgsis   = dgsis_agg.agg(sum(dgsis_agg.visit_cnt).alias('all_vis')).collect()[0].all_vis
    mylo_vis_dgsis  = dgsis_agg.where(dgsis_agg.hh_flag == 1).select(dgsis_agg.visit_cnt).collect()[0].visit_cnt
    mylo_cnt_dgsis  = dgsis_agg.where(dgsis_agg.hh_flag == 1).select(dgsis_agg.cc_cnt).collect()[0].cc_cnt

    mylo_vpen_dgsis = mylo_vis_dgsis/all_vis_dgsis
    aprox_acc_dgsis = mylo_cnt_dgsis/ mylo_vpen_dgsis
    aprox_ncc_dgsis = aprox_acc_dgsis - mylo_cnt_dgsis
    pcst_cost_dgsis = dgsis_mfee/aprox_acc_dgsis
    
    print('Customer and Visits at instore Level - DGS + Instore media')
    print('-' * 80)
    print('all_vis_dgsis   = ' + str(all_vis_dgsis))
    print('mylo_vis        = ' + str(mylo_vis_dgsis))
    print('mylo_vpen       = ' + str(mylo_vpen_dgsis))
    print('Mylo Cust       = ' + str(mylo_cnt_dgsis))
    print('approx all cust = ' + str(aprox_acc_dgsis))
    print('approx non Mylo = ' + str(aprox_ncc_dgsis))
    print('Total Digital screen + instore media fee         = ' + str(dgsis_mfee))
    print('per cust cost for Digital Screen + Instore media = ' + str(pcst_cost_dgsis))
    print('-' * 80)    
    ## get min visits aisle for each customers & save as 1 table
    cst_is_dgs   = txn_dgsis.where  (txn_dgsis.hh_flag == 1)\
                           .groupBy (txn_dgsis.household_id)\
                           .agg     ( min(txn_dgsis.date_id).alias('first_vis')
                                     ,max(txn_dgsis.date_id).alias('last_vis')
                                     ,lit('is_dgs').alias('cust_group')
                                    )
                             
    ## save customer visits homeshelf in target store
    print('='*80)
    print('Save Digital Screen & Instore media customer to table : ' + str(cst_isdgs_tab))
    print('='*80)
    sqltxt = 'drop table if exists ' + cst_isdgs_tab
    
    sqlContext.sql(sqltxt)

    cst_is_dgs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_isdgs_tab)
    
    ## delete df and re-read from table
    del cst_is_dgs
    cst_is_dgs = spark.table(cst_isdgs_tab)
    #cst_is_dgs.limit(5).display()
## end if  ==> for is_digital screen

# COMMAND ----------

# MAGIC %md ### dgs+is customers (2) from customers visits 2 type of store

# COMMAND ----------

# cst_is_dgs.limit(10).display()
# print('flag_ism   = ' + str(flag_ism))
# print('flag_dgs   = ' + str(flag_dgs))
# print('flag_dgsis = ' + str(flag_dgsis))

# COMMAND ----------

## get customer visit both dgs store and is store
if (flag_ism == 1) & (flag_dgs == 1) :
  
  #flag_dgsis = 1
  ## inner join check customers in both
  cst_dgs_j  = cst_dgs.select( cst_dgs.household_id.alias('hh_id')
                              ,cst_dgs.first_vis.alias('f_date')
                              ,cst_dgs.last_vis.alias('l_date')
                              ,cst_dgs.cust_group
                             )
  
  cst_isdgs2 = cst_is_all.join   (cst_dgs_j, cst_is_all.household_id == cst_dgs_j.hh_id , 'inner')\
                         .select ( cst_is_all.household_id
                                  ,F.when     ((cst_is_all.first_vis <= cst_dgs_j.f_date), cst_is_all.first_vis)
                                    .otherwise(cst_dgs_j.f_date)
                                    .alias    ("first_vis")
                                  ,F.when     ((cst_is_all.last_vis >= cst_dgs_j.l_date), cst_is_all.last_vis)
                                    .otherwise(cst_dgs_j.l_date)
                                    .alias    ("last_vis")
                                  ,lit('is_dgs').alias('cust_group')
                                 )
  
  ## remove customers who are is_dgs from both is_all and dgs
  cst_is_all = cst_is_all.join(cst_isdgs2, cst_is_all.household_id == cst_isdgs2.household_id, 'left_anti')
  cst_dgs    = cst_dgs.join   (cst_isdgs2, cst_dgs.household_id == cst_isdgs2.household_id, 'left_anti')

  ## merge cst_isdgs2 to cst_is_dgs
  print('-' *80)
  print('Show example table cst_isdgs2 :')
  cst_isdgs2.limit(5).display()
  print('-' *80)
  chk_cst = len(cst_isdgs2.head(1))
  print('len(cst_isdgs2.head(1)) = ' + str(chk_cst))
  if chk_cst > 0:
    flag_dgsis2 = 1
  else:
    flag_dgsis2 = 0
  ## end if
  
  print('flag_dgsis2             = ' + str(flag_dgsis2))

  if ((flag_dgsis == 1) & (flag_dgsis2 == 1) & (len(cst_isdgs2.head(1)) > 0)):
      #print(flag_dgsis)
      cst_is_dgs_nw = cst_is_dgs.unionByName(cst_isdgs2, allowMissingColumns = True).dropDuplicates()
      
      print('='*80)
      print('Rewrite Digital Screen media + instore media customers to table : ' + str(cst_isdgs_tab))
      print('='*80)
      # sqltxt = 'drop table if exists ' + cst_isdgs_tab    
      # sqlContext.sql(sqltxt) 

      cst_is_dgs_nw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_isdgs_tab)
      
      del cst_is_dgs_nw
      cst_is_dgs = spark.table(cst_isdgs_tab)

  elif ((flag_dgsis2 == 1) & (len(cst_isdgs2.head(1)) > 0)) :
      cst_is_dgs_nw = cst_isdgs2
      pcst_cost_dgsis = all_pcst_cost_is + pcst_cost_dgs
      
      print('='*80)
      print('Rewrite Digital Screen media + instore media customers to table : ' + str(cst_isdgs_tab))
      print('='*80)
      # sqltxt = 'drop table if exists ' + cst_isdgs_tab    
      # sqlContext.sql(sqltxt)  
      cst_is_dgs_nw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_isdgs_tab)
      
      del cst_is_dgs_nw
      cst_is_dgs = spark.table(cst_isdgs_tab)
  else:
      print('='*80)
      print('No customers who cross shop "IS" and "DGS" : ')
      print('='*80)
  ## end if
  

  # pcst_cost_dgsis = all_pcst_cost_is + pcst_cost_dgs

  # print('='*80)
  # print('Rewrite Digital Screen media + instore media customers to table : ' + str(cst_isdgs_tab))
  # print('='*80)
  # sqltxt = 'drop table if exists ' + cst_isdgs_tab    
  # sqlContext.sql(sqltxt)  
  # cst_is_dgs_nw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_isdgs_tab)
    
  # ## delete df and re-read from table
  # del cst_is_dgs_nw
  # cst_is_dgs = spark.table(cst_isdgs_tab)
## end if

## Exclude is_dgs customer from is, dgs only
if (flag_ism == 1) & (flag_dgsis == 1)  :
  cst_is_all = cst_is_all.join(cst_is_dgs, cst_is_all.household_id == cst_is_dgs.household_id, 'left_anti')
## end if
if (flag_dgs == 1) & (flag_dgsis == 1)  :
  cst_dgs    = cst_dgs.join   (cst_is_dgs, cst_dgs.household_id == cst_is_dgs.household_id, 'left_anti')
## end if


# COMMAND ----------

# MAGIC %md ### Get Main Control customers group

# COMMAND ----------

txn_nis_str   =  ctxn.where( (ctxn.date_id.between(cmp_start, cmp_end)) &
                             (ctxn.hh_flag == 1)
                           )\
                     .join(all_trg_df, ctxn.store_id == all_trg_df.store_id, 'left_anti')

txn_nis_hm    =  txn_nis_str.where((txn_nis_str.store_format_group == 'HDE') &
                                   (txn_nis_str.offline_online_other_channel == 'OFFLINE')
                                  )\
                            .join(use_ai_df, ctxn.upc_id == use_ai_df.upc_id, 'left_semi')  

## get customers nis

cst_nis       = txn_nis_hm.select(txn_nis_hm.household_id).dropDuplicates()

## save cst_nis to table
cst_nis.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_nis_tab)

del cst_nis
cst_nis       = spark.table(cst_nis_tab)

cst_ntrg_hde  = txn_nis_str.where((txn_nis_str.store_format_group == 'HDE') &
                                  (txn_nis_str.offline_online_other_channel == 'OFFLINE')
                                  )\
                           .join(cst_nis, txn_nis_str.household_id == cst_nis.household_id, 'left_anti')\
                           .select(txn_nis_str.household_id)\
                           .dropDuplicates()

cst_ntrg_hde.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_ntrg_hde_tab)
del cst_ntrg_hde

cst_ntrg_hde  = spark.table(cst_ntrg_hde_tab)

cst_ntrg_oth  = txn_nis_str.join(cst_nis, txn_nis_str.household_id == cst_nis.household_id, 'left_anti')\
                           .join(cst_ntrg_hde, txn_nis_str.household_id == cst_ntrg_hde.household_id, 'left_anti')\
                           .select(txn_nis_str.household_id)\
                           .dropDuplicates()

cst_ntrg_oth.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_ntrg_oth_tab)
del cst_ntrg_oth

cst_ntrg_oth  = spark.table(cst_ntrg_oth_tab)


# COMMAND ----------

# MAGIC %md ### Get CA to merge with 3 group of target & control customers

# COMMAND ----------

#ca_rdf.limit(10).display()

# COMMAND ----------

##-------------------------------------------------------------------------------------
## Start Merge target OOH + OnShelf with target CA
##-------------------------------------------------------------------------------------
if flag_ca == 1:

    ca_trg  = ca_rdf.where(ca_rdf.c_type == 'trg')\
                    .select(ca_rdf.hh_id)\
                    .dropDuplicates()
    
    ca_ctl  = ca_rdf.where(ca_rdf.c_type == 'ctl')\
                    .select(ca_rdf.hh_id)\
                    .dropDuplicates()
    
    ## Join ca_target with each group of target customers
    ## get cust IS + CA
    
    flag_isca = 0
    
    if(flag_ism == 1) :
    
      cst_is_ca       = cst_is_all.join  (ca_trg, cst_is_all.household_id == ca_trg.hh_id, 'left_semi')\
                                  .select( cst_is_all.household_id
                                          ,cst_is_all.first_vis
                                          ,cst_is_all.last_vis
                                          ,lit('is_ca').alias('cust_group')
                                          )
                                          
      cst_is_only     = cst_is_all.join  (ca_trg, cst_is_all.household_id == ca_trg.hh_id, 'left_anti')\
      
      if (len(cst_is_ca.head(1)) > 0) :
      
          cst_trg_all     = cst_is_ca.unionByName(cst_is_only, allowMissingColumns = True)
          flag_isca = 1
      else:
          print('No customers get both "Instore Media & Online media" ')      
          flag_isca = 0
          cst_trg_all     = cst_is_all
      ## end if  
    ## endif
    
    ## get cust dgs + CA
    flag_dgs_ca = 0
    
    if (flag_dgs == 1) :
    
      cst_dgs_ca      = cst_dgs.join  (ca_trg, cst_dgs.household_id == ca_trg.hh_id, 'left_semi')\
                               .select( cst_dgs.household_id
                                       ,cst_dgs.first_vis
                                       ,cst_dgs.last_vis
                                       ,lit('dgs_ca').alias('cust_group')
                                      )
      cst_dgs_only    = cst_dgs.join  (ca_trg, cst_dgs.household_id == ca_trg.hh_id, 'left_anti')
      
      if(len(cst_dgs_ca.head(1)) > 0):  
          flag_dgs_ca = 1      
      else:
          print('No customers get both "OOH Media & Online media" ')      
          flag_dgs_ca = 0
      ## end if  
      
      if ((flag_dgs_ca == 1) & (flag_ism == 1)) :
        cst_trg_all = cst_trg_all.unionByName(cst_dgs_ca, allowMissingColumns = True)\
                                 .unionByName(cst_dgs_only, allowMissingColumns = True)
      elif ((flag_dgs_ca == 0) & (flag_ism == 1)):
        cst_trg_all = cst_trg_all.unionByName(cst_dgs, allowMissingColumns = True)
      else:
        cst_trg_all = cst_dgs
      ## endif
      
    ## end if
    
    ## Get cust 3 media
    flag_dgs_is_ca = 0
    
    if ((flag_dgsis == 1) & (flag_ca == 1)) :
    
      cst_dgsis_ca    = cst_is_dgs.join  (ca_trg, cst_is_dgs.household_id == ca_trg.hh_id, 'left_semi')\
                                  .select( cst_is_dgs.household_id
                                          ,cst_is_dgs.first_vis
                                          ,cst_is_dgs.last_vis
                                          ,lit('dgs_is_ca').alias('cust_group')
                                         )
                                 
      cst_dgsis_only  = cst_is_dgs.join  (ca_trg, cst_is_dgs.household_id == ca_trg.hh_id, 'left_anti')       
      
      if(len(cst_dgsis_ca.head(1)) > 0) :  
          flag_dgs_is_ca = 1      
      else:
          print('No customers get all "Instore & OOH Media & Online media" ')      
          flag_dgs_is_ca = 0
      ## end if  
       
      if ((cst_dgsis_ca == 1) & ((flag_isca == 1) | (flag_dgs_ca == 1))) :
        cst_trg_all = cst_trg_all.unionByName(cst_dgsis_ca, allowMissingColumns = True)\
                                 .unionByName(cst_dgsis_only, allowMissingColumns = True)
      elif ((cst_dgsis_ca == 0) & ((flag_isca == 1) | (flag_dgs_ca == 1))) :
        cst_trg_all = cst_trg_all.unionByName(cst_is_dgs, allowMissingColumns = True)
      else: 
        cst_trg_all = cst_dgsis_ca
      ## end if
    ## end if
    
    ## Get CA only
    cst_ca          = ca_trg.join  (cst_trg_all, ca_trg.hh_id == cst_trg_all.household_id, 'left_anti')\
                            .select( ca_trg.hh_id.alias('household_id')
                                    ,lit(cmp_start).alias('first_vis')
                                    ,lit(cmp_end).alias('last_vis')
                                    ,lit('ca').alias('cust_group')
                                   )
    
    cst_trg_all     = cst_trg_all.unionByName(cst_ca, allowMissingColumns = True)
    
    ## end if  
    ## Save All target group to table
    cst_trg_all.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_trg_tab)
    
    del cst_trg_all
    
    cst_trg_all = spark.table(cst_trg_tab)

else:
    print('-' * 80)
    print('No Online CA campaign for this evaluation will merge O2 data to table')
    print('-' * 80)
    
    cst_trg_flag = 0
    
    ## Add Instore customers
    if flag_ism == 1 :
        cst_trg_all = cst_is_all
        cst_trg_flag = 1
    ## end if
    
    ## Add OOH customers
    if   (flag_dgs == 1) & (cst_trg_flag == 1) :
        cst_trg_all = cst_trg_all.unionByName(cst_dgs, allowMissingColumns = True)        
    elif (flag_dgs == 1) & (cst_trg_flag == 0) :
        cst_trg_all = cst_dgs
        cst_trg_flag = 1
    ## end if
    
    ## add DGS IS customers
    if   (flag_dgsis == 1) & (cst_trg_flag == 1) :
        cst_trg_all = cst_trg_all.unionByName(cst_is_dgs, allowMissingColumns = True)        
    elif (flag_dgsis == 1) & (cst_trg_flag == 0) :
        cst_trg_all = cst_is_dgs
        cst_trg_flag = 1
    ## end if
   
   ## Save All target group to table
    cst_trg_all.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_trg_tab)
    
    del cst_trg_all
    
    cst_trg_all = spark.table(cst_trg_tab)
    
 ## end if flag_ca

# COMMAND ----------

# MAGIC %md ### Get final control customers group

# COMMAND ----------

## join control ca to control group
# cst_nis
# cst_ntrg_hde
# cst_ntrg_oth
# ca_ctl
##----------------
if flag_ca == 1:

    cst_nis_cca   = cst_nis.join(ca_ctl, cst_nis.household_id == ca_ctl.hh_id, 'left_semi')\
                           .select( cst_nis.household_id
                                   ,lit('nis_ndgs_cca').alias('cust_group')
                                  )
    
    cst_nis_only  = cst_nis.join(ca_ctl, cst_nis.household_id == ca_ctl.hh_id, 'left_anti')\
                           .select( cst_nis.household_id
                                   ,lit('nis_ndgs').alias('cust_group')
                                  )
    
    cst_ctl_all   = cst_nis_cca.unionByName(cst_nis_only, allowMissingColumns = True)
else:
    cst_ctl_all   = cst_nis.select( cst_nis.household_id
                                   ,lit('nis_ndgs').alias('cust_group')
                                  )
##----------------
if flag_ca == 1:
    cst_ntrg_hde_cca  = cst_ntrg_hde.join(ca_ctl, cst_ntrg_hde.household_id == ca_ctl.hh_id, 'left_semi')\
                                    .select( cst_ntrg_hde.household_id
                                            ,lit('ndgs_cca').alias('cust_group')
                                           )
    
    cst_ntrg_hde_only = cst_ntrg_hde.join  (ca_ctl, cst_ntrg_hde.household_id == ca_ctl.hh_id, 'left_anti')\
                                    .select( cst_ntrg_hde.household_id
                                            ,lit('ndgs').alias('cust_group')
                                           )
    
    cst_ctl_all       = cst_ctl_all.unionByName(cst_ntrg_hde_cca, allowMissingColumns = True)\
                                   .unionByName(cst_ntrg_hde_only, allowMissingColumns = True)
else:
    cst_ntrg_hde_only = cst_ntrg_hde.select( cst_ntrg_hde.household_id
                                            ,lit('ndgs').alias('cust_group')
                                           )
    cst_ctl_all       = cst_ctl_all.unionByName(cst_ntrg_hde_only, allowMissingColumns = True)    
##-------------------
if flag_ca == 1:
    cst_ntrg_oth_cca  = cst_ntrg_oth.join(ca_ctl, cst_ntrg_oth.household_id == ca_ctl.hh_id, 'left_semi')\
                                    .select( cst_ntrg_oth.household_id
                                            ,lit('cca').alias('cust_group')
                                           )
    ##cst_ntrg_oth_only >> not use
    
    cst_ctl_all       = cst_ctl_all.unionByName(cst_ntrg_oth_cca, allowMissingColumns = True)
    
    cst_cca_pure      = ca_ctl.join  (cst_ctl_all, cst_ctl_all.household_id == ca_ctl.hh_id, 'left_anti')\
                              .select( ca_ctl.hh_id.alias('household_id')
                                      ,lit('cca').alias('cust_group')
                                     )
    
    cst_ctl_all       = cst_ctl_all.unionByName(cst_cca_pure, allowMissingColumns = True)
else:
    ## do nothing 
    cst_ctl_all       = cst_ctl_all
##--------------------
    
## add step dedup with target group

cst_ctl_dup_trg   = cst_ctl_all.join(cst_trg_all, 'household_id', 'left_semi')

ctl_in_trg        = cst_ctl_dup_trg.count()
print('#Control customer become target customer = ' + str(ctl_in_trg) + ' custs \n')

cst_ctl_all_use   = cst_ctl_all.join(cst_trg_all, 'household_id', 'left_anti')

## Save All Control group to table
cst_ctl_all_use.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cst_ctl_tab)

del cst_ctl_all_use
del cst_ctl_all

cst_ctl_all = spark.table(cst_ctl_tab)

print('#' * 80)
print('Display Control group ')
print('#' * 80)

cst_ctl_grp = cst_ctl_all.groupBy('cust_group')\
                         .agg    (countDistinct(cst_ctl_all.household_id).alias('cc_cnt'))

                             
cst_ctl_pd  = cst_ctl_grp.toPandas()

cst_ctl_pd.display()

# cst_ctl_all.groupBy('cust_group')\
#            .agg    (countDistinct(cst_ctl_all.household_id).alias('cc_cnt'))\
#            .display()

print('#' * 80)
print('Display Target group ')
print('#' * 80)
cst_trg_grp = cst_trg_all.groupBy('cust_group')\
                         .agg    (countDistinct(cst_trg_all.household_id).alias('cc_cnt'))

cst_trg_pd  = cst_trg_grp.toPandas()
cst_trg_pd.display()
          
print('#' * 80)


# COMMAND ----------

# MAGIC %md ## Get KPI for each cust group

# COMMAND ----------

# MAGIC %md ### Function to get kpi of each group

# COMMAND ----------

#def get_cust_kpi(intxn, cst_base, cst_grp):
#   ##
#   ## 2 inputs parameters
#   ## intxn   : spark Dataframe: Focus transaction in Focus period
#   ## cst_base: double : base customers of customer group (activated + non activated)
#   ## cst_grp : string : string : name of customer group
#   ##
#   ## Return a spark Dataframe contain KPI of customers group in focus transaciton
  
#   atxn = intxn.agg( lit(cst_grp).alias('cust_group')
#                    ,lit(cst_base).alias('based_cust')
#                    ,countDistinct(intxn.household_id).alias('atv_cust')                   
#                    ,countDistinct(intxn.transaction_uid).alias('cc_visits')                   
#                    ,sum(intxn.net_spend_amt).alias('sum_cc_sales')
#                    ,sum(intxn.pkg_weight_unit).alias('sum_cc_units')                   
#                    ,avg(intxn.net_spend_amt).alias('avg_cc_sales')
#                    ,avg(intxn.pkg_weight_unit).alias('avg_cc_units')
#                   )\
              
  
#   odf  = atxn.withColumn('avg_vpc', (atxn.cc_visits/atxn.atv_cust))\
#              .withColumn('avg_spc', (atxn.sum_cc_sales/atxn.atv_cust))\
#              .withColumn('avg_spv', (atxn.sum_cc_sales/atxn.cc_visits))\
#              .withColumn('avg_upv', (atxn.sum_cc_units/atxn.cc_visits))\
#              .withColumn('avg_ppu', (atxn.sum_cc_sales/atxn.sum_cc_units))\
#              .withColumn('unatv_cust', (atxn.based_cust - atxn.atv_cust))\
#              .withColumn('atv_rate', (atxn.atv_cust/atxn.based_cust))
  
#   o_df = odf.select( odf.cust_group
#                     ,odf.based_cust
#                     ,odf.atv_cust
#                     ,odf.unatv_cust
#                     ,odf.atv_rate
#                     ,odf.cc_visits.alias('visits')
#                     ,odf.avg_spc
#                     ,odf.avg_vpc
#                     ,odf.avg_spv
#                     ,odf.avg_upv
#                     ,odf.avg_ppu
#                     ,odf.sum_cc_sales.alias('all_sales')
#                     ,odf.sum_cc_units.alias('all_units')
#                     ,odf.avg_cc_sales.alias('avg_sales')
#                     ,odf.avg_cc_units.alias('avg_units')
#                    )
  
#   return o_df

# ## end def 

def get_cust_kpi(catv_txn):

#   ## 1 inputs parameters 
#   ## intxn   : spark Dataframe: Focus transaction in Focus period of customers conversion group
# 
#   ## Return a spark Dataframe contain KPI of customers group in focus transaciton

    atxn = catv_txn.groupBy('cust_group')\
                       .agg( max(catv_txn.based_cust).alias('based_cust')
                            ,countDistinct(catv_txn.household_id).alias('atv_cust')                   
                            ,countDistinct(catv_txn.transaction_uid).alias('cc_visits')                   
                            ,sum(catv_txn.net_spend_amt).alias('sum_cc_sales')
                            ,sum(catv_txn.pkg_weight_unit).alias('sum_cc_units')                   
                            ,avg(catv_txn.net_spend_amt).alias('avg_cc_sales')
                            ,avg(catv_txn.pkg_weight_unit).alias('avg_cc_units')
                           )\
    
    odf  = atxn.withColumn('avg_vpc', (atxn.cc_visits/atxn.atv_cust))\
               .withColumn('avg_spc', (atxn.sum_cc_sales/atxn.atv_cust))\
               .withColumn('avg_spv', (atxn.sum_cc_sales/atxn.cc_visits))\
               .withColumn('avg_upv', (atxn.sum_cc_units/atxn.cc_visits))\
               .withColumn('avg_ppu', (atxn.sum_cc_sales/atxn.sum_cc_units))\
               .withColumn('unatv_cust', (atxn.based_cust - atxn.atv_cust))\
               .withColumn('atv_rate', (atxn.atv_cust/atxn.based_cust))
    
    ## brand activated KPI
    o_df = odf.select( odf.cust_group
                      ,odf.based_cust
                      ,odf.atv_cust
                      ,odf.unatv_cust
                      ,odf.atv_rate
                      ,odf.cc_visits.alias('visits')
                      ,odf.avg_spc
                      ,odf.avg_vpc
                      ,odf.avg_spv
                      ,odf.avg_upv
                      ,odf.avg_ppu
                      ,odf.sum_cc_sales.alias('all_sales')
                      ,odf.sum_cc_units.alias('all_units')
                      ,odf.avg_cc_sales.alias('avg_sales')
                      ,odf.avg_cc_units.alias('avg_units')
                     )
    
    return o_df
## end def    

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ###Funciton "get cust KPI" for online customer separated by CA group

# COMMAND ----------

def get_cust_kpi_ca(intxn, cst_df, cst_grp):
  ##
  ## 2 inputs parameters
  ## intxn   : spark Dataframe: Focus transaction in Focus period
  ## cst_base: double : base customers of customer group (activated + non activated)
  ## cst_grp : string : string : name of customer group
  ##
  ## Return a spark Dataframe contain KPI of customers group in focus transaciton
  
  # Get #customers for each CA group
  
  cst_cnt_df = cst_df.groupby( cst_df.ca_group)\
                     .agg    ( sum(lit(1)).alias('ca_cnt'))
  
  atxn       = intxn.join(cst_cnt_df, 'ca_group', 'inner')\
                    .groupBy(intxn.ca_group)\
                    .agg( lit(cst_grp).alias('cust_group')
                         ,max(cst_cnt_df.ca_cnt).alias('based_cust')
                         ,countDistinct(intxn.household_id).alias('atv_cust')                   
                         ,countDistinct(intxn.transaction_uid).alias('cc_visits')                   
                         ,sum(intxn.net_spend_amt).alias('sum_cc_sales')
                         ,sum(intxn.pkg_weight_unit).alias('sum_cc_units')                   
                         ,avg(intxn.net_spend_amt).alias('avg_cc_sales')
                         ,avg(intxn.pkg_weight_unit).alias('avg_cc_units')
                        )\
                    
        
  odf        = atxn.withColumn('avg_vpc', (atxn.cc_visits/atxn.atv_cust))\
                   .withColumn('avg_spc', (atxn.sum_cc_sales/atxn.atv_cust))\
                   .withColumn('avg_spv', (atxn.sum_cc_sales/atxn.cc_visits))\
                   .withColumn('avg_upv', (atxn.sum_cc_units/atxn.cc_visits))\
                   .withColumn('avg_ppu', (atxn.sum_cc_sales/atxn.sum_cc_units))\
                   .withColumn('unatv_cust', (atxn.based_cust - atxn.atv_cust))\
                   .withColumn('atv_rate', (atxn.atv_cust/atxn.based_cust))
        
  o_df       = odf.select( odf.cust_group
                          ,odf.ca_group
                          ,odf.based_cust
                          ,odf.atv_cust
                          ,odf.unatv_cust
                          ,odf.atv_rate
                          ,odf.cc_visits.alias('visits')
                          ,odf.avg_spc
                          ,odf.avg_vpc
                          ,odf.avg_spv
                          ,odf.avg_upv
                          ,odf.avg_ppu
                          ,odf.sum_cc_sales.alias('all_sales')
                          ,odf.sum_cc_units.alias('all_units')
                          ,odf.avg_cc_sales.alias('avg_sales')
                          ,odf.avg_cc_units.alias('avg_units')
                         )\
                  .orderBy(odf.cust_group
                          ,odf.ca_group)
  
  return o_df

## end def 


# COMMAND ----------

# MAGIC %md ### Get number of customer in each group for information

# COMMAND ----------

#cst_trg_pd.dtypes

# COMMAND ----------

## Get number of customer in each group for information

## Target group
grp_trg      = len(cst_trg_pd)
trg_nm_list  = cst_trg_pd['cust_group'].tolist()
trg_grp_val  = cst_trg_pd['cc_cnt'].tolist()
all_trg      = cst_trg_pd['cc_cnt'].sum()

print('-'*80)
print('Target customers separated into = ' + str(grp_trg) + ' groups')
print('Total customers                 = ' + str(all_trg) + ' custs')
print('-'*80)

print(trg_nm_list)
print(trg_grp_val)

## Control group

grp_ctl      = len(cst_ctl_pd)
ctl_nm_list  = cst_ctl_pd['cust_group'].tolist()
ctl_grp_val  = cst_ctl_pd['cc_cnt'].tolist()
all_ctl      = cst_ctl_pd['cc_cnt'].sum()

print('='*80)

print('Control customers separated into = ' + str(grp_ctl) + ' groups')
print('Total customers                  = ' + str(all_ctl) + ' custs')
print('-'*80)

print(ctl_nm_list)
print(ctl_grp_val)

# ## all target
# t1_ccnt = t1_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
# t2_ccnt = t2_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
# t3_ccnt = t3_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt

# all_trg = t1_ccnt + t2_ccnt + t3_ccnt
# ## all control
# c1_ccnt = c1_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
# c2_ccnt = c2_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
# c3_ccnt = c3_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt

# all_ctl = c1_ccnt + c2_ccnt + c3_ccnt

# ## print 

# print(' Number T1 customers (offline only) = ' + str(t1_ccnt ))
# print(' Number C1 customers (offline only) = ' + str(c1_ccnt ))
# print('-'*80)
# print(' Number T2 customers (online + offline cust) = ' + str(t2_ccnt ))
# print(' Number C2 customers (online + offline cust) = ' + str(c2_ccnt ))
# print('-'*80)
# print(' Number T3 customers (online only) = ' + str(t3_ccnt ))
# print(' Number C3 customers (online only) = ' + str(c3_ccnt ))
# print('-'*80)

# print(' All target customers  = ' + str(all_trg ))
# print(' All control customers = ' + str(all_ctl ))

# COMMAND ----------

# MAGIC %md ### Brand level conversion

# COMMAND ----------

## All group
## expose before buy as conversion only 
trg_batv_txn = bnd_txn_dur.join  (cst_trg_all, bnd_txn_dur.household_id == cst_trg_all.household_id, 'inner')\
                          .where (bnd_txn_dur.date_id >= cst_trg_all.first_vis)\
                          .select(bnd_txn_dur['*']
                                 ,cst_trg_all.cust_group
                                 )
                          
## control has no date (as no media to expose)                          
ctl_batv_txn = bnd_txn_dur.join (cst_ctl_all, bnd_txn_dur.household_id == cst_ctl_all.household_id, 'inner')\
                          .select(bnd_txn_dur['*']
                                 ,cst_ctl_all.cust_group
                                 )

##----------------------------------------------------
## brand level KPI
##----------------------------------------------------
## target 
trg_batv_txn    = trg_batv_txn.join  (cst_trg_grp, trg_batv_txn.cust_group == cst_trg_grp.cust_group, 'inner')\
                              .select(trg_batv_txn['*']
                                     ,cst_trg_grp.cc_cnt.alias('based_cust'))            

trg_grp_kpi_bnd = get_cust_kpi(trg_batv_txn)

print('=' * 80)
print('Brand KPI result by Target customer group')
print('=' * 80)
trg_grp_kpi_bnd.display()
## control
print('=' * 80)

## for control goup
ctl_batv_txn    = ctl_batv_txn.join  (cst_ctl_grp, ctl_batv_txn.cust_group == cst_ctl_grp.cust_group, 'inner')\
                              .select(ctl_batv_txn['*']
                                     ,cst_ctl_grp.cc_cnt.alias('based_cust'))     

ctl_grp_kpi_bnd = get_cust_kpi(ctl_batv_txn)

print('Brand KPI result by Control customer group')
print('=' * 80)
ctl_grp_kpi_bnd.display()
## control
print('=' * 80)

## Export Brand Level file

## Target
file_tnm           = cmp_nm + '_o3_target_cust_kpi_brandlevel.csv'
#full_tnm           = cmp_out_path_fl + file_tnm
trg_grp_kpi_bnd_pd = trg_grp_kpi_bnd.toPandas()

#trg_grp_kpi_bnd_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')
pandas_to_csv_filestore(trg_grp_kpi_bnd_pd, file_tnm, prefix=os.path.join(cmp_out_path_fl, 'result'))

## control
file_cnm           = cmp_nm + '_o3_control_cust_kpi_brandlevel.csv'
#full_cnm           = cmp_out_path_fl + file_tnm
ctl_grp_kpi_bnd_pd = ctl_grp_kpi_bnd.toPandas()

#ctl_grp_kpi_bnd_pd.to_csv(full_cnm, index = False, header = True, encoding = 'utf8')
pandas_to_csv_filestore(ctl_grp_kpi_bnd_pd, file_cnm, prefix=os.path.join(cmp_out_path_fl, 'result'))


# COMMAND ----------

# MAGIC %md ### SKU level conversion

# COMMAND ----------

## All group
trg_skuatv_txn = sku_txn_dur.join  (cst_trg_all, sku_txn_dur.household_id == cst_trg_all.household_id, 'inner')\
                            .where (sku_txn_dur.date_id >= cst_trg_all.first_vis)\
                            .select(sku_txn_dur['*']
                                   ,cst_trg_all.cust_group
                                   )
## control has no date (as no media to expose)                         
ctl_skuatv_txn = sku_txn_dur.join (cst_ctl_all, sku_txn_dur.household_id == cst_ctl_all.household_id, 'inner')\
                            .select(sku_txn_dur['*']
                                   ,cst_ctl_all.cust_group
                                   )

##----------------------------------------------------
## brand level KPI
##----------------------------------------------------
## target 
trg_skuatv_txn  = trg_skuatv_txn.join  (cst_trg_grp, trg_skuatv_txn.cust_group == cst_trg_grp.cust_group, 'inner')\
                                  .select(trg_skuatv_txn['*']
                                         ,cst_trg_grp.cc_cnt.alias('based_cust'))            

trg_grp_kpi_sku = get_cust_kpi(trg_skuatv_txn)

print('=' * 80)
print('SKU KPI result by Target customer group')
print('=' * 80)
trg_grp_kpi_sku.display()
## control
print('=' * 80)

## for control goup
ctl_skuatv_txn  = ctl_skuatv_txn.join  (cst_ctl_grp, ctl_skuatv_txn.cust_group == cst_ctl_grp.cust_group, 'inner')\
                                .select(ctl_skuatv_txn['*']
                                       ,cst_ctl_grp.cc_cnt.alias('based_cust'))     

ctl_grp_kpi_sku = get_cust_kpi(ctl_skuatv_txn)

print('SKU KPI result by Control customer group')
print('=' * 80)
ctl_grp_kpi_sku.display()
## control
print('=' * 80)

## Export Brand Level file

## Target
file_tnm           = cmp_nm + '_o3_target_cust_kpi_skulevel.csv'
#full_tnm           = cmp_out_path_fl + file_tnm
trg_grp_kpi_sku_pd = trg_grp_kpi_sku.toPandas()

#trg_grp_kpi_bnd_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')
pandas_to_csv_filestore(trg_grp_kpi_sku_pd, file_tnm, prefix=os.path.join(cmp_out_path_fl, 'result'))

## control
file_cnm           = cmp_nm + '_o3_control_cust_kpi_skulevel.csv'
#full_cnm           = cmp_out_path_fl + file_tnm
ctl_grp_kpi_sku_pd = ctl_grp_kpi_sku.toPandas()

#ctl_grp_kpi_bnd_pd.to_csv(full_cnm, index = False, header = True, encoding = 'utf8')
pandas_to_csv_filestore(ctl_grp_kpi_sku_pd, file_cnm, prefix=os.path.join(cmp_out_path_fl, 'result'))


# COMMAND ----------

# MAGIC %md ## Export Cost per head

# COMMAND ----------

out_data_list = []

if flag_ism == 1 :
    print('Cost per head OnShelf customers   = ' + str(all_pcst_cost_is) + ' THB')
    out_data_list.append(all_pcst_cost_is)
else:
    all_pcst_cost_is = 0
    out_data_list.append(all_pcst_cost_is)
##
if flag_ca == 1 :    
    print('Cost per head online customers    = ' + str(pcst_cost_ca) + ' THB')
    out_data_list.append(pcst_cost_ca)
else:
    pcst_cost_ca = 0
    out_data_list.append(pcst_cost_ca)
## end if
if flag_dgs == 1 :
    print('Cost per head OutOfHome customers = ' + str(pcst_cost_dgs) + ' THB')
    out_data_list.append(pcst_cost_dgs)
else:
    pcst_cost_dgs = 0
    out_data_list.append(pcst_cost_dgs)
## end if
## For IS_CA ------
pcst_cost_isca = all_pcst_cost_is + pcst_cost_ca
out_data_list.append(pcst_cost_isca)
## ----------------
if flag_dgsis == 1:
    print('Cost per head OOH+IS customers    = ' + str(pcst_cost_dgsis) + ' THB')
    out_data_list.append(pcst_cost_dgsis)
else:
    pcst_cost_dgsis = 0
    out_data_list.append(pcst_cost_dgsis)
## end if

## For CA-DGS ------
pcst_cost_cadgs = pcst_cost_dgs + pcst_cost_ca
out_data_list.append(pcst_cost_cadgs)
## ----------------
## For IS_CA-DGS ------
pcst_cost_o3 = pcst_cost_dgsis + pcst_cost_ca
out_data_list.append(pcst_cost_o3)
## ----------------

out_col = [ 'is_pcst_cost'
           ,'ca_pcst_cost'
           ,'dgs_pcst_cost'
           ,'is_ca_pcst_cost'
           ,'is_dgs_pcst_cost'
           ,'ca_dgs_pcst_cost'
           ,'is_ca_dgs_pcst_cost'
           ,]
  
out_cost_pd = pd.DataFrame([out_data_list], columns = out_col)

out_cost_pd.display()

file_nm     = cmp_nm + '_o3_per_cust_cost_info.csv'

pandas_to_csv_filestore(out_cost_pd, file_nm, prefix=os.path.join(cmp_out_path_fl, 'result'))


# COMMAND ----------

# MAGIC %md ## Drop unused table for customer group conversion

# COMMAND ----------

spark.sql('drop table if exists ' + cst_ishm_tab  )
spark.sql('drop table if exists ' + cst_isstr_tab )
spark.sql('drop table if exists ' + cst_isall_tab )
spark.sql('drop table if exists ' + cst_dgs_tab   )
spark.sql('drop table if exists ' + cst_isdgs_tab )
spark.sql('drop table if exists ' + cst_ca_all_tab)
## cust target all >> keep target cust table
#cst_trg_tab   = 'tdm_dev.media_campaign_eval_cust_target_all_data_o3_' + cmp_id.lower()

## control - set 1 
spark.sql('drop table if exists ' + cst_nis_tab     )
spark.sql('drop table if exists ' + cst_ntrg_hde_tab)
spark.sql('drop table if exists ' + cst_ntrg_oth_tab)

## cust control all >> keep target cust table
#cst_ctl_tab   = 'tdm_dev.media_campaign_eval_cust_control_all_data_o3_' + cmp_id.lower()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Break-point for Standard report (Exposure report only)

# COMMAND ----------

if eval_type == 'std':
  
     ## add check if zip file exists will remove and create new
    full_file_zip = cmp_out_path_fl + str(cmp_nm) + '_all_eval_result.zip'
    
    ## 8 Jan 2024, found problem in creating zip file, comment this zip file path out as Azure storage explorere is easier to download -- Paksirinat
    
    # try:
    #     dbutils.fs.ls(full_file_zip)
    #     print('-' * 80 + '\n' + ' Warning !! Current zip file exists : ' + full_file_zip + '.\n Process will remove and recreate zip file. \n' + '-' * 80)    
    #     dbutils.fs.rm(full_file_zip)    
    #     print(' Removed file already! Process will re-create new file. \n')    
    # except :
    #     print(' Zip file : ' + str(full_file_zip) + ' is creating, please wait \n')
        
        
    # create_zip_from_dbsf_prefix_indir(cmp_out_path_fl, f'{cmp_nm}_all_eval_result.zip')
    
    ## Add clean up temp table from p'Danny object  -- Pat 3 Aug 2023
    
    ## from utils import cleanup

    cleanup.clear_attr_and_temp_tbl(cmp)
    ## ----------------------------------------
    
    dbutils.notebook.exit('Finish Standard Evaluation for campaign ' + str(cmp_nm) + ', Exit status = 0 .')


# COMMAND ----------



# COMMAND ----------

# MAGIC %md #Full Evaluation

# COMMAND ----------

# MAGIC %md ##Store matching

# COMMAND ----------

## for testing only
# txn_all = sqlContext.table('tdm_seg.media_campaign_eval_txn_data_2022_0136_M02E')
# print(dbfs_project_path)

# COMMAND ----------

# txn_all.printSchema()

# COMMAND ----------

## call new matching auto select product level to do matching
# if wk_type == 'fis_wk' :
    
#     ctr_store_list, store_matching_df = get_store_matching_at( txn_all
#                                                               ,pre_en_wk =pre_en_wk
#                                                               ,brand_df  = brand_df
#                                                               ,sel_sku   = feat_list
#                                                               ,test_store_sf = trg_str_df
#                                                               ,reserved_store_sf=u_ctl_str_df
#                                                               ,matching_methodology='varience')
# elif wk_type == 'promo_wk' :
#     ctr_store_list, store_matching_df = get_store_matching_promo_at( txn_all
#                                                                 ,pre_en_promowk = pre_en_promo_wk
#                                                                 ,brand_df = brand_df
#                                                                 ,sel_sku  = feat_list
#                                                                 ,test_store_sf = trg_str_df
#                                                                 ,reserved_store_sf = u_ctl_str_df
#                                                                 ,matching_methodology = 'varience')
# ## end if
    
# ## Export to csv file
# pandas_to_csv_filestore(store_matching_df, 'store_matching.csv', prefix= os.path.join(dbfs_project_path, 'output'))

#print('-'*80 + '\n Store Matching information Show below \n' + '-'*80)

#--- New store matching code - Danny 28 Jan 2023
ctr_store_list, store_matching_df = get_store_matching_across_region(txn=txn_all,
                                                       pre_en_wk=pre_en_wk,
                                                       wk_type=wk_type,
                                                       feat_sf=feat_df,
                                                       brand_df=brand_df,
                                                       sclass_df=sclass_df,
                                                       test_store_sf=trg_str_df,
                                                       reserved_store_sf=u_ctl_str_df,
                                                       matching_methodology="cosine_distance",
                                                       bad_match_threshold=2.5,
                                                       dbfs_project_path=dbfs_project_path)

pandas_to_csv_filestore(store_matching_df, 'store_matching.csv', prefix= os.path.join(dbfs_project_path, 'output'))


# COMMAND ----------

# MAGIC %md ## Customer Share and KPI

# COMMAND ----------

store_match_df = spark.createDataFrame(store_matching_df)
#store_match_df.display()

if wk_type == 'fis_wk' :
    combined_kpi, kpi_df, df_pv = cust_kpi_by_mech( txn_all
                                           ,store_fmt = store_fmt
                                           ,store_match_df = store_match_df
                                           ,feat_df   = feat_df
                                           ,brand_df  = brand_df
                                           ,sclass_df = sclass_df
                                           ,class_df  = class_df)
elif wk_type == 'promo_wk':
    combined_kpi, kpi_df, df_pv = cust_kpi_promo_wk_by_mech( txn_all
                                           ,store_fmt = store_fmt
                                           ,store_match_df = store_match_df
                                           ,feat_df   = feat_df
                                           ,brand_df  = brand_df
                                           ,sclass_df = sclass_df
                                           ,class_df  = class_df)
## end if
                        
pandas_to_csv_filestore(kpi_df, 'kpi_test_ctrl_pre_dur.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
pandas_to_csv_filestore(df_pv, 'customer_share_test_ctrl_pre_dur.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md ##Customer Uplift

# COMMAND ----------

# MAGIC %md ### Brand Level

# COMMAND ----------

## Change to use new customer uplift from pDanny code -- 24 Jul 2023  Pat

# COMMAND ----------

#---- Uplift at brand level : Danny 1 Aug 2022
# uplift_brand = get_customer_uplift(txn=txn_all, 
#                                    cp_start_date=cmp_st_date, 
#                                    cp_end_date=cmp_end_date,
#                                    wk_type = week_type,
#                                    test_store_sf=test_store_sf,
#                                    adj_prod_sf=adj_prod_sf, 
#                                    brand_sf=brand_df,
#                                    feat_sf=feat_df,
#                                    ctr_store_list=ctr_store_list,
#                                    cust_uplift_lv="brand")

# uplift_brand_df = to_pandas(uplift_brand)
# pandas_to_csv_filestore(uplift_brand_df, 'customer_uplift_brand.csv', prefix=os.path.join(cmp_out_path, 'result'))

# COMMAND ----------

# # Customer uplift by mech : Ta 15 Sep 2022

# #store_matching_df_var = spark.createDataFrame(store_matching_df).select('store_id', 'ctr_store_var')
# # store_matching_df_var = spark.createDataFrame(store_matching_df).select('store_id', 'ctr_store_cos')

# # uplift_out_brand, exposed_unexposed_buy_flag_by_mech_brand = get_customer_uplift_per_mechanic(txn=txn_all,
# #                                                                                           cp_start_date=cmp_st_date,
# #                                                                                           cp_end_date=cmp_end_date,
# #                                                                                           wk_type=week_type,
# #                                                                                           test_store_sf=test_store_sf,
# #                                                                                           adj_prod_sf=use_ai_df,
# #                                                                                           brand_sf=brand_df,
# #                                                                                           feat_sf=feat_df,
# #                                                                                           ctr_store_list=ctr_store_list,
# #                                                                                           cust_uplift_lv="brand", store_matching_df_var=store_matching_df_var)

# # uplift_out_brand_df = to_pandas(uplift_out_brand)
# # pandas_to_csv_filestore(uplift_out_brand_df, 'customer_uplift_brand_by_mech.csv', prefix=os.path.join(cmp_out_path, 'result'))


cust_uplift       = uplift.get_cust_uplift_by_mech(cmp, brand_df, "brand")

cust_uplift_brand = cust_uplift.select( cust_uplift.customer_mv_group.alias('customer_group')
                                       ,cust_uplift.mech_name.alias('mechanic')
                                       ,cust_uplift.exposed_custs
                                       ,cust_uplift.unexposed_custs
                                       ,cust_uplift.exposed_purchased_custs
                                       ,cust_uplift.unexposed_purchased_custs
                                       ##,cust_uplift.uplift_lv  ## >> exclude this column as incompat with 
                                       ,cust_uplift.cvs_rate_exposed
                                       ,cust_uplift.cvs_rate_unexposed
                                       ,cust_uplift.pct_uplift
                                       ,cust_uplift.uplift_cust
                                       ,cust_uplift.pstv_cstmr_uplift
                                       ,cust_uplift.pct_positive_cust_uplift)

## Change to use new customer uplift from pDanny code -- 24 Jul 2023  Pat

uplift_out_brand_df = to_pandas(cust_uplift_brand)
pandas_to_csv_filestore(uplift_out_brand_df, 'customer_uplift_brand_by_mech.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))


# COMMAND ----------

# MAGIC %md ### SKU Level

# COMMAND ----------

# # #---- Uplift at Sku : Danny 1 Aug 2022

# # uplift_feature = get_customer_uplift(txn=txn_all, 
# #                                    cp_start_date=cmp_st_date, 
# #                                    cp_end_date=cmp_end_date,
# #                                    wk_type=week_type,
# #                                    test_store_sf=test_store_sf,
# #                                    adj_prod_sf=adj_prod_sf, 
# #                                    brand_sf=brand_df,
# #                                    feat_sf=feat_df,
# #                                    ctr_store_list=ctr_store_list,
# #                                    cust_uplift_lv="sku")

# # uplift_feature_df = to_pandas(uplift_feature)
# # pandas_to_csv_filestore(uplift_feature_df, 'customer_uplift_features_sku.csv', prefix=os.path.join(cmp_out_path, 'result'))


cust_uplift     = uplift.get_cust_uplift_by_mech(cmp, feat_df, "sku")

cust_uplift_sku = cust_uplift.select( cust_uplift.customer_mv_group.alias('customer_group')
                                       ,cust_uplift.mech_name.alias('mechanic')
                                       ,cust_uplift.exposed_custs
                                       ,cust_uplift.unexposed_custs
                                       ,cust_uplift.exposed_purchased_custs
                                       ,cust_uplift.unexposed_purchased_custs
                                       ##,cust_uplift.uplift_lv  ## >> exclude this column as incompat with 
                                       ,cust_uplift.cvs_rate_exposed
                                       ,cust_uplift.cvs_rate_unexposed
                                       ,cust_uplift.pct_uplift
                                       ,cust_uplift.uplift_cust
                                       ,cust_uplift.pstv_cstmr_uplift
                                       ,cust_uplift.pct_positive_cust_uplift)


## Change to use new customer uplift from pDanny code -- 24 Jul 2023  Pat

uplift_feature_df = to_pandas(cust_uplift_sku)
pandas_to_csv_filestore(uplift_feature_df, 'customer_uplift_sku_by_mech.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))


# COMMAND ----------

# # Customer uplift by mech : Ta 15 Sep 2022

# #store_matching_df_var = spark.createDataFrame(store_matching_df).select('store_id', 'ctr_store_var')

# uplift_out_sku, exposed_unexposed_buy_flag_by_mech_sku = get_customer_uplift_per_mechanic(txn=txn_all,
#                                                                                           cp_start_date=cmp_st_date,
#                                                                                           cp_end_date=cmp_end_date,
#                                                                                           wk_type=week_type,
#                                                                                           test_store_sf=test_store_sf,
#                                                                                           adj_prod_sf=use_ai_df,
#                                                                                           brand_sf=brand_df,
#                                                                                           feat_sf=feat_df,
#                                                                                           ctr_store_list=ctr_store_list,
#                                                                                           cust_uplift_lv="sku", store_matching_df_var=store_matching_df_var)

# uplift_out_sku_df = to_pandas(uplift_out_sku)
# pandas_to_csv_filestore(uplift_out_sku_df, 'customer_uplift_sku_by_mech.csv', prefix=os.path.join(cmp_out_path, 'result'))

# COMMAND ----------

# MAGIC %md ##CLTV

# COMMAND ----------

uplift_brand_df = pd.read_csv(os.path.join(cmp_out_path_fl, 'result', 'customer_uplift_brand_by_mech.csv'))
uplift_brand = spark.createDataFrame(uplift_brand_df)

brand_cltv, brand_svv = get_cust_cltv(txn_all,
                                      cmp_id=cmp_id,
                                      wk_type=week_type,
                                      feat_sf=feat_df,
                                      brand_sf=brand_df,
                                      lv_svv_pcyc=cate_lvl,
                                      uplift_brand=uplift_brand,
                                      media_spend=float(media_fee),
                                      svv_table=svv_table,
                                      pcyc_table=pcyc_table,
                                      cate_cd_list=cate_cd_list,
                                      store_format=store_fmt
                                     )

pandas_to_csv_filestore(brand_cltv, 'cltv.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
pandas_to_csv_filestore(brand_svv, 'brand_survival_rate.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category average survival rate

# COMMAND ----------

svv_df       = sqlContext.table(svv_table)
cate_avg_svv = get_avg_cate_svv(svv_df, cate_lvl, cate_cd_list)

cate_avg_svv.display()

## export to csv to output path
cate_avg_svv_pd = cate_avg_svv.toPandas()
outfile         = cmp_out_path_fl + 'result/' + 'cate_avg_svv.csv'
cate_avg_svv_pd.to_csv(outfile, index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uplift by region & mechanics

# COMMAND ----------

# #del txn_all

# txn_all = sqlContext.table('tdm_seg.media_campaign_eval_txn_data_2022_0136_M02E')
# matching_spkapi  = 'dbfs:/FileStore/media/promozone_eval/2022_0136_M02E_HYGIENE/output/store_matching.csv'
# matching_fileapi =  '/dbfs/FileStore/media/promozone_eval/2022_0136_M02E_HYGIENE/output/store_matching.csv'


# COMMAND ----------

# ## check schema txn
# txn_all.printSchema()
# txn_all.display(10)

# COMMAND ----------

# store_matching_df = pd.read_csv(matching_fileapi)
# store_matching_df.display()

# COMMAND ----------

#%md ###Recall util2 for test will be removed

# COMMAND ----------

#%run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

# MAGIC %md ###uplift fisweek SKU

# COMMAND ----------

## Change to call sale uplift by region + mechanics set -- Pat 7 Sep 2022
if wk_type == 'fis_wk' :
    ## SKU Level
    sku_sales_matching_df, sku_uplift_table, sku_uplift_wk_graph, kpi_table, uplift_reg_pd, uplift_by_mech_pd = sales_uplift_reg_mech( txn_all 
                                                                                                                                       ,sales_uplift_lv='sku'
                                                                                                                                       ,brand_df = brand_df
                                                                                                                                       ,feat_list = feat_list
                                                                                                                                       ,matching_df=store_matching_df
                                                                                                                                       ,matching_methodology="cosine_sim")

## end if    

# COMMAND ----------

## Convert uplift df from row to columns and add period identifier
if wk_type == 'fis_wk': 
  
  if sku_sales_matching_df.empty :
    print('No data for SKU sales level matching - This could occured in the NPD campaigns \n')
    ## create empty dataframe with header
    pd_cols       = ['week','sku_test', 'sku_ctr', 'wk_period']
    pd_data       = [ ['','','','','']]    
    sku_wk_uplift = pd.DataFrame(pd_data, columns = pd_cols)

    kpi_fiswk_t   = pd.DataFrame() 
  else :
    
    sku_wk_g = sku_uplift_wk_graph.reset_index()
    sku_wk_g.rename(columns = {'index' : 'week'}, inplace = True)
    #sku_wk_g.display()
    
    sku_wk_t              = sku_wk_g.T.reset_index()
    hdr                   = sku_wk_t.iloc[0]  ## get header from first row
    sku_wk_uplift         = sku_wk_t[1:]      ## get data start from row 1 (row 0 is header)
    sku_wk_uplift.columns = hdr         ## set header to df
    
    #sku_wk_uplift.display()
    
    #sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
    sku_wk_uplift['wk_period'] = np.where(sku_wk_uplift.loc[:, ('week')].astype(int) < chk_pre_wk, 'pre', 'dur')   ## change to use chk_pre_week instead of campaign start week
    
    print('\n' + '-'*80)
    print(' Display sku_wk_uplift for Trend chart : column mode ')
    print('-'*80)
    sku_wk_uplift.display()
    
    ## KPI table transpose
    kpi_fiswk_t = kpi_table.T.reset_index()
    kpi_fiswk_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
    print('-'*80)
    print(' Display kpi_fiswk_table : column mode ')
    print('-'*80)
    kpi_fiswk_t.display()
  ## end if   
## end if

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uplift promo SKU
# MAGIC

# COMMAND ----------

if wk_type == 'promo_wk' :
    # ## call sale uplift by region & mechanics -- Pat 10 Sep 2022

    ## SKU Level
    sku_sales_matching_promo_df, sku_uplift_promo_table, sku_uplift_promowk_graph, kpi_table_promo, uplift_promo_reg_pd, uplift_promo_mech_pd = sales_uplift_promo_reg_mech( txn_all 
                                                                                                                                                                            ,sales_uplift_lv='sku'
                                                                                                                                                                            ,brand_df    = brand_df
                                                                                                                                                                            ,feat_list   = feat_list
                                                                                                                                                                            ,matching_df = store_matching_df
                                                                                                                                                                            ,period_col  = 'period_promo_wk'
                                                                                                                                                                            ,matching_methodology="cosine_sim")
 ## endif   

# COMMAND ----------

# # ## call sale uplift by region -- Pat 25 May 2022

# ## SKU Level
# sku_sales_matching_promo_df, sku_uplift_promo_table, sku_uplift_promowk_graph, kpi_table_promo, uplift_promo_reg_pd = sales_uplift_promo_reg( txn_all 
#                                                                                                                                              ,sales_uplift_lv='sku'
#                                                                                                                                              ,brand_df = brand_df
#                                                                                                                                              ,feat_list = feat_list
#                                                                                                                                              ,matching_df=store_matching_df
#                                                                                                                                              ,matching_methodology='varience')


# COMMAND ----------

##---------------------------
## Trend chart by promo week

if wk_type == 'promo_wk' :

  if sku_sales_matching_promo_df.empty :
    print('No data for SKU sales level matching (pre-period) - This could occured in the NPD campaigns \n')
    sku_promowk_uplift = pd.DataFrame()
    kpi_promo_t        = pd.DataFrame()
  else:
    sku_promowk_g = sku_uplift_promowk_graph.reset_index()
    sku_promowk_g.rename(columns = {'index' : 'promo_week'}, inplace = True)
    #sku_promowk_g.display()
    
    sku_promowk_t              = sku_promowk_g.T.reset_index()
    hdr                        = sku_promowk_t.iloc[0]  ## get header from first row
    sku_promowk_uplift         = sku_promowk_t[1:]      ## get data start from row 1 (row 0 is header)
    sku_promowk_uplift.columns = hdr                    ## set header to df
    
    #sku_wk_uplift.display()
    
    #sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
    sku_promowk_uplift['wk_period'] = np.where(sku_promowk_uplift.loc[:, ('promo_week')].astype(int) < chk_pre_wk, 'pre', 'dur')  ## change to use chk_pre_week instead of campaign start week
    
    print('\n' + '-'*80)
    print(' Display sku_wk_uplift for Trend chart : column mode ')
    print('-'*80)
    sku_promowk_uplift.display()
    
    ## KPI table transpose
    kpi_promo_t = kpi_table_promo.T.reset_index()
    kpi_promo_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
    print('-'*80)
    print(' Display kpi_promo_table : column mode ')
    print('-'*80)
    kpi_promo_t.display()
  ## end if
## end if


# COMMAND ----------

# MAGIC %md
# MAGIC ### Export file - uplift at level feature SKU

# COMMAND ----------

if wk_type == 'fis_wk' :
    pandas_to_csv_filestore(sku_uplift_table, 'sales_sku_uplift_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(sku_uplift_wk_graph.reset_index(), 'sales_sku_uplift_wk_graph.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat change to column mode Dataframe for weekly trend
    pandas_to_csv_filestore(sku_wk_uplift, 'sales_sku_uplift_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(sku_promowk_uplift, 'sales_sku_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift region
    pandas_to_csv_filestore(uplift_reg_pd, 'sales_sku_uplift_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(uplift_promo_reg_pd, 'sales_sku_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift by mechanic set
    pandas_to_csv_filestore(uplift_by_mech_pd, 'sales_sku_uplift_by_mechanic_set_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(uplift_promo_reg_pd, 'sales_sku_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add KPI table -- 8 May 2022
    
    pandas_to_csv_filestore(kpi_fiswk_t, 'sale_kpi_target_control_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(kpi_promo_t, 'sale_kpi_target_control_promo_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign
    
    pandas_to_csv_filestore(sku_sales_matching_df, 'sku_sales_matching_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))
    #pandas_to_csv_filestore(sku_sales_matching_promo_df, 'sku_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))

elif wk_type == 'promo_wk' :

    #sku_uplift_promo_table
    pandas_to_csv_filestore(sku_uplift_promo_table, 'sales_sku_uplift_promo_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    #pandas_to_csv_filestore(sku_uplift_wk_graph.reset_index(), 'sales_sku_uplift_wk_graph.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat change to column mode Dataframe for weekly trend
    #pandas_to_csv_filestore(sku_wk_uplift, 'sales_sku_uplift_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sku_promowk_uplift, 'sales_sku_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift region
    #pandas_to_csv_filestore(uplift_reg_pd, 'sales_sku_uplift_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(uplift_promo_reg_pd, 'sales_sku_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Uplift by media mechanic setup --> uplift_promo_mech_pd
    
    pandas_to_csv_filestore(uplift_promo_mech_pd, 'sales_sku_uplift_promo_wk_by_mechanic_set_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add KPI table -- 8 May 2022
    
    #pandas_to_csv_filestore(kpi_fiswk_t, 'sale_kpi_target_control_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(kpi_promo_t, 'sale_kpi_target_control_promo_feat.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign
        
    pandas_to_csv_filestore(sku_sales_matching_promo_df, 'sku_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))


    ## end if

# COMMAND ----------

# MAGIC %md ###uplift fisweek Brand

# COMMAND ----------

#brand_df.printSchema()

# COMMAND ----------

## CHange to call sale uplift by region + Mechanics set -- Pat 7 Sep 2022
if wk_type == 'fis_wk' :
    ## Brand Level
    
    bnd_sales_matching_df, bnd_uplift_table, bnd_uplift_wk_graph, kpi_table_bnd, uplift_reg_bnd_pd, uplift_by_mech_bnd_pd = sales_uplift_reg_mech( txn_all 
                                                                                                                                              ,sales_uplift_lv='brand'
                                                                                                                                              ,brand_df = brand_df
                                                                                                                                              ,feat_list = feat_list
                                                                                                                                              ,matching_df=store_matching_df
                                                                                                                                              ,matching_methodology="cosine_sim")
                            
## end if    

# COMMAND ----------

## Convert uplift df from row to columns and add period identifier
if wk_type == 'fis_wk' :
  
  if bnd_sales_matching_df.empty :
    print('No data for Brand sales level matching (pre-period) - This could occured in the NPD campaigns \n')
    bnd_wk_uplift      = pd.DataFrame()
    kpi_fiswk_bnd_t    = pd.DataFrame()
  else :

    bnd_wk_g = bnd_uplift_wk_graph.reset_index()
    bnd_wk_g.rename(columns = {'index' : 'week'}, inplace = True)
    #sku_wk_g.display()
    
    bnd_wk_t              = bnd_wk_g.T.reset_index()
    hdr                   = bnd_wk_t.iloc[0]  ## get header from first row
    bnd_wk_uplift         = bnd_wk_t[1:]      ## get data start from row 1 (row 0 is header)
    bnd_wk_uplift.columns = hdr         ## set header to df
    
    
    #sku_wk['wk_period'] = np.where(sku_wk['week'].astype(int) < cmp_st_wk, 'pre', 'dur')
    bnd_wk_uplift['wk_period'] = np.where(bnd_wk_uplift.loc[:, ('week')].astype(int) < chk_pre_wk, 'pre', 'dur')
    
    print('\n' + '-'*80)
    print(' Display brand_wk_uplift for Trend chart : column mode ')
    print('-'*80)
    bnd_wk_uplift.display()
    
    ## KPI table transpose
    kpi_fiswk_bnd_t = kpi_table_bnd.T.reset_index()
    kpi_fiswk_bnd_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
    print('-'*80)
    print(' Display kpi_fiswk_table : column mode ')
    print('-'*80)
    kpi_fiswk_bnd_t.display()
  ## end if
## end if    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Uplift Promo Brand

# COMMAND ----------

# ## call sale uplift promo week by region -- Pat 31 May 2022
## promo will not have KPI

# ## call sale uplift by region -- Pat 25 May 2022
if wk_type == 'promo_wk' :
    ## Brand Level
    bnd_sales_matching_promo_df, bnd_uplift_promo_table, bnd_uplift_promowk_graph, kpi_table_promo_bnd, uplift_promo_reg_bnd_pd, uplift_promo_mech_bnd_pd = sales_uplift_promo_reg_mech( txn_all 
                                                                                                                                                                              ,sales_uplift_lv='brand'
                                                                                                                                                                              ,brand_df    = brand_df
                                                                                                                                                                              ,feat_list   = feat_list
                                                                                                                                                                              ,matching_df =store_matching_df
                                                                                                                                                                              ,period_col  = 'period_promo_wk'
                                                                                                                                                                              ,matching_methodology="cosine_sim")

 ## end if

# COMMAND ----------

##---------------------------
## promo wk - brand Transpose
##---------------------------

if wk_type == 'promo_wk' :
  
  if bnd_sales_matching_promo_df.empty :
    print('No data for Brand sales level matching (pre-period) - This could occured in the NPD campaigns \n')
    bnd_promowk_uplift   = pd.DataFrame()
    kpi_promowk_bnd_t    = pd.DataFrame()
  else :

    bnd_promowk_g = bnd_uplift_promowk_graph.reset_index()
    bnd_promowk_g.rename(columns = {'index' : 'promo_week'}, inplace = True)
    #bnd_promowk_g.display()
    
    bnd_promowk_t              = bnd_promowk_g.T.reset_index()
    hdr                        = bnd_promowk_t.iloc[0]  ## get header from first row
    bnd_promowk_uplift         = bnd_promowk_t[1:]      ## get data start from row 1 (row 0 is header)
    bnd_promowk_uplift.columns = hdr                    ## set header to df
    
    bnd_promowk_uplift['wk_period'] = np.where(bnd_promowk_uplift.loc[:, ('promo_week')].astype(int) < chk_pre_wk, 'pre', 'dur')  ## change to use chk_pre_week instead of campaign start week
    
    print('\n' + '-'*80)
    print(' Display brand_promowk_uplift for Trend chart : column mode ')
    print('-'*80)
    bnd_promowk_uplift.display()
    
    ## KPI table transpose
    kpi_promowk_bnd_t = kpi_table_promo_bnd.T.reset_index()
    kpi_promowk_bnd_t.rename(columns = {'index': 'kpi_value'}, inplace = True)
    print('-'*80)
    print(' Display kpi_promo_table : column mode ')
    print('-'*80)
    kpi_promowk_bnd_t.display()
  ## end if
## end if

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export file - uplift at level Brand

# COMMAND ----------

if wk_type == 'fis_wk' :
    pandas_to_csv_filestore(bnd_uplift_table, 'sales_brand_uplift_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(sku_uplift_wk_graph.reset_index(), 'sales_sku_uplift_wk_graph.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat change to column mode Dataframe for weekly trend
    pandas_to_csv_filestore(bnd_wk_uplift, 'sales_brand_uplift_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(bnd_promowk_uplift, 'sales_brand_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift region
    pandas_to_csv_filestore(uplift_reg_bnd_pd, 'sales_brand_uplift_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(uplift_promo_reg_bnd_pd, 'sales_brand_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Uplift by mechanic setup 
    pandas_to_csv_filestore(uplift_by_mech_bnd_pd, 'sales_brand_uplift_by_mechanic_set_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add KPI table -- 8 May 2022
    
    pandas_to_csv_filestore(kpi_fiswk_bnd_t, 'sale_kpi_target_control_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    #pandas_to_csv_filestore(kpi_promowk_bnd_t, 'sale_kpi_target_control_promo_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign
    
    pandas_to_csv_filestore(bnd_sales_matching_df, 'brand_sales_matching_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))
    #pandas_to_csv_filestore(bnd_sales_matching_promo_df, 'brand_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))

elif wk_type == 'promo_wk' :
    
    pandas_to_csv_filestore(bnd_uplift_promo_table, 'sales_brand_uplift_promo_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat change to column mode Dataframe for weekly trend

    pandas_to_csv_filestore(bnd_promowk_uplift, 'sales_brand_uplift_promo_wk_graph_col.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift region

    pandas_to_csv_filestore(uplift_promo_reg_bnd_pd, 'sales_brand_uplift_promo_wk_by_region_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## uplift by mechanic set up -- 7SEp2022  -- Pat
    ## uplift_promo_mech_bnd_pd
    pandas_to_csv_filestore(uplift_promo_mech_bnd_pd, 'sales_brand_uplift_promo_wk_by_mechanic_set_table.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add KPI table -- 8 May 2022
    

    pandas_to_csv_filestore(kpi_promowk_bnd_t, 'sale_kpi_target_control_promo_brand.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
    ## Pat add sku_sales_matching_df  -- to output path just for checking purpose in all campaign
    
    pandas_to_csv_filestore(bnd_sales_matching_promo_df, 'brand_sales_matching_promo_df_info.csv', prefix=os.path.join(dbfs_project_path, 'output'))

## end if

# COMMAND ----------

#class_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Growth fis week

# COMMAND ----------

## Code
## Change function, and call function 4 times manually, may excluded some line later

## def get_sales_mkt_growth( txn
##                          ,prod_df
##                          ,cate_df
##                          ,prod_level
##                          ,cate_level
##                          ,week_type
##                          ,store_format
##                          ,store_matching_df
##                         ):
## convert matching pandas dataframe to spark for this function

if wk_type == 'fis_wk' :
        
    store_matching_spk = spark.createDataFrame(store_matching_df)
    
    sales_brand_class_fiswk = get_sales_mkt_growth( txn_all
                                                      ,brand_df
                                                      ,class_df
                                                      ,'brand'
                                                      ,'class'
                                                      ,'fis_wk'
                                                      ,store_fmt
                                                      ,store_matching_spk
                                                     )
    
    sales_brand_subclass_fiswk = get_sales_mkt_growth( txn_all
                                                        ,brand_df
                                                        ,sclass_df
                                                        ,'brand'
                                                        ,'subclass'
                                                        ,'fis_wk'
                                                        ,store_fmt
                                                        ,store_matching_spk
                                                     )
    
    sales_sku_class_fiswk = get_sales_mkt_growth( txn_all
                                                   ,feat_df
                                                   ,class_df
                                                   ,'sku'
                                                   ,'class'
                                                   ,'fis_wk'
                                                   ,store_fmt
                                                   ,store_matching_spk
                                                  )
    
    sales_sku_subclass_fiswk = get_sales_mkt_growth( txn_all
                                                      ,feat_df
                                                      ,sclass_df
                                                      ,'sku'
                                                      ,'subclass'
                                                      ,'fis_wk'
                                                      ,store_fmt
                                                      ,store_matching_spk
                                                     )
    
    #sales_brand_class_fiswk.display()
    
    ## Export File
    
    pandas_to_csv_filestore(sales_brand_class_fiswk, 'sales_brand_class_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_brand_subclass_fiswk, 'sales_brand_subclass_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_sku_class_fiswk, 'sales_sku_class_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_sku_subclass_fiswk, 'sales_sku_subclass_growth_fiswk.csv', prefix=os.path.join(dbfs_project_path, 'result'))

## end if    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Growth promo week
# MAGIC

# COMMAND ----------

## Code
## Change function, and call function 4 times manually, may excluded some line later

## def get_sales_mkt_growth( txn
##                          ,prod_df
##                          ,cate_df
##                          ,prod_level
##                          ,cate_level
##                          ,week_type
##                          ,store_format
##                          ,store_matching_df
##                         ):
## convert matching pandas dataframe to spark for this function

if wk_type == 'promo_wk' :

    store_matching_spk = spark.createDataFrame(store_matching_df)
    
    sales_brand_class_promowk = get_sales_mkt_growth( txn_all
                                                      ,brand_df
                                                      ,class_df
                                                      ,'brand'
                                                      ,'class'
                                                      ,'promo_wk'
                                                      ,store_fmt
                                                      ,store_matching_spk
                                                     )
    
    sales_brand_subclass_promowk = get_sales_mkt_growth( txn_all
                                                        ,brand_df
                                                        ,sclass_df
                                                        ,'brand'
                                                        ,'subclass'
                                                        ,'promo_wk'
                                                        ,store_fmt
                                                        ,store_matching_spk
                                                     )
    
    sales_sku_class_promowk = get_sales_mkt_growth( txn_all
                                                   ,feat_df
                                                   ,class_df
                                                   ,'sku'
                                                   ,'class'
                                                   ,'promo_wk'
                                                   ,store_fmt
                                                   ,store_matching_spk
                                                  )
    
    sales_sku_subclass_promowk = get_sales_mkt_growth( txn_all
                                                      ,feat_df
                                                      ,sclass_df
                                                      ,'sku'
                                                      ,'subclass'
                                                      ,'promo_wk'
                                                      ,store_fmt
                                                      ,store_matching_spk
                                                     )
    
    #sales_brand_class_promowk.display()
    
    ## Export File
    
    pandas_to_csv_filestore(sales_brand_class_promowk, 'sales_brand_class_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_brand_subclass_promowk, 'sales_brand_subclass_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_sku_class_promowk, 'sales_sku_class_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    pandas_to_csv_filestore(sales_sku_subclass_promowk, 'sales_sku_subclass_growth_promowk.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
## end if

# COMMAND ----------

# MAGIC %md ## Cross Category metrics for campaign which need cross cateogry information  -- Added 24 Jul 2023  Pat
# MAGIC

# COMMAND ----------

## Call Thanakrit code for asso_basket.get_asso_kpi

xcate_flag = float(x_cate_flag)

if xcate_flag == 1 :
    print('#'*80)
    print(' Start runing metrics for cross category campaign ')
    print('#'*80)
    combine_df, lift_score_df, uplift_df = asso_basket.get_asso_kpi(cmp, feat_df)
    
    combine_pd   = to_pandas(combine_df)
    pandas_to_csv_filestore(combine_pd, 'crosscate_combine_metric.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
    del combine_pd
    
    liftscore_pd = to_pandas(lift_score_df)
    pandas_to_csv_filestore(liftscore_pd, 'crosscate_liftscore_metric.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
    del liftscore_pd

    uplift_pd = to_pandas(uplift_df)
    pandas_to_csv_filestore(uplift_pd, 'crosscate_uplift_metric.csv', prefix=os.path.join(cmp_out_path_fl, 'result'))
    del uplift_pd
        
else :
    print('#'*80)
    print(' This campaign has no cross category, will not have cross category metrics ')
    print('#'*80)

## end if


# COMMAND ----------

# MAGIC %md
# MAGIC ## Zip file for send out

# COMMAND ----------

## add check if zip file exists will remove and create new
full_file_zip = cmp_out_path + str(cmp_nm) + '_all_eval_result.zip'

## 8 Jan 2024, found problem in creating zip file, comment this zip file path out as Azure storage explorere is easier to download -- Paksirinat

# try:
#     dbutils.fs.ls(full_file_zip)
#     print('-' * 80 + '\n' + ' Warning !! Current zip file exists : ' + full_file_zip + '.\n Process will remove and recreate zip file. \n' + '-' * 80)    
#     dbutils.fs.rm(full_file_zip)    
#     print(' Removed file already! Process will re-create new file. \n')    
# except :
#     print(' Zip file : ' + str(full_file_zip) + ' is creating, please wait \n')
    
    
# create_zip_from_dbsf_prefix_indir(cmp_out_path_fl, f'{cmp_nm}_all_eval_result.zip')


# COMMAND ----------

# MAGIC %md ## Add step drop temp configuratin table from p'Danny code

# COMMAND ----------

#cmp_id      = '2023_0276_M01M_M03M'

# cmp_id_lowc = cmp_id.lower()

# sqltxt      = 'drop table if exists ' + 'tdm_seg.th_lotuss_media_eval_aisle_target_store_conf_' + cmp_id_lowc + '_temp'

# print('SQL command to drop = ' + sqltxt + '\n')

# sqlContext.sql(sqltxt) 

# Add clean up temp table from p'Danny object  -- Pat 3 Aug 2023
    
# from utils import cleanup

cleanup.clear_attr_and_temp_tbl(cmp)
##--------------------------------------

# COMMAND ----------

# %sql

# drop table if exists  tdm_seg.th_lotuss_media_eval_aisle_target_store_conf_2023_0276_m01m_m03m_temp

# -- select * from tdm_seg.th_lotuss_media_eval_aisle_target_store_conf_2023_0276_m01m_m03m_temp
