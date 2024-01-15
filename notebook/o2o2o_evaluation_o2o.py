# Databricks notebook source
sc

# COMMAND ----------

# MAGIC %md #Import Library

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
## Add 13 Jul 2023 -- Pat
from pyspark.sql import DataFrame

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

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# # ## check table
# main_ca = spark.table('tdm_seg.consolidate_ca_master')
# ca_grp  = spark.table('tdm_seg.consolidate_ca_group')
# cmp_inf = spark.table('tdm_seg.consolidate_ca_campaign')

# # # main_ca.where(main_ca.cmp_id == '2023_0355_0323')\
# # #        .groupBy(main_ca.flag_type, main_ca.group)\
# # #        .agg    (sum(lit(1)).alias('ca_cnt'))\
# # #        .display()

# COMMAND ----------

# MAGIC %md # Get parameter

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

## initial widget
dbutils.widgets.text('c_id', defaultValue='', label='01_Campaign ID = :')
dbutils.widgets.text('c_nm', defaultValue='', label='02_Campaign Name = :')
dbutils.widgets.text('c_id_ofn', defaultValue='', label='011_Campaign ID Offline = :')
dbutils.widgets.text('c_id_oln', defaultValue='', label='011_Campaign ID Online = :')

dbutils.widgets.text('cmp_st_dt', defaultValue='', label='03_Campaign Start Date = :')
dbutils.widgets.text('cmp_en_dt', defaultValue='', label='04_Campaign End Date = :')

dbutils.widgets.text('wk_type', defaultValue='', label='05_Campaign Week Type (fis_wk/promo_wk) = :')
#dbutils.widgets.text('brand_name', defaultValue='', label='06_Focus brand name = :')
dbutils.widgets.text('cate_lvl', defaultValue='', label='07_Category Level (class/subclass) = :')
dbutils.widgets.text('x_cate_flag', defaultValue='', label='08_Cross category flag (0 or 1) = :')
dbutils.widgets.text('x_cate_cd', defaultValue='', label='09_Cross Category code (if any) = :')
#dbutils.widgets.text('txn_tab', defaultValue='', label='Input transaction table = :')
dbutils.widgets.text('exp_sto_flg', defaultValue='', label='10_Store Level Exposure Flag = :')
dbutils.widgets.text('sprj_path', defaultValue='', label='11_Project path spark api = :')
dbutils.widgets.text('use_txn_ofn', defaultValue='', label='12_Flag if use offline campaign txn as initial (0 or 1) :')

## Add 14 Jul 2023 : flag if using consolidate CA table as source table -- Pat
dbutils.widgets.text('use_ca_tab', defaultValue='', label='13_Flag if use consolidate CA table value (0 or 1) = :')
## Online information

dbutils.widgets.text('reach_online', defaultValue='', label='14_#Reach Customers (online) = :')
dbutils.widgets.text('match_rate', defaultValue='', label='15_%Match Rate (online) = :')

## Other information

dbutils.widgets.text('offline_fee', defaultValue='', label='16_Media Fee (offline) = :')
dbutils.widgets.text('online_fee', defaultValue='', label='17_Media Fee (online) = :')
dbutils.widgets.text('adjacency_file', defaultValue='', label='18_adjacency_file value = :')

## read widgets

c_id          = dbutils.widgets.get('c_id').strip()
c_nm          = dbutils.widgets.get('c_nm').strip().lower()

c_id_ofn      = dbutils.widgets.get('c_id_ofn').strip()
c_id_oln      = dbutils.widgets.get('c_id_oln').strip()

#txn_tab       = dbutils.widgets.get('txn_tab').strip()
cate_lvl      = dbutils.widgets.get('cate_lvl').strip().lower()
#brand_name    = dbutils.widgets.get('brand_name').strip().lower()
exp_sto_flg   = dbutils.widgets.get('exp_sto_flg').strip().lower()
cmp_st_dt     = dbutils.widgets.get('cmp_st_dt').strip()
cmp_en_dt     = dbutils.widgets.get('cmp_en_dt').strip()
wk_type       = dbutils.widgets.get('wk_type').strip().lower()
x_cate_flag   = dbutils.widgets.get('x_cate_flag').strip().lower()
x_cate_cd     = dbutils.widgets.get('x_cate_cd').strip()
reach_online  = dbutils.widgets.get('reach_online').strip()
match_rate    = dbutils.widgets.get('match_rate').strip()
ofn_fee       = dbutils.widgets.get('offline_fee').strip()
oln_fee       = dbutils.widgets.get('online_fee').strip()
aisle_file    = dbutils.widgets.get('adjacency_file').strip()
sprj_path     = dbutils.widgets.get('sprj_path').strip()
use_txn_ofn   = dbutils.widgets.get('use_txn_ofn').strip()
use_ca_tab    = dbutils.widgets.get('use_ca_tab').strip()

## print read value

print( ' Campaign ID                = : '  +  c_id + '\n')
print( ' Campaign ID offline        = : '  +  c_id_ofn + '\n')
print( ' Campaign ID online         = : '  +  c_id_oln + '\n')
print( ' Campaign name              = : '  +  c_nm + '\n')
print( ' Campaign Start Date        = : '  +  cmp_st_dt + '\n')
print( ' Campaign End Date          = : '  +  cmp_en_dt + '\n')
print( ' Campaign Week Type         = : '  +  wk_type + '\n')
    
#print( ' txn_table name             = : '  +  txn_tab + '\n')
print( ' spark API project_path     = : '  +  sprj_path + '\n')
#print( ' Input path spark API       = : '  +  s_in_path + '\n')
print( ' Category Level             = : '  +  cate_lvl + '\n')
    
#print( ' Focus brand name           = : '  +  brand_name + '\n')
print( ' Store Level Exposure Flag  = : '  +  exp_sto_flg + '\n')
print( ' Cross Category Flag        = : '  +  x_cate_flag + '\n')
print( ' Cross Category code        = : '  +  x_cate_cd + '\n')
print( ' Use Offline txn init Flag  = : '  +  use_txn_ofn + '\n')
print( ' Use CA Table Flag          = : '  +  use_ca_tab + '\n')

print( ' #Reach Customers (online)  = : '  +  reach_online + '\n')
print( ' %Match Rate (online)       = : '  +  match_rate + '\n')

print( ' Media Fee (offline)        = : '  +  ofn_fee + '\n')
print( ' Media Fee (online)         = : '  +  oln_fee + '\n')

print( ' Use Adjacency File         = : '  +  aisle_file + '\n')


# COMMAND ----------

# MAGIC %md # Prep path & Date parameter

# COMMAND ----------

##-----------------------------------------------------------
## Prepare path & date
##-----------------------------------------------------------

## ----------------------------------------------
## setup file path Spark API
## ----------------------------------------------

mpath      = 'dbfs:/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/'
stdin_path = mpath + '00_std_inputs/'
eval_path  = mpath + '04_O2O/'

## input path for evaluaton
incmp_path   = eval_path + '00_cmp_inputs/'
s_in_path    = incmp_path + 'input_files/'

## projet path

pprj_path  = sprj_path.replace('dbfs:/', '/dbfs/')
p_in_path  = s_in_path.replace('dbfs:/', '/dbfs/')

print('File API project path = ' + pprj_path)

out_path   = pprj_path + 'output/'
out_path_sk= sprj_path + 'output/'

## standard input path


##Fixed campaign start - end date first

# cmp_st_dt  = '2022-11-03'
# cmp_en_dt  = '2022-11-23'

## convert input string to date from input info

st_date    = datetime.strptime(cmp_st_dt, '%Y-%m-%d')
en_date    = datetime.strptime(cmp_en_dt, '%Y-%m-%d')

diff_delta = en_date - st_date

diff_dt    = diff_delta.days
#print(diff_dt)

##  campaign weeks
wk_cmp     = int(np.round(diff_dt/7,0) )
print('Number of campaign week = ' + str(wk_cmp))

if wk_type == 'promo_wk':
  ## get week of campaign - promo_wk
  cmp_st_promo_wk   = wk_of_year_promo_ls(cmp_st_dt)
  cmp_en_promo_wk   = wk_of_year_promo_ls(cmp_en_dt)
elif wk_type == 'fis_wk':
  ## get week of campaign - fis_week
  cmp_st_wk   = wk_of_year_ls(cmp_st_dt)
  cmp_en_wk   = wk_of_year_ls(cmp_en_dt)
## end if
  
## Pre-period, using 13 weeks pre
pre_en_date = (datetime.strptime(cmp_st_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
pre_en_wk   = wk_of_year_ls(pre_en_date)

pre_st_wk   = week_cal(pre_en_wk, (wk_cmp-1) * -1)                      
pre_st_date = f_date_of_wk(pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data

## promo week
pre_en_promo_wk    = wk_of_year_promo_ls(pre_en_date)
pre_st_promo_wk    = promo_week_cal(pre_en_promo_wk, (wk_cmp-1) * -1)
pre_st_date_promo = f_date_of_promo_wk(pre_st_promo_wk).strftime('%Y-%m-%d')

## for customer movement usign 13 + 13 weeks -- Pat added 5 Jul 2022

pre_en_promo_mv_wk = pre_en_promo_wk
pre_st_promo_mv_wk = promo_week_cal(pre_en_promo_wk, -12)   ## pre period = 13 weeks = pre_en_promo_wk

pre_st_date_promo_mv = f_date_of_promo_wk(pre_st_promo_mv_wk).strftime('%Y-%m-%d')

## fis_wk
ppp_en_wk       = week_cal(pre_st_wk, -1)
ppp_st_wk       = week_cal(ppp_en_wk, -12)
##promo week
ppp_en_promo_wk = promo_week_cal(pre_st_promo_wk, -1)
ppp_st_promo_wk = promo_week_cal(ppp_en_promo_wk, -12)

ppp_st_date = f_date_of_wk(ppp_en_wk).strftime('%Y-%m-%d')
ppp_en_date = f_date_of_wk(ppp_st_wk).strftime('%Y-%m-%d')

print('\n' + '-'*80 + '\n Date parameter for campaign ' + str(c_nm) + ' shown below \n' + '-'*80 )
print('Campaign period between ' + str(cmp_st_dt) + ' and ' + str(cmp_en_dt) + '\n')
print('Campaign prior period (13+13 weeks) between date ' + str(ppp_st_date) + ' and week ' + str(ppp_en_date) + ' \n')

if wk_type == 'promo_wk' : 
  print('Campaign is during Promo week ' + str(cmp_st_promo_wk) + ' to ' + str(cmp_en_promo_wk) + '\n')
  print('Campaign pre-period (13 weeks) between promo week ' + str(pre_st_promo_wk) + ' and week ' + str(pre_en_promo_wk) + ' \n')
  print('Campaign prior period (13+13 weeks) between promo week ' + str(ppp_st_promo_wk) + ' and week ' + str(ppp_en_promo_wk) + ' \n')
elif wk_type == 'fis_wk':
  print('Campaign is during Fis week ' + str(cmp_st_wk) + ' to ' + str(cmp_en_wk) + '\n')
  print('Campaign pre-period (13 weeks) between week ' + str(pre_st_wk) + ' and week ' + str(pre_en_wk) + ' \n')
  print('Campaign pre-period (13 weeks) between date ' + str(pre_st_date) + ' and week ' + str(pre_en_date) + ' \n')
  print('Campaign prior period (13+13 weeks) between week ' + str(ppp_st_wk) + ' and week ' + str(ppp_en_wk) + ' \n')
## end if     


# COMMAND ----------

# MAGIC %md #Check & Prep transaction table

# COMMAND ----------

# MAGIC %md
# MAGIC ##--------------------------------------------------------------------
# MAGIC ## main transaction table from campaign eval
# MAGIC ## tdm_dev.media_campaign_eval_txn_data_2022_0744_m01c
# MAGIC ##--------------------------------------------------------------------
# MAGIC use_txn_ofn_flag = float(use_txn_ofn)
# MAGIC
# MAGIC if use_txn_ofn_flag == 1 :
# MAGIC   
# MAGIC   txn_tab    = 'tdm_dev.media_campaign_eval_txn_data_' + c_id_ofn.lower()
# MAGIC   print(' Default campaign traction table name from instore = ' + txn_tab + '\n')
# MAGIC   
# MAGIC   ## Check if exist to use else need to create it for o2o
# MAGIC   
# MAGIC   try :
# MAGIC     ctxn       = spark.table(txn_tab)
# MAGIC     ctxn       = ctxn.withColumn('hh_flag',F.when(ctxn.household_id.isNull(), lit(0))
# MAGIC                                             .otherwise(lit(1)))  
# MAGIC     print(' Default campaign transaction table exist.  Process will use it as initial table \n')
# MAGIC     
# MAGIC   except:
# MAGIC     #print('test')
# MAGIC     txn_tab    = 'tdm_dev.media_campaign_eval_txn_data_o2o_' + c_id.lower()
# MAGIC     
# MAGIC     try :
# MAGIC       ctxn     = spark.table(txn_tab)
# MAGIC       print(' Default campaign transaction table from O2O exist process will use data from : ' + txn_tab + ' \n')
# MAGIC     except:
# MAGIC       print(' Default campaign transaction table from instore and O2O does not exist!! \n')
# MAGIC       print(' Process will create o2o transaction table ' + txn_tab + '\n')
# MAGIC       ## call function to create table
# MAGIC       if wk_type == 'fis_wk':
# MAGIC         txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_wk, end_week_id=cmp_en_wk, store_format=[1,2,3,4,5], 
# MAGIC                                       prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
# MAGIC                                                          'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
# MAGIC                                                          'department_code', 'section_code', 'class_code', 'subclass_code'])
# MAGIC       elif wk_type == 'promo_wk':
# MAGIC         txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_promo_wk, end_week_id=cmp_en_promo_wk, store_format=[1,2,3,4,5], 
# MAGIC                                       prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
# MAGIC                                                          'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
# MAGIC                                                          'department_code', 'section_code', 'class_code', 'subclass_code'])
# MAGIC       ## end if
# MAGIC       ## additional column based on household_id not cc_flag
# MAGIC       txn_all     = txn_all.withColumn('hh_flag',F.when(txn_all.household_id.isNull(), lit(0))
# MAGIC                                                   .otherwise(lit(1)))
# MAGIC       ## save table
# MAGIC       txn_all.write.mode('overwrite').partitionBy('hh_flag').saveAsTable(txn_tab)
# MAGIC       ## Pat add, delete dataframe before re-read
# MAGIC       del txn_all
# MAGIC       ## Re-read from table
# MAGIC       ctxn = spark.table(txn_tab)
# MAGIC       
# MAGIC else: ## use O2O trans as initial
# MAGIC   
# MAGIC   txn_tab    = 'tdm_dev.media_campaign_eval_txn_data_o2o_' + c_id.lower()
# MAGIC     
# MAGIC   try :
# MAGIC     ctxn     = spark.table(txn_tab)
# MAGIC     print(' Default campaign transaction table from O2O exist process will use data from : ' + txn_tab + ' \n')
# MAGIC   except:
# MAGIC     print(' Default campaign transaction table from instore and O2O does not exist!! \n')
# MAGIC     print(' Process will create o2o transaction table ' + txn_tab + '\n')
# MAGIC     ## call function to create table
# MAGIC     if wk_type == 'fis_wk':
# MAGIC       txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_wk, end_week_id=cmp_en_wk, store_format=[1,2,3,4,5], 
# MAGIC                                     prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
# MAGIC                                                        'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
# MAGIC                                                        'department_code', 'section_code', 'class_code', 'subclass_code'])
# MAGIC     elif wk_type == 'promo_wk':
# MAGIC       txn_all  = get_trans_itm_wkly(start_week_id=ppp_st_promo_wk, end_week_id=cmp_en_promo_wk, store_format=[1,2,3,4,5], 
# MAGIC                                     prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
# MAGIC                                                        'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
# MAGIC                                                        'department_code', 'section_code', 'class_code', 'subclass_code'])
# MAGIC     ## end if
# MAGIC     ## additional column based on household_id not cc_flag
# MAGIC     txn_all    = txn_all.withColumn('hh_flag',F.when(txn_all.household_id.isNull(), lit(0))
# MAGIC                                                 .otherwise(lit(1)))
# MAGIC
# MAGIC     ## save table
# MAGIC     txn_all.write.mode('overwrite').partitionBy('hh_flag').saveAsTable(txn_tab)
# MAGIC     ## Pat add, delete dataframe before re-read
# MAGIC     del txn_all
# MAGIC     ## Re-read from table
# MAGIC     ctxn = spark.table(txn_tab)
# MAGIC ## end if

# COMMAND ----------

# MAGIC %md # Read input

# COMMAND ----------

# # Test search text

# txt    = '2022_0744_M01C_target_ca_3-Nov-Downy NPD Repertore.csv'
# ln     = len(txt)
# kywd   = 'target_ca_'
# idx    = txt.find('target_ca_')

# st_idx = idx + len(kywd)

# print('ln = ' + str(ln) + ' idx = ' + str(st) + ' st_idx = ' + str(st_idx))

# get_wd = txt[st_idx: -4] ## remove .csv

# use_wd = ''.join(t for t in get_wd if(t.isalnum() or t == '_'))


# print('get_wd = ' + get_wd)
# print('use_wd = ' + use_wd)

# COMMAND ----------

##---------------------------------------------------------
## Add step split file name to list for loop 
##---------------------------------------------------------

##---------------------
## Target CA
##---------------------
## ----------
## Version - get from table consolidate - CA
## ----------

## Get CA Then get group name
##==================
## For test
#c_id_oln = '2023_0355'
#c_id_oln = '2023_0355_0323'
##==================

## convert to number
use_ca_tab_flg = float(use_ca_tab)

if use_ca_tab_flg == 1 : 

    mca     = spark.table('tdm_dev.consolidate_ca_master').where(F.col('cmp_id') == c_id_oln)
    ca_grp  = spark.table('tdm_dev.consolidate_ca_group').where(F.col('cmp_id') == c_id_oln)
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
    
    file_nm   = c_nm + '_target_CA_X_group_duplicate.csv'
    full_nm   = out_path + file_nm
    
    
    dup_trg_pd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')
    
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
    
    files      = dbutils.fs.ls(s_in_path + 'target_ca/')  ## get path in spartk API
    print(' Look for file in path ' + s_in_path + 'target_ca/' + ' \n')
    
    ## setup file filter
    txt_begin  = c_id_oln + '_target_ca_'
    print(' Filter "Targeting CA file " only file begin with : ' + txt_begin + '\n')
    
    files_list = [f.path for f in files]
    #print(files_list)
    use_file   = [uf for uf in files_list if txt_begin in uf ]
    #print(use_file)
    
    nfiles     = len(use_file)
    
    if nfiles == 0 :
      print(' No "Target CA" file for campaign ' + c_nm + '!! Code will skip.')
      raise Exception('No "Target CA" file for campaign ' + c_nm + '!! Code will skip.')
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
    
    file_nm   = c_nm + '_target_CA_X_group_duplicate.csv'
    full_nm   = out_path + file_nm
    
    # Use function to create new folder & save csv
    # dup_trg_pd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')
    pandas_to_csv_filestore(dup_trg_pd, csv_file_name=file_nm, prefix=out_path, index = False, header = True, encoding = 'utf8')

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
    files      = dbutils.fs.ls(s_in_path + 'control_ca/')  ## get path in spartk API
    
    print(' Look for file in path ' + s_in_path + 'control_ca/' + ' \n')
    
    ## setup file filter
    txt_begin  = c_id_oln + '_control_ca_'
    print(' Filter "Control CA file " only file begin with : ' + txt_begin + '\n')
    
    files_list = [f.path for f in files]
    use_file   = [uf for uf in files_list if txt_begin in uf ]
    #print(use_file)
    
    nfiles     = len(use_file)
    
    if nfiles == 0 :
      print(' No "Control CA" file for campaign ' + c_nm + '!! Code will skip.')
      raise Exception('No "Control CA" file for campaign ' + c_nm + '!! Code will skip.')
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

## End if check Use CA Table


# COMMAND ----------


# ##----------------------------------------------------------------------------
# ## Version - get from file  -- back up 12 Jul 2023
# ##----------------------------------------------------------------------------
# # files      = dbutils.fs.ls(s_in_path + 'target_ca/')  ## get path in spartk API
# # print(' Look for file in path ' + s_in_path + 'target_ca/' + ' \n')

# # ## setup file filter
# # txt_begin  = c_id_oln + '_target_ca_'
# # print(' Filter "Targeting CA file " only file begin with : ' + txt_begin + '\n')

# # files_list = [f.path for f in files]
# # #print(files_list)
# # use_file   = [uf for uf in files_list if txt_begin in uf ]
# # #print(use_file)

# # nfiles     = len(use_file)

# # if nfiles == 0 :
# #   print(' No "Target CA" file for campaign ' + c_nm + '!! Code will skip.')
# #   raise Exception('No "Target CA" file for campaign ' + c_nm + '!! Code will skip.')
# # ## end if

# # print(' Number of Target CA file = ' + str(nfiles) + ' files.')

# # trg_df     = spark.createDataFrame([], StructType())

# # ## read & combine data
# # for cfile in use_file: 
  
# # #   print(cfile)  
  
# # #   ## add step get CA group name from file name
  
# #   f_ln   = len(cfile)
# #   kywd   = '_target_ca_'
# #   idx    = cfile.find(kywd)
# #   st_idx = idx + len(kywd)
# #   get_wd = cfile[st_idx : -4 ] ## remove .csv
# #   grp_nm = ''.join(t for t in get_wd if(t.isalnum() or t == '_'))
  
# #   ##------------------------------------------
  
# #   fr_df  = spark.read.csv(cfile, header=True, inferSchema=True)
# #   fr_dfe = fr_df.select    ( fr_df.golden_record_external_id_hash.alias('crm_id')
# #                             ,lit('trg').alias('c_type')
# #                             ,lit(grp_nm).alias('ca_group')
# #                            )
  
# #   trg_df = trg_df.unionByName(fr_dfe, allowMissingColumns=True)

# # ## end for
# ##----------------------------------------------------------------------------

# # ## drop duplicates (within group)
# # trg_df   = trg_df.dropDuplicates()

# ## check if customer duplicate between group

# cnt_df   = trg_df.groupBy(trg_df.crm_id)\
#                  .agg    (count(trg_df.ca_group).alias('cnt_grp'))

# ## get hh_id which has duplicate between group
# dup_df   = cnt_df.where(cnt_df.cnt_grp > 1)\
#                  .select(cnt_df.crm_id)

## join back to get customer group and export for checking later

# dup_trg    = trg_df.join(dup_df, 'crm_id', 'left_semi')

# dup_trg_pd = dup_trg.toPandas()

# ## Export product info until brand level to file

# file_nm   = c_nm + '_target_CA_X_group_duplicate.csv'
# full_nm   = out_path + file_nm

# dup_trg_pd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')

# ## get number of duplicate customer to show
# cc_dup = dup_df.agg (sum(lit(1)).alias('hh_cnt')).collect()[0].hh_cnt 

# if cc_dup is None :
#   cc_dup = 0
# ## end if

# print('='*80)
# print('\n Number of target CA duplicate between group = ' + str(cc_dup) + ' household_id. \n ')
# print('='*80)

# print('Export Duplicate Target CA to : ' + str(full_nm))

# ## -----------------------------------------------------
# ## 7 Apr 2023  - Add remove customers which has duplicated across CA group from analysis first. -- Pat
# ## -----------------------------------------------------

# trg_df = trg_df.join(dup_df, 'crm_id', 'left_anti')  ## not included duplicates customers

# #trg_df.display()
# ##  

# ##---------------------
# ## Control CA
# ##---------------------
# files      = dbutils.fs.ls(s_in_path + 'control_ca/')  ## get path in spartk API

# print(' Look for file in path ' + s_in_path + 'control_ca/' + ' \n')

# ## setup file filter
# txt_begin  = c_id_oln + '_control_ca_'
# print(' Filter "Control CA file " only file begin with : ' + txt_begin + '\n')

# files_list = [f.path for f in files]
# use_file   = [uf for uf in files_list if txt_begin in uf ]
# #print(use_file)

# nfiles     = len(use_file)

# if nfiles == 0 :
#   print(' No "Control CA" file for campaign ' + c_nm + '!! Code will skip.')
#   raise Exception('No "Control CA" file for campaign ' + c_nm + '!! Code will skip.')
# ## end if

# print(' Number of Control CA file = ' + str(nfiles) + ' files.')

# ctl_df     = spark.createDataFrame([], StructType())

# ## read & combine data
# for cfile in use_file: 
  
#   print(cfile)  
  
#   fr_df  = spark.read.csv(cfile, header=True, inferSchema=True)
#   fr_dfe = fr_df.select    ( fr_df.golden_record_external_id_hash.alias('crm_id')
#                             ,lit('ctl').alias('c_type')
#                            )
  
#   ctl_df = ctl_df.unionByName(fr_dfe, allowMissingColumns=True)

# ## end for
# ##-----------------------------------
# ## drop duplicate
# ##-----------------------------------

# ## As of 5 Apr 2023 : Pat still not separated CA for each target CA group

# ctl_df = ctl_df.dropDuplicates()

# ## Add step to remove control CA which duplicate with Target CA out from group -- 25 May 2023 (Pat)

# ctl_df = ctl_df.join(trg_df, [ctl_df.crm_id == trg_df.crm_id], 'left_anti')

# #ctl_df.display()

##---------------------
## Conversion CA
##---------------------
files      = dbutils.fs.ls(s_in_path + 'offline_cvn/')  ## get path in spartk API

print(' Look for file in path ' + s_in_path + 'offline_cvn/' + ' \n')

## setup file filter
txt_begin  = c_id_oln + '_offline_cvn_'

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

#cvn_df.display()


# COMMAND ----------

# ## Target customer
# infile1 = in_path + 'mamy_ca1.csv'
# ca1 = pd.read_csv(infile1)

# infile2 = in_path + 'mamy_ca2.csv'
# ca2 = pd.read_csv(infile2)

# infile3 = in_path + 'mamy_ca3.csv'
# ca3 = pd.read_csv(infile3)

# ## Control customer

# infile1 = in_path + 'mamy_ctl1.csv'
# ctl1 = pd.read_csv(infile1)

# infile2 = in_path + 'mamy_ctl2.csv'
# ctl2 = pd.read_csv(infile2)

# infile3 = in_path + 'mamy_ctl3.csv'
# ctl3 = pd.read_csv(infile3)

## Read offline conversion

# infile1 = in_path + 'sku_actv.csv'
# sku_atv = pd.read_csv(infile1)

# infile2 = in_path + 'brand_actv.csv'
# bnd_atv = pd.read_csv(infile2)

##-----------------------------------
## Input SKU
##-----------------------------------
infile1   = p_in_path + 'upc_list_' + c_id_ofn + '.csv'
feat_pd   = pd.read_csv(infile1)

feat_list = feat_pd['feature'].astype(int).to_list()

print(feat_list)

## store universe (if any)

##-----------------------------------
## target store
##-----------------------------------
infile1   = p_in_path + 'target_store_' + c_id_ofn + '.csv'
trg_pd    = pd.read_csv(infile1)

##-----------------------------------
## Brand file (if any)
##-----------------------------------
## check if cmp_file exists

infile1    = p_in_path + 'upc_list_brand_' + c_id_ofn + '.csv'
infile_spk = s_in_path + 'upc_list_brand_' + c_id_ofn + '.csv'

try:
    dbutils.fs.ls(infile_spk)
    print('-' * 80 + '\n' + ' Campaign have self Brand definition file ' + infile1 + ' will use this file for evaluate. \n' + '-' * 80)
    brand_pd   = pd.read_csv(infile1)    
except:
    print('-'*80 + '\n' + ' Campaign do not have self brand definition file.  Will use default brand definition \n' + '-' * 80)
    ## create empty dataframe brand_pd
    brand_pd   = pd.DataFrame()
## end try

##-----------------------------------
## Adjacency file (if need)
##-----------------------------------

if float(exp_sto_flg) == 1 :
  #print(exp_sto_flg)
  print(' Exposure capture at store level, no Aisle defintion need. \n')  
else : 
  ## Not store level Need to load adjacency file
  print(' Exposure is not at store level need to load Aisle file for customers scope. \n')
  in_ai_file  = stdin_path + aisle_file
  std_ai_df   = spark.read.csv(in_ai_file, header="true", inferSchema="true")
  
## end if


# COMMAND ----------

# MAGIC %md # Prep product info

# COMMAND ----------

## use function to get list of feature_sku, brand, category and etc.

# ## Test value 

# x_cate_flag = '1'
# x_cate_cd   = '13_6_12_5_2'
##------------------------------

# function get product info

## store level no aisle need


if float(exp_sto_flg) == 1 :
  all_prod_df, feat_df, brand_df, class_df, sclass_df, cate_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list = _get_prod_df_no_aisle( feat_list
                                                                                                                                                                                                                                              ,cate_lvl
                                                                                                                                                                                                                                              ,x_cate_flag
                                                                                                                                                                                                                                              ,x_cate_cd)
  
  ## prep feature product info
  
  prod_info = feat_df.select( lit('store_level').alias('ai_group_list')
                             ,feat_df.div_nm.alias('division_name')
                             ,feat_df.dept_nm.alias('department_name'))\
                       .dropDuplicates()
else:
  feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list, use_ai_group_list, use_ai_sec_list = _get_prod_df( feat_list
                                                                                                                                                                                                                                              ,cate_lvl
                                                                                                                                                                                                                                              ,std_ai_df
                                                                                                                                                                                                                                              ,x_cate_flag
                                                                                                                                                                                                                                              ,x_cate_cd)
  
  prod_info = feat_df.select( lit(str(use_ai_group_list)).alias('ai_group_list')
                             ,feat_df.div_nm.alias('division_name')
                             ,feat_df.dept_nm.alias('department_name'))\
                     .dropDuplicates()
  
## end if

prod_info = prod_info.withColumn('section_name' , lit(str(sec_nm_list)))\
                     .withColumn('class_name'   , lit(str(class_nm_list)))\
                     .withColumn('subclass_name', lit(str(sclass_nm_list)))\
                     .withColumn('brand_name'   , lit(str(brand_list)))\
                     .withColumn('manufactor'   , lit(str(mfr_nm_list)))

prod_ipd  = prod_info.toPandas()

## Export product info until brand level to file

file_nm   = c_nm + '_product_brand_info.csv'
full_nm   = out_path + file_nm

# prod_ipd.to_csv(full_nm, index = False, header = True, encoding = 'utf8')
pandas_to_csv_filestore(prod_ipd, csv_file_name=file_nm, prefix=out_path, index = False, header = True, encoding = 'utf8')


# COMMAND ----------

# MAGIC %md # Merge online target/control customers

# COMMAND ----------

#feat_df.display()

# COMMAND ----------

#ctl_df.count()

# COMMAND ----------

# ## target customer
# ca     = pd.concat([ca1, ca2, ca3])
# ## control customer
# ctl    = pd.concat([ctl1, ctl2, ctl3])

# ## drop duplicates (if any)

# ca_c   = ca[['golden_record_external_id_hash']].drop_duplicates()

# ca_c.rename(columns={'golden_record_external_id_hash' :'crm_id'},inplace=True)
# ca_c['c_type']  = 'trg'

# ctl_c  = ctl[['golden_record_external_id_hash']].drop_duplicates()
# ctl_c.rename(columns={'golden_record_external_id_hash' :'crm_id'},inplace=True)

# ctl_c['c_type'] = 'ctl'

# ## merge target and control to join cust_dim only 1 time

# ca_all = pd.concat([ca_c, ctl_c])

# ## brand activated customers
# bnd_atv_c = bnd_atv[['golden_record_external_id_hash']].drop_duplicates()
# sku_atv_c = sku_atv[['golden_record_external_id_hash']].drop_duplicates()

## Get Target store code only

trg_str   = trg_pd[['store_id']].drop_duplicates()

## convert list of CA to spark

ca_df      = trg_df.unionByName(ctl_df, allowMissingColumns=True)

#bnd_atv_df = spark.createDataFrame(bnd_atv_c)
#sku_atv_df = spark.createDataFrame(sku_atv_c)

trg_str_df = spark.createDataFrame(trg_str)


#print( '#record ca = ' + str(len(ca)) + ' after drop duplicate = ' + str(len(ca_c)))
#print( '#record control ca = ' + str(len(ctl)) + ' after drop duplicate = ' + str(len(ctl_c)))

# COMMAND ----------

# MAGIC %md ## Split DGS by mech_name

# COMMAND ----------

ctxn = spark.table("tdm_dev.media_campaign_eval_txn_data_o2o_dev_2023_0150_m03e_2023_0150_0518")
target_store_sf = spark.read.csv(s_in_path + 'target_store_' + c_id_ofn + '.csv', header=True, inferSchema=True)

txn_cmp_hde_offline = (ctxn.where(F.col("household_id").isNotNull())
                           .where(F.col("date_id").between(cmp_st_dt, cmp_en_dt))
                           .where(F.col("store_format_group") == "HDE")
                           .where(F.col("offline_online_other_channel") == 'OFFLINE')) 

# COMMAND ----------

dgs_target_store_sf = target_store_sf.where( F.trim(F.lower(F.col("mech_name"))).isin(["dgs"]) ).select("store_id").drop_duplicates()
is_target_store_sf =  target_store_sf.join(dgs_target_store_sf, "store_id", "leftanti").select("store_id").drop_duplicates()
rest_hde_target_store_sf = spark.table("tdm.v_store_dim_c").where(F.col("format_id").isin([1,2,3])).join(target_store_sf, "store_id", "left_anti").select("store_id").drop_duplicates()

is_cust = (txn_cmp_hde_offline
           .join(is_target_store_sf, "store_id", "left_semi")
           .join(use_ai_df, "upc_id", "left_semi")
           .select("household_id")
           .drop_duplicates()
)

dgs_cust = (txn_cmp_hde_offline
           .join(dgs_target_store_sf, "store_id", "left_semi")
           .select("household_id")           
           ).drop_duplicates()

is_dgs_gr = is_cust.join(dgs_cust, "household_id", "inner").drop_duplicates()
is_gr = is_cust.join(is_dgs_gr, "household_id", "left_semi").drop_duplicates()
dgs_gr = dgs_cust.join(is_dgs_gr, "household_id", "left_semi").drop_duplicates()

rest_hde_txn = txn_cmp_hde_offline.join(is_target_store_sf, "store_id", "left_anti").join(dgs_target_store_sf, "store_id", "left_anti")

nis_ndgs_gr = (rest_hde_txn
               .join(use_ai_df, "upc_id", "left_semi")
               .select("household_id")
               .drop_duplicates()
               .join(is_dgs_gr, "household_id", "left_anti")
               .join(is_gr, "household_id", "left_anti")
               .join(dgs_gr,"household_id", "left_anti")
)

ndgs_gr = (rest_hde_txn
           .join(nis_ndgs_gr, "household_id", "left_anti")
           .select("household_id")
           .drop_duplicates()
           .join(is_dgs_gr, "household_id", "left_anti")
           .join(is_gr, "household_id", "left_anti")
           .join(dgs_gr,"household_id", "left_anti")
           .join(ndgs_gr,"household_id", "left_anti")
)
           

# COMMAND ----------

target_store.display()

# COMMAND ----------

# ca_df.limit(10).display()
#len(ca_c)

# COMMAND ----------

# %sql

# show create table tdm.v_transaction_item

# COMMAND ----------

# MAGIC %md #Get customer dim

# COMMAND ----------

cst_dim = spark.table('tdm.v_customer_dim')


# COMMAND ----------

## Join all CA to cust_dim to get internal customer_id and household_id

ca_df  = ca_df.join  (cst_dim, ca_df.crm_id == cst_dim.golden_record_external_id_hash, 'inner')\
              .select( cst_dim.household_id.alias('hh_id')
                      ,ca_df.crm_id
                      ,ca_df.c_type
                      ,ca_df.ca_group
                     )\
              .dropDuplicates()

## Get household_id for offline conversion

cvn_df = cvn_df.join  (cst_dim, cvn_df.crm_id == cst_dim.golden_record_external_id_hash, 'inner')\
               .select( cst_dim.household_id.alias('hh_id')
                       ,cvn_df.crm_id
                      )\
               .dropDuplicates()

#ca_df.limit(20).display()

# COMMAND ----------

# chk = ca_df.groupBy(ca_df.crm_id)\
#            .agg    (sum(lit(1)).alias('row_cnt'))\

# #chk.where(chk.row_cnt > 1).agg(sum(lit(1)).alias('crm_more_cid')).display()

# chk.where(chk.row_cnt > 1).limit(20).display()

#cst_dim 007d4b41d64821a99d1ed3976df3e767200847a0a59af4ec69895d914266b462



# COMMAND ----------

#cst_dim.where(cst_dim.golden_record_external_id_hash == '007d4b41d64821a99d1ed3976df3e767200847a0a59af4ec69895d914266b462').display()

# COMMAND ----------

# MAGIC %md # Get all offline customer with Brand

# COMMAND ----------

#ctxn.select(ctxn.store_format_group).dropDuplicates().display()
#ctxn.select(ctxn.offline_online_other_channel).dropDuplicates().display()

# COMMAND ----------

## join target store to get store_flag, all format, all channel

ctxnp          = ctxn.join  (trg_str_df, ctxn.store_id == trg_str_df.store_id, 'left')\
                     .select( ctxn['*']
                             ,F.when(trg_str_df.store_id.isNotNull(), lit(1))
                               .otherwise(lit(0))
                               .alias('trg_str_flag')
                             )
## get cc_txn during & pre period

cctxn_dur     = ctxnp.where((ctxnp.hh_flag == 1)  & (ctxn.date_id.between(cmp_st_dt, cmp_en_dt)))
#cctxn_pre     = ctxnp.where(ctxn.customer_id.isNotNull() & ctxn.date_id.between(pre_st_date_promo_mv, pre_en_date))



# COMMAND ----------

# cctxn_dur.groupBy(cctxn_dur.hh_flag)\
#          .agg(sum(lit(1)).alias('hh_cnt')).display()

#feat_df.display()


# COMMAND ----------

# MAGIC %md ## filter txn at cate, brand , feat level

# COMMAND ----------

#ctxn.select( ctxn.offline_online_other_channel).dropDuplicates().display()

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

if not brand_pd.empty:
  print(' Having self idenfify brand sku \n ')  
  brand_pd.rename(columns={'feature' :'upc_id'},inplace=True)  
  brand_mdf = spark.createDataFrame(brand_pd)
  brand_df  = brand_mdf
  
## end if

if float(exp_sto_flg) == 0 :
  ## get Aisle txn
  ai_txn_dur    = get_prod_txn(cctxn_dur, use_ai_df)
## end if

## Txn from any format and any channel

## get category 
cate_txn_dur  = get_prod_txn(cctxn_dur, cate_df)
#cate_txn_pre  = get_prod_txn(cctxn_pre, cate_df)

## get brand 
bnd_txn_dur   = get_prod_txn(cctxn_dur, brand_df)
#bnd_txn_pre   = get_prod_txn(cctxn_pre, brand_df)

## get feature sku 
sku_txn_dur   = get_prod_txn(cctxn_dur, feat_df)
#sku_txn_pre   = get_prod_txn(cctxn_pre, feat_df)


# COMMAND ----------

#bnd_txn_dur.limit(10).display()

# COMMAND ----------

# MAGIC %md ## Get customer at product level

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

#cctxn_dur.where(ctxn.household_id.isNull()).limit(10).display()

# COMMAND ----------

# MAGIC %md ### Separated Store Level exposure & Aisle exposure

# COMMAND ----------

##=======================================================================
## For Store Level Exposure media (not specific aisle)
##=======================================================================

## get df list of customer for each txn

if float(exp_sto_flg) == 1 :
  ## get target store customers
  
  trg_txnd      = cctxn_dur.where((cctxn_dur.trg_str_flag == 1) & (cctxn_dur.offline_online_other_channel == 'OFFLINE'))
  #oth_txnd        = cctxn_dur.where(cctxn_dur.trg_str_flag == 0) ## any store online & offline
  ## add other hde txn
  #hde_txnd        = cctxn_dur.where((cctxn_dur.store_format_group == 'HDE') & (cctxn_dur.offline_online_other_channel == 'OFFLINE')) ## hde store offline
  
  ## hde offline non target
  hdec_txnd     = cctxn_dur.where((cctxn_dur.store_format_group == 'HDE') & 
                                  (cctxn_dur.offline_online_other_channel == 'OFFLINE') & 
                                  (cctxn_dur.trg_str_flag == 0) &
                                  (cctxn_dur.store_id < 7000)  ## add filter out store 7xxx, 8xxx, 9xxx which might not have physical store -- Pat 29 Jun 2023
                                 ) 
 
  ## Get MyLo visits penetration (For all store level)
  trg_bsk_df    = ctxnp.where( (ctxnp.trg_str_flag == 1) & (ctxnp.date_id.between(cmp_st_dt, cmp_en_dt)) )\
                       .groupBy(ctxnp.hh_flag)\
                       .agg    (countDistinct(ctxnp.transaction_uid).alias('visits_cnt'))
  
##=======================================================================
## Else not store level exposure will use aisle definition 
##=======================================================================
else:  ## use_ai_df to flag only basket with product in Aisle
  
  trg_txnd      = cctxn_dur.where((cctxn_dur.trg_str_flag == 1) & (cctxn_dur.offline_online_other_channel == 'OFFLINE'))\
                           .join (use_ai_df, cctxn_dur.upc_id == use_ai_df.upc_id, 'left_semi')
  
  ## hde offline non target
  hdec_txnd     = cctxn_dur.where((cctxn_dur.store_format_group == 'HDE') & 
                                  (cctxn_dur.offline_online_other_channel == 'OFFLINE') & 
                                  (cctxn_dur.trg_str_flag == 0) &
                                  (cctxn_dur.store_id < 7000)  ## add filter out store 7xxx, 8xxx, 9xxx which might not have physical store -- Pat 29 Jun 2023
                                 )\
                             .join (use_ai_df, cctxn_dur.upc_id == use_ai_df.upc_id, 'left_semi')
  
  ## Get MyLo visits penetration (For Exposure at Aisle)
  trg_bsk_df    = ctxnp.where( (ctxnp.trg_str_flag == 1) & (ctxnp.date_id.between(cmp_st_dt, cmp_en_dt)) )\
                       .join (use_ai_df, ctxnp.upc_id == use_ai_df.upc_id, 'left_semi')\
                       .groupBy(ctxnp.hh_flag)\
                       .agg    (countDistinct(ctxnp.transaction_uid).alias('visits_cnt'))
  
## end if

## offline all format
 ## add filter out store 7xxx, 8xxx, 9xxx which might not have physical store -- Pat 29 Jun 2023

ofn_txnd        = cctxn_dur.where((cctxn_dur.offline_online_other_channel == 'OFFLINE') &
                                  (cctxn_dur.store_id < 7000 )
                                 )

## Get Visits penetration 

trg_bsk_pd      = trg_bsk_df.toPandas()
## mylo
trg_bsk_mo      = trg_bsk_pd[trg_bsk_pd['hh_flag'] == 1].iloc[0]['visits_cnt']
## non mylo
trg_bsk_nmo     = trg_bsk_pd[trg_bsk_pd['hh_flag'] == 0].iloc[0]['visits_cnt']

all_bsk_trg     = trg_bsk_mo + trg_bsk_nmo

cc_pen          = trg_bsk_mo/all_bsk_trg

print(' MyLo visits penetration in target store at media area = ' + str(cc_pen) + '\n')
  
## Get Customer during
trgstr_cst_d_df = get_cdf(trg_txnd, 'trg')
#othstr_cst_d_df = get_cdf(oth_txnd, 'oth')
allofn_cst_d_df = get_cdf(ofn_txnd, 'ofn')
#hdestr_cst_d_df = get_cdf(hde_txnd, 'hde')
hdectl_cst_d_df = get_cdf(hdec_txnd,'hdec')

#cate_cst_d_df   = get_cdf(cate_txn_dur, 'cate')
bnd_cst_d_df    = get_cdf(bnd_txn_dur, 'brand')
sku_cst_d_df    = get_cdf(sku_txn_dur, 'sku')


# COMMAND ----------

# MAGIC %md #Separate customer to group - All CA

# COMMAND ----------

#hdectl_cst_d_df.limit(20).display()

# COMMAND ----------

# ## ca_df = all online customers both control & target

# # ca_df.limit(20).display()

# ## brand level

# ### during period

# oln_cdf    = ca_df       .join(allofn_cst_d_df, ca_df.hh_id == allofn_cst_d_df.ofn_h_id, 'left_anti')
# ono_cdf    = ca_df       .join(allofn_cst_d_df, ca_df.hh_id == allofn_cst_d_df.ofn_h_id, 'left_semi')
# ofn_cdf    = allofn_cst_d_df.join(ca_df       , allofn_cst_d_df.ofn_h_id == ca_df.hh_id, 'left_anti')

# ## ----------------------------------------------------
# ## separate customer
# ## ----------------------------------------------------
# ##
# ### online only , check if control if yes, C3 (online only), else target T3

# oln_t3_cdf = oln_cdf.where(oln_cdf.c_type == 'trg').select(oln_cdf.hh_id.alias('t3_hid'))
# oln_c3_cdf = oln_cdf.where(oln_cdf.c_type == 'ctl').select(oln_cdf.hh_id.alias('c3_hid'))

# ### offline only, check if go to target store, if yes T1 else C1

# t1_cdf     = ofn_cdf.join(trgstr_cst_d_df , ofn_cdf.ofn_h_id == trgstr_cst_d_df.trg_h_id , 'left_semi').select(ofn_cdf.ofn_h_id.alias('t1_hid'))
# c1_cdf_p   = ofn_cdf.join(hdectl_cst_d_df , ofn_cdf.ofn_h_id == hdectl_cst_d_df.hdec_h_id, 'left_semi')
# ## excluded T1 from C1 in case customer go to both store type
# c1_cdf     = c1_cdf_p.join(trgstr_cst_d_df, c1_cdf_p.ofn_h_id == trgstr_cst_d_df.trg_h_id , 'left_anti').select(ofn_cdf.ofn_h_id.alias('c1_hid'))

# ### o2o, separatd, control ca & target ca
# #### > for control ca, if go to target store, C2 (o2o control), else c3 (online only)
# #### > for target ca, if go to target store, T2 (o2o target), else T3 (online only)

# ## separate control CA, target ca for online & offline

# ono_t_cdf  = ono_cdf.where(ono_cdf.c_type == 'trg')
# ono_c_cdf  = ono_cdf.where(ono_cdf.c_type == 'ctl') 

# ## get O2O customers
# ## target

# t2_cdf     = ono_t_cdf.join(trgstr_cst_d_df, ono_t_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_semi').select(ono_t_cdf.hh_id.alias('t2_hid'))
# ## target CA not go to Target store, can be T3 (online media only)
# ono_t3_cdf = ono_t_cdf.join(trgstr_cst_d_df, ono_t_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_anti').select(ono_t_cdf.hh_id.alias('t3_hid'))

# ## control
# ## control CA not go to Target and go to other hde
# ono_c2p    = ono_c_cdf.join(trgstr_cst_d_df, ono_c_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_anti')
# c2_cdf     = ono_c2p  .join(hdectl_cst_d_df, ono_c2p.hh_id == hdectl_cst_d_df.hdec_h_id , 'left_semi').select(ono_c2p.hh_id.alias('c2_hid'))

# ## control CA not go to target all can be C3 (control of online)
# ono_c3_cdf = ono_c2p.select(ono_c2p.hh_id.alias('c3_hid'))

# ## combine T3 and C3

# t3_cdf = oln_t3_cdf.union(ono_t3_cdf)
# c3_cdf = oln_c3_cdf.union(ono_c3_cdf)


# COMMAND ----------

# MAGIC %md ## save customer group to table

# COMMAND ----------

# ## write customer group to temp table and will be remove later after code finish

# tbl_nm = 'tdm_seg.amp_' + brand_name + 't1_cdf'

# t1_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# t1_cdf = spark.table(tbl_nm)

# tbl_nm = 'tdm_seg.amp_' + brand_name + 'c1_cdf'

# c1_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# c1_cdf = spark.table(tbl_nm)

# tbl_nm = 'tdm_seg.amp_' + brand_name + 'c2_cdf'

# c2_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# c2_cdf = spark.table(tbl_nm)

# tbl_nm = 'tdm_seg.amp_' + brand_name + 't2_cdf'

# t2_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# t2_cdf = spark.table(tbl_nm)

# tbl_nm = 'tdm_seg.amp_' + brand_name + 't3_cdf'

# t3_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# t3_cdf = spark.table(tbl_nm)

# tbl_nm = 'tdm_seg.amp_' + brand_name + 'c3_cdf'

# c3_cdf.write.mode("overwrite").saveAsTable(tbl_nm)

# c3_cdf = spark.table(tbl_nm)

# COMMAND ----------

# ## Get number of customer in each group for information

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

# MAGIC %md # Get customer KPI for each group

# COMMAND ----------

# MAGIC %md ## Get Number of each group activation at brand and SKU level ?
# MAGIC
# MAGIC

# COMMAND ----------

#bnd_txn_dur.printSchema()

# COMMAND ----------

# ##----------------------------------------------------
# ## brand level
# ##----------------------------------------------------

# ## Offline only
# t1_batv_txn = bnd_txn_dur.join(t1_cdf, bnd_txn_dur.household_id == t1_cdf.t1_hid, 'left_semi')
# c1_batv_txn = bnd_txn_dur.join(c1_cdf, bnd_txn_dur.household_id == c1_cdf.c1_hid, 'left_semi')
  
# ## online 2 offline
# t2_batv_txn = bnd_txn_dur.join(t2_cdf, bnd_txn_dur.household_id == t2_cdf.t2_hid, 'left_semi')
# c2_batv_txn = bnd_txn_dur.join(c2_cdf, bnd_txn_dur.household_id == c2_cdf.c2_hid, 'left_semi')

# ## online only
# t3_batv_txn = bnd_txn_dur.join(t3_cdf, bnd_txn_dur.household_id == t3_cdf.t3_hid, 'left_semi')
# c3_batv_txn = bnd_txn_dur.join(c3_cdf, bnd_txn_dur.household_id == c3_cdf.c3_hid, 'left_semi')


# COMMAND ----------

# ##----------------------------------------------------
# ## sku level
# ##----------------------------------------------------

# ## Offline only
# t1_satv_txn = sku_txn_dur.join(t1_cdf, sku_txn_dur.household_id == t1_cdf.t1_hid, 'left_semi')
# c1_satv_txn = sku_txn_dur.join(c1_cdf, sku_txn_dur.household_id == c1_cdf.c1_hid, 'left_semi')
  
# ## online 2 offline
# t2_satv_txn = sku_txn_dur.join(t2_cdf, sku_txn_dur.household_id == t2_cdf.t2_hid, 'left_semi')
# c2_satv_txn = sku_txn_dur.join(c2_cdf, sku_txn_dur.household_id == c2_cdf.c2_hid, 'left_semi')

# ## online only
# t3_satv_txn = sku_txn_dur.join(t3_cdf, sku_txn_dur.household_id == t3_cdf.t3_hid, 'left_semi')
# c3_satv_txn = sku_txn_dur.join(c3_cdf, sku_txn_dur.household_id == c3_cdf.c3_hid, 'left_semi')


# COMMAND ----------

# def get_cust_kpi(intxn, cst_base, cst_grp):
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


# COMMAND ----------

# ##----------------------------------------------------
# ## brand level
# ##----------------------------------------------------

# ## Offline only
# t1_kpi = get_cust_kpi(t1_batv_txn, t1_ccnt,'T1')
# c1_kpi = get_cust_kpi(c1_batv_txn, c1_ccnt,'C1')

# ## ONO group
# t2_kpi = get_cust_kpi(t2_batv_txn, t2_ccnt,'T2')
# c2_kpi = get_cust_kpi(c2_batv_txn, c2_ccnt,'C2')

# ## Online only
# t3_kpi = get_cust_kpi(t3_batv_txn, t3_ccnt,'T3')
# c3_kpi = get_cust_kpi(c3_batv_txn, c3_ccnt,'C3')

# COMMAND ----------

# MAGIC %md ### Brand Level

# COMMAND ----------

# ## offline only

# t1_kpi.display()
# c1_kpi.display()

# COMMAND ----------

# ## O2O

# t2_kpi.display()
# c2_kpi.display()

# COMMAND ----------

# ## Online
# t3_kpi.display()
# c3_kpi.display()

# COMMAND ----------

# ##----------------------------------------------------
# ## SKU level
# ##----------------------------------------------------

# ## Offline only
# t1_kpi_sku = get_cust_kpi(t1_satv_txn, t1_ccnt,'T1')
# c1_kpi_sku = get_cust_kpi(c1_satv_txn, c1_ccnt,'C1')

# ## ONO group
# t2_kpi_sku = get_cust_kpi(t2_satv_txn, t2_ccnt,'T2')
# c2_kpi_sku = get_cust_kpi(c2_satv_txn, c2_ccnt,'C2')

# ## Online only
# t3_kpi_sku = get_cust_kpi(t3_satv_txn, t3_ccnt,'T3')
# c3_kpi_sku = get_cust_kpi(c3_satv_txn, c3_ccnt,'C3')


# COMMAND ----------

# MAGIC %md ### SKU Level

# COMMAND ----------

# ## offline only

# t1_kpi_sku.display()
# c1_kpi_sku.display()


# COMMAND ----------

# ## O2O

# t2_kpi_sku.display()
# c2_kpi_sku.display()

# COMMAND ----------

# ## online

# t3_kpi.display()
# c3_kpi.display()

# COMMAND ----------

# MAGIC %md # Select Random customer to Reach Number

# COMMAND ----------

# MAGIC %md ## Prep input number

# COMMAND ----------

## get target CA
ca_df_trg   = ca_df.where(ca_df.c_type == 'trg')
ca_df_ctl   = ca_df.where(ca_df.c_type == 'ctl')

#match_rate  = 0.7  ## use value from passing parameter if need
#ca_trg_cnt  = len(ca_c)
#ca_trg_70   = ca_trg_cnt * match_rate

## Change to use real reach number from Online team >> reach_online

reach_use    = float(reach_online)

## Brand offline conversion during
bnd_atv_dur = ca_df_trg.join(bnd_cst_d_df, ca_df_trg.hh_id == bnd_cst_d_df.brand_h_id, 'left_semi')
oth_ca      = ca_df_trg.join(bnd_cst_d_df, ca_df_trg.hh_id == bnd_cst_d_df.brand_h_id, 'left_anti')

## Brand offline conversion during
bnd_atv_cnt = bnd_atv_dur.agg(sum(lit(1)).alias('cst_cnt')).collect()[0].cst_cnt

## Offline conversion from n'Nhung  -- Pat add 9 Apr 2023
cvn_cnt     = cvn_df.agg(sum(lit(1)).alias('cst_cnt')).collect()[0].cst_cnt

## if #customers from conversion file is more then use customer from conversion file as initial -- Pat add 9 Apr 2023

if bnd_atv_cnt < cvn_cnt:
  
  bnd_atv_cnt = cvn_cnt
  bnd_atv_dur = ca_df_trg.join(cvn_df, ca_df_trg.hh_id == cvn_df.hh_id, 'left_semi')
  oth_ca      = ca_df_trg.join(cvn_df, ca_df_trg.hh_id == cvn_df.hh_id, 'left_anti')
# end if

## find number of customers to get random select

ca_random   = float(reach_use - bnd_atv_cnt)

## get percentage to put in select randome

ca_rnd_pct  = ca_random/reach_use

#seedn       = int(np.round(ca_rnd_pct * 100,0))

print(' Number of CA activated at brand level = ' + str(bnd_atv_cnt))
print(' Number of online Reach customer       = ' + str(reach_use) + ' custs')
print(' Total CA to random select             = ' + str(ca_random) + ' custs or equal to ' + str(ca_rnd_pct) + ' percent from reach \n')
#print(' Seed number to randome = ' + str(seedn))


# COMMAND ----------

# same_ca      = cvn_df.join(bnd_atv_dur, 'hh_id', 'left_semi')
# ca_notin_cvn = bnd_atv_dur.join(same_ca, 'hh_id', 'left_anti')

# #ca_notin_cvn.display()
# txn_ca_dif   = bnd_txn_dur.join(ca_notin_cvn, bnd_txn_dur.household_id == ca_notin_cvn.hh_id, 'inner')\
#                           .select( ca_notin_cvn.crm_id
#                                   ,bnd_txn_dur['*']
#                                  )

# txn_ca_dif.display()

# COMMAND ----------

# MAGIC %md ## select random customers

# COMMAND ----------

rand_ca = oth_ca.orderBy(rand()).limit(int(ca_random))

#rand_ca.count()
## union rand_ca with cust activated

ca_rdf = rand_ca.union(bnd_atv_dur)\
                .union(ca_df_ctl)\
                .distinct()

#ca_rdf.count()

# COMMAND ----------

#rand_ca.limit(10).display()

# COMMAND ----------

# MAGIC %md # Doing customer separate from random CA

# COMMAND ----------

## ca_df = all online customers both control & target

# ca_df.limit(20).display()

## brand level

### during period

oln_cdf    = ca_rdf         .join(allofn_cst_d_df, ca_rdf.hh_id == allofn_cst_d_df.ofn_h_id, 'left_anti') ## online only not go to offline (any store format)
ono_cdf    = ca_rdf         .join(allofn_cst_d_df, ca_rdf.hh_id == allofn_cst_d_df.ofn_h_id, 'left_semi') ## offline and online (Target + control)
ofn_cdf    = allofn_cst_d_df.join(ca_rdf       , allofn_cst_d_df.ofn_h_id == ca_rdf.hh_id, 'left_anti')  ## offline which not in CA (both target and control)

## ----------------------------------------------------
## separate customer
## ----------------------------------------------------
##
### online only , check if control if yes, C3 (online only), else target T3

oln_t3_cdf = oln_cdf.where(oln_cdf.c_type == 'trg').select(oln_cdf.hh_id.alias('t3_hid'),oln_cdf.ca_group)
oln_c3_cdf = oln_cdf.where(oln_cdf.c_type == 'ctl').select(oln_cdf.hh_id.alias('c3_hid'),lit('online').alias('ca_group'))

### offline only, check if go to target store, if yes T1 else C1

t1_cdf     = ofn_cdf.join(trgstr_cst_d_df , ofn_cdf.ofn_h_id == trgstr_cst_d_df.trg_h_id , 'left_semi')\
                    .select( ofn_cdf.ofn_h_id.alias('t1_hid')
                            ,lit('instore').alias('ca_group')
                           ) ##cut target which also CA

c1_cdf_p   = ofn_cdf.join(hdectl_cst_d_df , ofn_cdf.ofn_h_id == hdectl_cst_d_df.hdec_h_id, 'left_semi')  ## offline walk to control store
## excluded T1 from C1 in case customer go to both store type
c1_cdf     = c1_cdf_p.join(trgstr_cst_d_df, c1_cdf_p.ofn_h_id == trgstr_cst_d_df.trg_h_id , 'left_anti')\
                     .select( ofn_cdf.ofn_h_id.alias('c1_hid')
                             ,lit('instore').alias('ca_group')
                            ) ## not go to target

### o2o, separatd, control ca & target ca
#### > for control ca, if go to target store, C2 (o2o control), else c3 (online only)
#### > for target ca, if go to target store, T2 (o2o target), else T3 (online only)

## separate control CA, target ca for online & offline

ono_t_cdf  = ono_cdf.where(ono_cdf.c_type == 'trg')
ono_c_cdf  = ono_cdf.where(ono_cdf.c_type == 'ctl') 

## get O2O customers
## target

t2_cdf     = ono_t_cdf.join(trgstr_cst_d_df, ono_t_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_semi')\
                      .select( ono_t_cdf.hh_id.alias('t2_hid')
                              ,ono_t_cdf.ca_group
                             ) ## target ca and come to media store
## target CA not go to Target store, can be T3 (online media only)
ono_t3_cdf = ono_t_cdf.join(trgstr_cst_d_df, ono_t_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_anti')\
                      .select( ono_t_cdf.hh_id.alias('t3_hid')
                              ,ono_t_cdf.ca_group
                             )

## control
## control CA not go to Target and go to other hde
ono_c2p    = ono_c_cdf.join(trgstr_cst_d_df, ono_c_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_anti')  ## online control not go to target
c2_cdf     = ono_c2p  .join(hdectl_cst_d_df, ono_c2p.hh_id == hdectl_cst_d_df.hdec_h_id , 'left_semi')\
                      .select( ono_c2p.hh_id.alias('c2_hid')
                              ,lit('online').alias('ca_group')
                             ) ## online control go to offline control store

## control CA not go to target all can be C3 (control of online)
ono_c3_cdf = ono_c2p.select(ono_c2p.hh_id.alias('c3_hid'),lit('online').alias('ca_group'))

## combine T3 and C3

t3_cdf = oln_t3_cdf.union(ono_t3_cdf) 
c3_cdf = oln_c3_cdf.union(ono_c3_cdf) ## online only control + o2o but not to media store


# COMMAND ----------

## for checking number of customer in target store those are not in T1 or T2

chk_ca       = ono_c_cdf.join  (trgstr_cst_d_df, ono_c_cdf.hh_id == trgstr_cst_d_df.trg_h_id, 'left_semi')\
                        .select(ono_c_cdf.hh_id)\
                        .dropDuplicates()

ca_ctl_intrg = chk_ca.agg(sum(lit(1)).alias('cst_ca_cnt')).collect()[0].cst_ca_cnt

print(' Number of Control CA in target store - need to cut from analysis = ' + str(ca_ctl_intrg) + ' customers')

# COMMAND ----------

# MAGIC %md ## save group to temp table

# COMMAND ----------

## write customer group to temp table and will be remove later after code finish

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_t1_cdfr'

t1_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

t1_cdf = spark.table(tbl_nm)

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_c1_cdfr'

c1_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

c1_cdf = spark.table(tbl_nm)

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_c2_cdfr'

c2_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

c2_cdf = spark.table(tbl_nm)

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_t2_cdfr'

t2_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

t2_cdf = spark.table(tbl_nm)

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_t3_cdfr'

t3_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

t3_cdf = spark.table(tbl_nm)

tbl_nm = 'tdm_dev.o2o_' + c_nm + '_c3_cdfr'

c3_cdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_nm)

c3_cdf = spark.table(tbl_nm)

# COMMAND ----------

# MAGIC %md ## Get KPI for each cust group

# COMMAND ----------

def get_cust_kpi(intxn, cst_base, cst_grp):
  ##
  ## 2 inputs parameters
  ## intxn   : spark Dataframe: Focus transaction in Focus period
  ## cst_base: double : base customers of customer group (activated + non activated)
  ## cst_grp : string : string : name of customer group
  ##
  ## Return a spark Dataframe contain KPI of customers group in focus transaciton
  
  atxn = intxn.agg( lit(cst_grp).alias('cust_group')
                   ,lit(cst_base).alias('based_cust')
                   ,countDistinct(intxn.household_id).alias('atv_cust')                   
                   ,countDistinct(intxn.transaction_uid).alias('cc_visits')                   
                   ,sum(intxn.net_spend_amt).alias('sum_cc_sales')
                   ,sum(intxn.pkg_weight_unit).alias('sum_cc_units')                   
                   ,avg(intxn.net_spend_amt).alias('avg_cc_sales')
                   ,avg(intxn.pkg_weight_unit).alias('avg_cc_units')
                  )\
              
  
  odf  = atxn.withColumn('avg_vpc', (atxn.cc_visits/atxn.atv_cust))\
             .withColumn('avg_spc', (atxn.sum_cc_sales/atxn.atv_cust))\
             .withColumn('avg_spv', (atxn.sum_cc_sales/atxn.cc_visits))\
             .withColumn('avg_upv', (atxn.sum_cc_units/atxn.cc_visits))\
             .withColumn('avg_ppu', (atxn.sum_cc_sales/atxn.sum_cc_units))\
             .withColumn('unatv_cust', (atxn.based_cust - atxn.atv_cust))\
             .withColumn('atv_rate', (atxn.atv_cust/atxn.based_cust))
  
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

# MAGIC %md ### Add Funciton "get cust KPI" for online customer separated by CA group

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

# MAGIC %md ###Get number of customer in each group for information

# COMMAND ----------

## Get number of customer in each group for information

## all target
t1_ccnt = t1_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
t2_ccnt = t2_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
t3_ccnt = t3_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt

all_trg = t1_ccnt + t2_ccnt + t3_ccnt
## all control
c1_ccnt = c1_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
c2_ccnt = c2_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt
c3_ccnt = c3_cdf.agg(sum(lit(1)).alias('ccnt')).collect()[0].ccnt

all_ctl = c1_ccnt + c2_ccnt + c3_ccnt

## print 

print(' Number T1 customers (offline only) = ' + str(t1_ccnt ))
print(' Number C1 customers (offline only) = ' + str(c1_ccnt ))
print('-'*80)
print(' Number T2 customers (online + offline cust) = ' + str(t2_ccnt ))
print(' Number C2 customers (online + offline cust) = ' + str(c2_ccnt ))
print('-'*80)
print(' Number T3 customers (online only) = ' + str(t3_ccnt ))
print(' Number C3 customers (online only) = ' + str(c3_ccnt ))
print('-'*80)

print(' All target customers  = ' + str(all_trg ))
print(' All control customers = ' + str(all_ctl ))

# COMMAND ----------

# MAGIC %md ### brand level

# COMMAND ----------

##----------------------------------------------------
## brand level
##----------------------------------------------------

## Offline only
t1_batv_txn = bnd_txn_dur.join(t1_cdf, bnd_txn_dur.household_id == t1_cdf.t1_hid, 'left_semi')
c1_batv_txn = bnd_txn_dur.join(c1_cdf, bnd_txn_dur.household_id == c1_cdf.c1_hid, 'left_semi')
  
# ## online 2 offline
# t2_batv_txn = bnd_txn_dur.join(t2_cdf, bnd_txn_dur.household_id == t2_cdf.t2_hid, 'left_semi')
# c2_batv_txn = bnd_txn_dur.join(c2_cdf, bnd_txn_dur.household_id == c2_cdf.c2_hid, 'left_semi')

# ## online only
# t3_batv_txn = bnd_txn_dur.join(t3_cdf, bnd_txn_dur.household_id == t3_cdf.t3_hid, 'left_semi')
# c3_batv_txn = bnd_txn_dur.join(c3_cdf, bnd_txn_dur.household_id == c3_cdf.c3_hid, 'left_semi')

##---------------------------------------------------------------------
## change to inner join for online customer to get 'ca_group' 
##---------------------------------------------------------------------
## As of 7 Apr 2023: Control still not separated by CA group - one group as 'online'  -- Pat
##---------------------------------------------------------------------

## online 2 offline
t2_batv_txn = bnd_txn_dur.join  (t2_cdf, bnd_txn_dur.household_id == t2_cdf.t2_hid, 'inner')\
                         .select( bnd_txn_dur['*']
                                 ,t2_cdf.ca_group)

c2_batv_txn = bnd_txn_dur.join  (c2_cdf, bnd_txn_dur.household_id == c2_cdf.c2_hid, 'inner')\
                         .select( bnd_txn_dur['*']
                                 ,c2_cdf.ca_group)

## online only
t3_batv_txn = bnd_txn_dur.join  (t3_cdf, bnd_txn_dur.household_id == t3_cdf.t3_hid, 'inner')\
                         .select( bnd_txn_dur['*']
                                 ,t3_cdf.ca_group)
  
c3_batv_txn = bnd_txn_dur.join(c3_cdf, bnd_txn_dur.household_id == c3_cdf.c3_hid, 'inner')\
                         .select( bnd_txn_dur['*']
                                 ,c3_cdf.ca_group)

# COMMAND ----------

##----------------------------------------------------
## brand level
##----------------------------------------------------

## Offline only
t1_kpi = get_cust_kpi(t1_batv_txn, t1_ccnt,'T1')
c1_kpi = get_cust_kpi(c1_batv_txn, c1_ccnt,'C1')

## ONO group
t2_kpi = get_cust_kpi(t2_batv_txn, t2_ccnt,'T2')
c2_kpi = get_cust_kpi(c2_batv_txn, c2_ccnt,'C2')

## Online only
t3_kpi = get_cust_kpi(t3_batv_txn, t3_ccnt,'T3')
c3_kpi = get_cust_kpi(c3_batv_txn, c3_ccnt,'C3')

##----------------------------------
## By CA group
##----------------------------------
## ONO group

t2_kpi_ca = get_cust_kpi_ca(t2_batv_txn, t2_cdf,'T2')
## Online only
t3_kpi_ca = get_cust_kpi_ca(t3_batv_txn, t3_cdf,'T3')


# COMMAND ----------

## offline only
file_tnm   = 't1c1_offline_' + c_nm + '_brandlevel_random_reach.csv'
full_tnm   = out_path + file_tnm

t1_kpi_pd = t1_kpi.toPandas()
c1_kpi_pd = c1_kpi.toPandas()

g1_kpi_pd = pd.concat([t1_kpi_pd,c1_kpi_pd], axis = 0)

g1_kpi_pd.display()

g1_kpi_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## o2o 
file_tnm   = 't2c2_omni_' + c_nm + '_brandlevel_random_reach.csv'
full_tnm   = out_path + file_tnm

t2_kpi_pd = t2_kpi.toPandas()
c2_kpi_pd = c2_kpi.toPandas()

g2_kpi_pd = pd.concat([t2_kpi_pd,c2_kpi_pd], axis = 0)

g2_kpi_pd.display()

g2_kpi_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## Add export T2 by CA group ------------------- 7 Arp 2023 Pat

file_tnm   = 't2_omni_' + c_nm + '_by_cagroup_brandlevel_random_reach.csv'
full_tnm   = out_path + file_tnm

t2_kpi_ca_pd = t2_kpi_ca.toPandas()

t2_kpi_ca_pd.display()

t2_kpi_ca_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## online 

file_tnm   = 't3c3_online_' + c_nm + '_brandlevel_random_reach.csv'
full_tnm   = out_path + file_tnm

t3_kpi_pd  = t3_kpi.toPandas()
c3_kpi_pd  = c3_kpi.toPandas()

g3_kpi_pd = pd.concat([t3_kpi_pd, c3_kpi_pd], axis = 0)

g3_kpi_pd.display()

g3_kpi_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## Add export T3 by CA group ------------------- 7 Arp 2023 Pat

file_tnm   = 't3_online_' + c_nm + '_by_cagroup_brandlevel_random_reach.csv'
full_tnm   = out_path + file_tnm

t3_kpi_ca_pd = t3_kpi_ca.toPandas()

t3_kpi_ca_pd.display()

t3_kpi_ca_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')


# COMMAND ----------

# MAGIC %md ### SKU Level

# COMMAND ----------

##----------------------------------------------------
## sku level
##----------------------------------------------------

## Offline only
t1_satv_txn = sku_txn_dur.join(t1_cdf, sku_txn_dur.household_id == t1_cdf.t1_hid, 'left_semi')
c1_satv_txn = sku_txn_dur.join(c1_cdf, sku_txn_dur.household_id == c1_cdf.c1_hid, 'left_semi')
  
# ## online 2 offline
# t2_satv_txn = sku_txn_dur.join(t2_cdf, sku_txn_dur.household_id == t2_cdf.t2_hid, 'left_semi')
# c2_satv_txn = sku_txn_dur.join(c2_cdf, sku_txn_dur.household_id == c2_cdf.c2_hid, 'left_semi')

# ## online only
# t3_satv_txn = sku_txn_dur.join(t3_cdf, sku_txn_dur.household_id == t3_cdf.t3_hid, 'left_semi')
# c3_satv_txn = sku_txn_dur.join(c3_cdf, sku_txn_dur.household_id == c3_cdf.c3_hid, 'left_semi')


##---------------------------------------------------------------------
## change to inner join for online customer to get 'ca_group' 
##---------------------------------------------------------------------
## As of 7 Apr 2023: Control still not separated by CA group - one group as 'online'  -- Pat
##---------------------------------------------------------------------

## online 2 offline
t2_satv_txn = sku_txn_dur.join  (t2_cdf, sku_txn_dur.household_id == t2_cdf.t2_hid, 'inner')\
                         .select( sku_txn_dur['*']
                                 ,t2_cdf.ca_group)

c2_satv_txn = sku_txn_dur.join  (c2_cdf, sku_txn_dur.household_id == c2_cdf.c2_hid, 'inner')\
                         .select( sku_txn_dur['*']
                                 ,c2_cdf.ca_group)

## online only
t3_satv_txn = sku_txn_dur.join  (t3_cdf, sku_txn_dur.household_id == t3_cdf.t3_hid, 'inner')\
                         .select( sku_txn_dur['*']
                                 ,t3_cdf.ca_group)
  
c3_satv_txn = sku_txn_dur.join(c3_cdf, sku_txn_dur.household_id == c3_cdf.c3_hid, 'inner')\
                         .select( sku_txn_dur['*']
                                 ,c3_cdf.ca_group)


# COMMAND ----------

##----------------------------------------------------
## SKU level
##----------------------------------------------------

## Offline only
t1_kpi_sku = get_cust_kpi(t1_satv_txn, t1_ccnt,'T1')
c1_kpi_sku = get_cust_kpi(c1_satv_txn, c1_ccnt,'C1')

## ONO group
t2_kpi_sku = get_cust_kpi(t2_satv_txn, t2_ccnt,'T2')
c2_kpi_sku = get_cust_kpi(c2_satv_txn, c2_ccnt,'C2')

## Online only
t3_kpi_sku = get_cust_kpi(t3_satv_txn, t3_ccnt,'T3')
c3_kpi_sku = get_cust_kpi(c3_satv_txn, c3_ccnt,'C3')

##----------------------------------
## By CA group
##----------------------------------

## ONO group
t2_kpi_sku_ca = get_cust_kpi_ca(t2_satv_txn, t2_cdf,'T2')
## Online only
t3_kpi_sku_ca = get_cust_kpi_ca(t3_satv_txn, t3_cdf,'T3')

# COMMAND ----------

## offline only

file_tnm      = 't1c1_offline_' + c_nm + '_skulevel_random_reach.csv'
full_tnm      = out_path + file_tnm

t1_kpi_sku_pd = t1_kpi_sku.toPandas()
c1_kpi_sku_pd = c1_kpi_sku.toPandas()

g1_kpi_sku_pd = pd.concat([t1_kpi_sku_pd,c1_kpi_sku_pd], axis = 0)

g1_kpi_sku_pd.display()

g1_kpi_sku_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')


## o2o

file_tnm      = 't2c2_omni_' + c_nm + '_skulevel_random_reach.csv'
full_tnm      = out_path + file_tnm
  

t2_kpi_sku_pd = t2_kpi_sku.toPandas()
c2_kpi_sku_pd = c2_kpi_sku.toPandas()

g2_kpi_sku_pd = pd.concat([t2_kpi_sku_pd,c2_kpi_sku_pd], axis = 0)

g2_kpi_sku_pd.display()

g2_kpi_sku_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## Add export T2 by CA group ------------------- 7 Arp 2023 Pat

file_tnm         = 't2_omni_' + c_nm + '_by_cagroup_skulevel_random_reach.csv'
full_tnm         = out_path + file_tnm

t2_kpi_sku_ca_pd = t2_kpi_sku_ca.toPandas()

t2_kpi_sku_ca_pd.display()

t2_kpi_sku_ca_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## online
file_tnm      = 't3c3_online_' + c_nm + '_skulevel_random_reach.csv'
full_tnm      = out_path + file_tnm

t3_kpi_sku_pd = t3_kpi_sku.toPandas()
c3_kpi_sku_pd = c3_kpi_sku.toPandas()

g3_kpi_sku_pd = pd.concat([t3_kpi_sku_pd,c3_kpi_sku_pd], axis = 0)
g3_kpi_sku_pd.display()

g3_kpi_sku_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

## Add export T2 by CA group ------------------- 7 Arp 2023 Pat

file_tnm         = 't3_online_' + c_nm + '_by_cagroup_skulevel_random_reach.csv'
full_tnm         = out_path + file_tnm

t3_kpi_sku_ca_pd = t3_kpi_sku_ca.toPandas()

t3_kpi_sku_ca_pd.display()

t3_kpi_sku_ca_pd.to_csv(full_tnm, index = False, header = True, encoding = 'utf8')

# COMMAND ----------

# MAGIC %md # Cost calculation

# COMMAND ----------

# MAGIC %md ## Online cost - per head

# COMMAND ----------

## Initial value

## Fix for online (this case)

online_cost    = float(oln_fee)

## number of ca => ca_trg_cnt
## 70%          => ca_trg_70  *** using this

oln_pcst_cost  = online_cost/reach_use

print(' cost per head = ' + str(oln_pcst_cost))

out_hdr_col        = []
out_cost_data_list = []

out_hdr_col.extend(['ca_reach', 'oln_pcst_cost'])
out_cost_data_list.extend([reach_use, oln_pcst_cost])

# print(out_hdr_col)
#print(out_cost_data_list)

# COMMAND ----------

# MAGIC %md ## Offline cost (myLo + nonMyLo)

# COMMAND ----------

offline_cost   = int(ofn_fee)  ## from input

trg_cc_pen     = cc_pen    ## from campaign eval NPD5Star (Need visit mylo pen)

## all mylo offline => trgstr_cst_d_df --> 55% 

trg_cst_cnt    = trgstr_cst_d_df.agg(sum(lit(1)).alias('trg_cc_cnt')).collect()[0].trg_cc_cnt

all_cst_estm   = trg_cst_cnt/trg_cc_pen

ofn_pcst_cost  = offline_cost/all_cst_estm

o2n_pcst_cost  = oln_pcst_cost + ofn_pcst_cost

print(' Total Mylo customer in target store           = ' + str(trg_cst_cnt))
print(' Total estimated all customers in target store = ' + str(all_cst_estm))
print(' Cost per head = ' + str(ofn_pcst_cost))

out_hdr_col.extend(['trg_cst_cnt','all_ofn_cust_estm', 'cc_pen', 'ofn_pcst_cost', 'o2n_pcst_cost','#ca_ctl_intrg_removed'])
out_cost_data_list.extend([trg_cst_cnt, all_cst_estm, trg_cc_pen, ofn_pcst_cost, o2n_pcst_cost, ca_ctl_intrg])


# COMMAND ----------

# MAGIC %md # ROAS calculation

# COMMAND ----------

# MAGIC %md ## ROAS brand level

# COMMAND ----------

# MAGIC %md ### Offline (Group 1)

# COMMAND ----------

## Get based customers

#t1_kpi_pd  = t1_kpi.toPandas() 

t1_cst         = float(t1_kpi_pd.iloc[0]['based_cust'] )
t1_atv_cst     = float(t1_kpi_pd.iloc[0]['atv_cust'] )
  
t1_spend       = float(t1_kpi_pd.iloc[0]['all_sales'] )
  
t1_cost        = float(t1_cst * ofn_pcst_cost)
t1_roas        = t1_spend / float(t1_cost)
t1_cpa         = t1_cost/t1_atv_cst
  
## SKU Level  
t1_spend_sku   = float(t1_kpi_sku_pd.iloc[0]['all_sales'] )
t1_atv_cst_sku = float(t1_kpi_sku_pd.iloc[0]['atv_cust'] )

t1_roas_sku    = t1_spend_sku/t1_cost
t1_cpa_sku     = t1_cost/t1_atv_cst_sku

print( ' T1 based customers    = ' + str(t1_cst))
print( ' T1 Cost for all custs = ' + str(t1_cost))
print( ' T1 Spend to brand     = ' + str(t1_spend))
print( ' T1 ROAS Brand         = ' + str(t1_roas) )
print( ' T1 CPA Brand          = ' + str(t1_cpa) )

print( ' T1 ROAS SKU           = ' + str(t1_roas_sku) )
print( ' T1 CPA SKU            = ' + str(t1_cpa_sku) )

##--------------------------------------------------
## Control ** using the same per cust cost
##--------------------------------------------------

out_hdr_col.extend([ 't1_ofn_cst'
                    ,'t1_ofn_all_cost'
                    ,'t1_ofn_all_brand_spend'
                    ,'t1_ofn_roas_brand'
                    ,'t1_ofn_cpa_brand'
                    ,'t1_ofn_roas_sku'
                    ,'t1_ofn_cpa_sku'
                   ])

out_cost_data_list.extend([t1_cst, t1_cost, t1_spend, t1_roas, t1_cpa, t1_roas_sku, t1_cpa_sku])

# COMMAND ----------

# MAGIC %md ### Online (Group 3)

# COMMAND ----------

## Get based customers

#t3_kpi_pd  = t3_kpi.toPandas()

t3_cst         = float(t3_kpi_pd.iloc[0]['based_cust'] )
t3_spend       = float(t3_kpi_pd.iloc[0]['all_sales'] )

t3_cost        = float(t3_cst * oln_pcst_cost)
t3_roas        = t3_spend / float(t3_cost)
t3_cpa         = t3_cost/t3_cst

## SKU Level  
t3_spend_sku   = float(t3_kpi_sku_pd.iloc[0]['all_sales'] )
t3_atv_cst_sku = float(t3_kpi_sku_pd.iloc[0]['atv_cust'] )

t3_roas_sku    = t3_spend_sku/t3_cost
t3_cpa_sku     = t3_cost/t3_atv_cst_sku

print( ' T3 based customers    = ' + str(t3_cst))
print( ' T3 Cost for all custs = ' + str(t3_cost))
print( ' T3 Spend to brand     = ' + str(t3_spend))
print( ' T3 ROAS brand         = ' + str(t3_roas) )
print( ' T3 CPA brand          = ' + str(t3_cpa) )

print( ' T3 ROAS SKU           = ' + str(t3_roas_sku) )
print( ' T3 CPA SKU            = ' + str(t3_cpa_sku) )

out_hdr_col.extend([ 't3_ofn_cst'
                    ,'t3_ofn_all_cost'
                    ,'t3_ofn_all_brand_spend'
                    ,'t3_ofn_roas_brand'
                    ,'t3_ofn_cpa_brand'
                    ,'t3_ofn_roas_sku'
                    ,'t3_ofn_cpa_sku'
                   ])
out_cost_data_list.extend([t3_cst, t3_cost, t3_spend, t3_roas, t3_cpa, t3_roas_sku, t3_cpa_sku])

# COMMAND ----------

# MAGIC %md ### Online & Offline media (Group 2)

# COMMAND ----------

## Get based customers

#t2_kpi_pd  = t2_kpi.toPandas()

t2_cst     = float(t2_kpi_pd.iloc[0]['based_cust'] )
t2_spend   = float(t2_kpi_pd.iloc[0]['all_sales'] )

t2_cost    = float(t2_cst * (oln_pcst_cost + ofn_pcst_cost))
t2_roas    = t2_spend / float(t2_cost)
t2_cpa     = t2_cost/t2_cst

## SKU Level  
t2_spend_sku   = float(t2_kpi_sku_pd.iloc[0]['all_sales'] )
t2_atv_cst_sku = float(t2_kpi_sku_pd.iloc[0]['atv_cust'] )

t2_roas_sku    = t2_spend_sku/t2_cost
t2_cpa_sku     = t2_cost/t2_atv_cst_sku

print( ' T2 based customers    = ' + str(t2_cst))
print( ' T2 Cost for all custs = ' + str(t2_cost))
print( ' T2 Spend to brand     = ' + str(t2_spend))
print( ' T2 ROAS brand         = ' + str(t2_roas))
print( ' T2 CPA brand          = ' + str(t2_cpa))

print( ' T2 ROAS SKU           = ' + str(t2_roas_sku) )
print( ' T2 CPA SKU            = ' + str(t2_cpa_sku) )

out_hdr_col.extend([ 't2_ofn_cst'
                    ,'t2_ofn_all_cost'
                    ,'t2_ofn_all_brand_spend'
                    ,'t2_ofn_roas_brand'
                    ,'t2_ofn_cpa_brand'
                    ,'t2_ofn_roas_sku'
                    ,'t2_ofn_cpa_sku'
                   ])

out_cost_data_list.extend([t2_cst, t2_cost, t2_spend, t2_roas, t2_cpa, t2_roas_sku, t2_cpa_sku])

# COMMAND ----------

# MAGIC %md ### Create PD dataframe for cost calcuation & export

# COMMAND ----------

#print(out_hdr_col)
#print(out_cost_data_list)

pd_o2o_cost = pd.DataFrame([out_cost_data_list], columns = out_hdr_col)

pd_o2o_cost.display()

#file_nm      = 'all_cost_roas_' + brand_name + '_' + c_id + '_brandlevel_wt_random_reach.csv'
file_nm      = c_nm + '_all_cost_roas_brandlevel_random_reach.csv'
full_nm      = out_path + file_nm

pd_o2o_cost.to_csv(full_nm, index = False, header = True, encoding = 'utf8')

print(' Cost-Return data is sucessfully save at :' + full_nm)

# COMMAND ----------

# MAGIC %md # Zip File and send out

# COMMAND ----------

## add check if zip file exists will remove and create new
outfile       = c_nm + '_o2o_output.zip'
full_file_zip = sprj_path + outfile

try:
    dbutils.fs.ls(full_file_zip)
    print('-' * 80 + '\n' + ' Warning !! Current zip file exists : ' + full_file_zip + '.\n Process will remove and recreate zip file. \n' + '-' * 80)    
    dbutils.fs.rm(full_file_zip)    
    print(' Removed file already! Process will re-create new file. \n')    
except :
    print(' Zip file : ' + str(full_file_zip) + ' is creating, please wait \n')
    
    
create_zip_from_dbsf_prefix_indir(pprj_path, outfile)

# COMMAND ----------

# MAGIC %md # Temp section

# COMMAND ----------


# # file_path    = '/dbfs/FileStore/media/category_overview/outputs/mamypoko_pr_tape/switching/'
# file_path = 'dbfs:/FileStore/media/campaign_eval/04_O2O/Nov_2022/2022_0744_M01C_downy_npd5star/'

# # file_cmove   = file_path_t + 'custmove_mamy_pr_tape.zip'
# dbutils.fs.rm(file_path, recurse = True)
# # create_zip_from_dbsf_prefix_indir(file_path, file_cmove)

# COMMAND ----------

#
