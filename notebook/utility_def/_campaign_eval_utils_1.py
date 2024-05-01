# Databricks notebook source
# MAGIC %md
# MAGIC /Users/thanakrit.boonquarmdee@lotuss.com/utils/edm_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Library + Tables

# COMMAND ----------

#import libraries
from pyspark.sql import functions as F
from pyspark.sql import types as T

import pandas as pd
import numpy as np
import re
import seaborn as sns
import matplotlib.pyplot as plt
# from lifelines import KaplanMeierFitter

from datetime import timedelta

from sklearn.metrics import auc
from sklearn.preprocessing import StandardScaler

from scipy.spatial import distance
import statistics as stats
from sklearn.metrics.pairwise import cosine_similarity

from typing import List
from pyspark.sql import DataFrame as sparkDataFrame
from pandas import DataFrame as pandasDataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import media utility function

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# def div_zero_chk(x, y):
#     if y == 0:
#         return 0
#     else:
#         return x/y
# ## end def

# COMMAND ----------

# MAGIC %md
# MAGIC # Get feature product df, brand_df and list of brand, class, subclass

# COMMAND ----------

def _get_prod_df( sku_list: List
                , cate_lvl: str
                , std_ai_df : sparkDataFrame
                , x_cate_flag
                , x_cate_cd:str ):
    """To get Product information refering to input SKU list (expected input as list )
       function will return feature dataframe (feat_df)
                          , brand dataframe (brand_df) 
                          , subclass dataframe (sclass_df) containing all product in subclass for matching
                          , category dataframe (cate_df) containing all product in category defined of campaign.
                          ,list of brand (brand_list) 
                          ,list of class_id (class_cd_list) 
                          ,list of class_name (class_nm_list) 
                          ,list of subclass_id (sclass_cd_list) 
                          ,list of subclass_name (sclass_nm_list)  
    """
    prod_all = sqlContext.table('tdm.v_prod_dim_c')
    mfr      = sqlContext.table('tdm.v_mfr_dim_c')
    
    prod_mfr = prod_all.join(mfr, 'mfr_id', 'left')\
                       .select( prod_all.upc_id
                               ,prod_all.product_en_desc.alias('prod_en_desc')
                               ,prod_all.brand_name.alias('brand_nm')
                               ,prod_all.mfr_id
                               ,mfr.mfr_name
                               ,prod_all.division_id.alias('div_id')
                               ,prod_all.division_name.alias('div_nm')
                               ,prod_all.department_id.alias('dept_id')
                               ,prod_all.department_code.alias('dept_cd')
                               ,prod_all.department_name.alias('dept_nm')
                               ,prod_all.section_id.alias('sec_id')
                               ,prod_all.section_code.alias('sec_cd')
                               ,prod_all.section_name.alias('sec_nm')
                               ,prod_all.class_id.alias('class_id')
                               ,prod_all.class_code.alias('class_cd')
                               ,prod_all.class_name.alias('class_nm')
                               ,prod_all.subclass_id.alias('sclass_id')
                               ,prod_all.subclass_code.alias('sclass_cd')
                               ,prod_all.subclass_name.alias('sclass_nm')
                              ).persist()
                              
    
    feat_df  = prod_mfr.where ( prod_mfr.upc_id.isin(sku_list))
                       
    del mfr
    del prod_all
    
    ## get brand name list
    brand_pd = feat_df.select(feat_df.brand_nm)\
                      .dropDuplicates()\
                      .toPandas()
    
    brand_list = brand_pd['brand_nm'].to_list()
    
    print('-'*80 + '\n List of Brand Name show below : \n ' + '-'*80)
    print(brand_list)
    
    del brand_pd
    
    ## get subclass list
    sclass_cd_list = feat_df.select(feat_df.sclass_cd).dropDuplicates().toPandas()['sclass_cd'].to_list()
    sclass_nm_list = feat_df.select(feat_df.sclass_nm).dropDuplicates().toPandas()['sclass_nm'].to_list()
    
    print('-'*80 + '\n List of Subclass Id and Subclass Name show below : \n ' + '-'*80)
    print(sclass_cd_list)
    print(sclass_nm_list)
    
    ## get class list
    class_cd_list = feat_df.select(feat_df.class_cd).dropDuplicates().toPandas()['class_cd'].to_list()
    class_nm_list = feat_df.select(feat_df.class_nm).dropDuplicates().toPandas()['class_nm'].to_list()
    
    print('-'*80 + '\n List of Class Name show below : \n ' + '-'*80)
    print(class_cd_list)
    print(class_nm_list)
    
       
    ## get section list
    sec_cd_list = feat_df.select(feat_df.sec_cd).dropDuplicates().toPandas()['sec_cd'].to_list()
    sec_nm_list = feat_df.select(feat_df.sec_nm).dropDuplicates().toPandas()['sec_nm'].to_list()
    
    print('-'*80 + '\n List of Section Name show below : \n ' + '-'*80)
    print(sec_nm_list)

    ## get mfr name
    mfr_nm_list = feat_df.select(feat_df.mfr_name).dropDuplicates().toPandas()['mfr_name'].to_list()
        
    print('-'*80 + '\n List of Manufactor Name show below : \n ' + '-'*80)
    print(mfr_nm_list)
    
    ## get use aisle dataframe
    print('Cross cate flag = '  + str(x_cate_flag) )
    
    if str(x_cate_flag) == '':
        x_cate_flag = 0
    ## end if
    
    ## check cross category
    if (float(x_cate_flag) == 1) | (str(x_cate_flag) == 'true') :
        x_cate_list   = x_cate_cd.split(',')
        get_ai_sclass = [cd.strip() for cd in x_cate_list]
        print('-'*80 + '\n Using Cross Category code to define Aisle, input list of subclass code show below \n ' + '-'*80)
    else:
        get_ai_sclass = sclass_cd_list ## use feature subclass list
        print('-'*80 + '\n Using Subclass of feature product to define Aisle, input list of subclass code show below \n ' + '-'*80)
    ## end if
    
    print('get_ai_sclass = ' + str(get_ai_sclass))
    
    use_ai_grp_list = list(std_ai_df.where (std_ai_df.subclass_code.isin(get_ai_sclass))\
                                    .select(std_ai_df.group)\
                                    .dropDuplicates()\
                                    .toPandas()['group'])
    
    print('-'*80 + '\n List of Aisle group to be use show below : \n ' + '-'*80)
    print(use_ai_grp_list)
    
    use_ai_sclass = list(std_ai_df.where (std_ai_df.group.isin(use_ai_grp_list))\
                                  .select(std_ai_df.subclass_code)\
                                  .dropDuplicates()\
                                  .toPandas()['subclass_code'])
    
    use_ai_df       = prod_mfr.where(prod_mfr.sclass_cd.isin(use_ai_sclass))   ## all product in aisles group
    
    use_ai_sec_list = list(use_ai_df.select(use_ai_df.sec_nm)\
                                  .dropDuplicates()\
                                  .toPandas()['sec_nm'])
    
    ## get class & Subclass DataFrame    
    class_df  = prod_mfr.where(prod_mfr.class_cd.isin(class_cd_list))
    sclass_df = prod_mfr.where(prod_mfr.sclass_cd.isin(sclass_cd_list))    
    
    # check category_level for brand definition
    if cate_lvl == 'subclass':
        brand_df  = prod_mfr.where ( prod_mfr.brand_nm.isin(brand_list) & prod_mfr.sclass_cd.isin(sclass_cd_list))
        cate_df   = prod_mfr.where  ( prod_mfr.sclass_cd.isin(sclass_cd_list))
        cate_cd_list = sclass_cd_list
    elif cate_lvl == 'class':
        brand_df  = prod_mfr.where ( prod_mfr.brand_nm.isin(brand_list) & prod_mfr.class_cd.isin(class_cd_list))
        cate_df   = prod_mfr.where (  prod_mfr.class_cd.isin(class_cd_list))
        cate_cd_list = class_cd_list
    else:
        raise Exception("Incorrect category Level")
    ## end if
                       
    return feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list, use_ai_grp_list, use_ai_sec_list

## End def


# COMMAND ----------

# MAGIC %md ## Get prod df version 2

# COMMAND ----------

def _get_prod_df_v2( sku_list: List
                    , cate_lvl: str
                    , std_ai_df : sparkDataFrame
                    , x_cate_flag
                    , x_cate_cd:str ):
    """To get Product information refering to input SKU list (expected input as list )
       function will return 
                            product dataframe (prod_mfr) ** added from _get_prod_df : 24 May 2023 Pat
                          ,feature dataframe (feat_df)
                          , brand dataframe (brand_df) 
                          , subclass dataframe (sclass_df) containing all product in subclass for matching
                          , category dataframe (cate_df) containing all product in category defined of campaign.
                          ,list of brand (brand_list) 
                          ,list of class_id (class_cd_list) 
                          ,list of class_name (class_nm_list) 
                          ,list of subclass_id (sclass_cd_list) 
                          ,list of subclass_name (sclass_nm_list)  
    """
    prod_all = sqlContext.table('tdm.v_prod_dim_c')
    mfr      = sqlContext.table('tdm.v_mfr_dim_c')
    
    prod_mfr = prod_all.join(mfr, 'mfr_id', 'left')\
                       .select( prod_all.upc_id
                               ,prod_all.product_en_desc.alias('prod_en_desc')
                               ,prod_all.brand_name.alias('brand_nm')
                               ,prod_all.mfr_id
                               ,mfr.mfr_name
                               ,prod_all.division_id.alias('div_id')
                               ,prod_all.division_name.alias('div_nm')
                               ,prod_all.department_id.alias('dept_id')
                               ,prod_all.department_code.alias('dept_cd')
                               ,prod_all.department_name.alias('dept_nm')
                               ,prod_all.section_id.alias('sec_id')
                               ,prod_all.section_code.alias('sec_cd')
                               ,prod_all.section_name.alias('sec_nm')
                               ,prod_all.class_id.alias('class_id')
                               ,prod_all.class_code.alias('class_cd')
                               ,prod_all.class_name.alias('class_nm')
                               ,prod_all.subclass_id.alias('sclass_id')
                               ,prod_all.subclass_code.alias('sclass_cd')
                               ,prod_all.subclass_name.alias('sclass_nm')
                              ).persist()
                              
    
    feat_df  = prod_mfr.where ( prod_mfr.upc_id.isin(sku_list))
                       
    del mfr
    del prod_all
    
    ## get brand name list
    brand_pd = feat_df.select(feat_df.brand_nm)\
                      .dropDuplicates()\
                      .toPandas()
    
    brand_list = brand_pd['brand_nm'].to_list()
    
    print('-'*80 + '\n List of Brand Name show below : \n ' + '-'*80)
    print(brand_list)
    
    del brand_pd
    
    ## get subclass list
    sclass_cd_list = feat_df.select(feat_df.sclass_cd).dropDuplicates().toPandas()['sclass_cd'].to_list()
    sclass_nm_list = feat_df.select(feat_df.sclass_nm).dropDuplicates().toPandas()['sclass_nm'].to_list()
    
    print('-'*80 + '\n List of Subclass Id and Subclass Name show below : \n ' + '-'*80)
    print(sclass_cd_list)
    print(sclass_nm_list)
    
    ## get class list
    class_cd_list = feat_df.select(feat_df.class_cd).dropDuplicates().toPandas()['class_cd'].to_list()
    class_nm_list = feat_df.select(feat_df.class_nm).dropDuplicates().toPandas()['class_nm'].to_list()
    
    print('-'*80 + '\n List of Class Name show below : \n ' + '-'*80)
    print(class_cd_list)
    print(class_nm_list)
    
       
    ## get section list
    sec_cd_list = feat_df.select(feat_df.sec_cd).dropDuplicates().toPandas()['sec_cd'].to_list()
    sec_nm_list = feat_df.select(feat_df.sec_nm).dropDuplicates().toPandas()['sec_nm'].to_list()
    
    print('-'*80 + '\n List of Section Name show below : \n ' + '-'*80)
    print(sec_nm_list)

    ## get mfr name
    mfr_nm_list = feat_df.select(feat_df.mfr_name).dropDuplicates().toPandas()['mfr_name'].to_list()
        
    print('-'*80 + '\n List of Manufactor Name show below : \n ' + '-'*80)
    print(mfr_nm_list)
    
    ## get use aisle dataframe
    print('Cross cate flag = '  + str(x_cate_flag) )
    
    if str(x_cate_flag) == '':
        x_cate_flag = 0
    ## end if
    
    ## check cross category
    if (float(x_cate_flag) == 1) | (str(x_cate_flag) == 'true') :
        x_cate_list   = x_cate_cd.split(',')
        get_ai_sclass = [cd.strip() for cd in x_cate_list]
        print('-'*80 + '\n Using Cross Category code to define Aisle, input list of subclass code show below \n ' + '-'*80)
    else:
        get_ai_sclass = sclass_cd_list ## use feature subclass list
        print('-'*80 + '\n Using Subclass of feature product to define Aisle, input list of subclass code show below \n ' + '-'*80)
    ## end if
    
    print('get_ai_sclass = ' + str(get_ai_sclass))
    
    use_ai_grp_list = list(std_ai_df.where (std_ai_df.subclass_code.isin(get_ai_sclass))\
                                    .select(std_ai_df.group)\
                                    .dropDuplicates()\
                                    .toPandas()['group'])
    
    print('-'*80 + '\n List of Aisle group to be use show below : \n ' + '-'*80)
    print(use_ai_grp_list)
    
    use_ai_sclass = list(std_ai_df.where (std_ai_df.group.isin(use_ai_grp_list))\
                                  .select(std_ai_df.subclass_code)\
                                  .dropDuplicates()\
                                  .toPandas()['subclass_code'])
    
    use_ai_df       = prod_mfr.where(prod_mfr.sclass_cd.isin(use_ai_sclass))   ## all product in aisles group
    
    use_ai_sec_list = list(use_ai_df.select(use_ai_df.sec_nm)\
                                  .dropDuplicates()\
                                  .toPandas()['sec_nm'])
    
    ## get class & Subclass DataFrame    
    class_df  = prod_mfr.where(prod_mfr.class_cd.isin(class_cd_list))
    sclass_df = prod_mfr.where(prod_mfr.sclass_cd.isin(sclass_cd_list))    
    
    # check category_level for brand definition
    if cate_lvl == 'subclass':
        brand_df  = prod_mfr.where ( prod_mfr.brand_nm.isin(brand_list) & prod_mfr.sclass_cd.isin(sclass_cd_list))
        cate_df   = prod_mfr.where  ( prod_mfr.sclass_cd.isin(sclass_cd_list))
        cate_cd_list = sclass_cd_list
    elif cate_lvl == 'class':
        brand_df  = prod_mfr.where ( prod_mfr.brand_nm.isin(brand_list) & prod_mfr.class_cd.isin(class_cd_list))
        cate_df   = prod_mfr.where (  prod_mfr.class_cd.isin(class_cd_list))
        cate_cd_list = class_cd_list
    else:
        raise Exception("Incorrect category Level")
    ## end if
    
    ## add step join aisle to product to have product with aisle name -- Pat 30 May 2023
    
    prod_mfr  = prod_mfr.join   (std_ai_df, [prod_mfr.sclass_cd == std_ai_df.subclass_code], 'left')\
                        .select ( prod_mfr['*']
                                 ,F.when(std_ai_df.group.isNull(), lit('Unidentified'))
                                   .otherwise(std_ai_df.group)
                                   .alias('aisle_group')
                                )
    
    return prod_mfr, feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list, use_ai_grp_list, use_ai_sec_list

## End def


# COMMAND ----------

# MAGIC %md ## Get prod no Aisle

# COMMAND ----------

def _get_prod_df_no_aisle( sku_list: List
                          , cate_lvl: str
                          , x_cate_flag
                          , x_cate_cd:str ):
    """To get Product information refering to input SKU list (expected input as list )
       function will return feature dataframe (feat_df)
                          , brand dataframe (brand_df) 
                          , subclass dataframe (sclass_df) containing all product in subclass for matching
                          , category dataframe (cate_df) containing all product in category defined of campaign.
                          ,list of brand (brand_list) 
                          ,list of class_id (class_cd_list) 
                          ,list of class_name (class_nm_list) 
                          ,list of subclass_id (sclass_cd_list) 
                          ,list of subclass_name (sclass_nm_list)  
    """
    prod_all = sqlContext.table('tdm.v_prod_dim_c')
    mfr      = sqlContext.table('tdm.v_mfr_dim')
    
    prod_mfr = prod_all.join(mfr, 'mfr_id', 'left')\
                       .select( prod_all.upc_id
                               ,prod_all.product_en_desc.alias('prod_en_desc')
                               ,prod_all.brand_name.alias('brand_nm')
                               ,prod_all.mfr_id
                               ,mfr.mfr_name
                               ,prod_all.division_id.alias('div_id')
                               ,prod_all.division_name.alias('div_nm')
                               ,prod_all.department_id.alias('dept_id')
                               ,prod_all.department_code.alias('dept_cd')
                               ,prod_all.department_name.alias('dept_nm')
                               ,prod_all.section_id.alias('sec_id')
                               ,prod_all.section_code.alias('sec_cd')
                               ,prod_all.section_name.alias('sec_nm')
                               ,prod_all.class_id.alias('class_id')
                               ,prod_all.class_code.alias('class_cd')
                               ,prod_all.class_name.alias('class_nm')
                               ,prod_all.subclass_id.alias('sclass_id')
                               ,prod_all.subclass_code.alias('sclass_cd')
                               ,prod_all.subclass_name.alias('sclass_nm')
                              ).persist()
                              
    
    feat_df  = prod_mfr.where ( prod_mfr.upc_id.isin(sku_list))
                       
    del mfr
    del prod_all
    
    ## get brand name list
    brand_pd = feat_df.select(feat_df.brand_nm)\
                      .dropDuplicates()\
                      .toPandas()
    
    brand_list = brand_pd['brand_nm'].to_list()
    
    print('-'*80 + '\n List of Brand Name show below : \n ' + '-'*80)
    print(brand_list)
    
    del brand_pd
    
    ## get subclass list
    sclass_cd_list = feat_df.select(feat_df.sclass_cd).dropDuplicates().toPandas()['sclass_cd'].to_list()
    sclass_nm_list = feat_df.select(feat_df.sclass_nm).dropDuplicates().toPandas()['sclass_nm'].to_list()
    
    print('-'*80 + '\n List of Subclass Id and Subclass Name show below : \n ' + '-'*80)
    print(sclass_cd_list)
    print(sclass_nm_list)
    
    ## get class list
    class_cd_list = feat_df.select(feat_df.class_cd).dropDuplicates().toPandas()['class_cd'].to_list()
    class_nm_list = feat_df.select(feat_df.class_nm).dropDuplicates().toPandas()['class_nm'].to_list()
    
    print('-'*80 + '\n List of Class Name show below : \n ' + '-'*80)
    print(class_cd_list)
    print(class_nm_list)
    
       
    ## get section list
    sec_cd_list = feat_df.select(feat_df.sec_cd).dropDuplicates().toPandas()['sec_cd'].to_list()
    sec_nm_list = feat_df.select(feat_df.sec_nm).dropDuplicates().toPandas()['sec_nm'].to_list()
    
    print('-'*80 + '\n List of Section Name show below : \n ' + '-'*80)
    print(sec_nm_list)

    ## get mfr name
    mfr_nm_list = feat_df.select(feat_df.mfr_name).dropDuplicates().toPandas()['mfr_name'].to_list()
        
    print('-'*80 + '\n List of Manufactor Name show below : \n ' + '-'*80)
    print(mfr_nm_list)
    
    #### get use aisle dataframe
    ##
    #### check cross category
    ##if x_cate_flag == 'true'     :
    ##    x_cate_list   = x_cate_cd.split(',')
    ##    get_ai_sclass = [cd.strip() for cd in x_cate_list]
    ##    print('-'*80 + '\n Using Cross Category code to define Aisle, input list of subclass code show below \n ' + '-'*80)
    ##else:
    ##    get_ai_sclass = sclass_cd_list ## use feature subclass list
    ##    print('-'*80 + '\n Using Subclass of feature product to define Aisle, input list of subclass code show below \n ' + '-'*80)
    #### end if
    ##
    ##print(get_ai_sclass)
    ##
    ##use_ai_grp_list = list(std_ai_df.where (std_ai_df.subclass_code.isin(get_ai_sclass))\
    ##                                .select(std_ai_df.group)\
    ##                                .dropDuplicates()\
    ##                                .toPandas()['group'])
    ##
    ##print('-'*80 + '\n List of Aisle group to be use show below : \n ' + '-'*80)
    ##print(use_ai_grp_list)
    ##
    ##use_ai_sclass = list(std_ai_df.where (std_ai_df.group.isin(use_ai_grp_list))\
    ##                              .select(std_ai_df.subclass_code)\
    ##                              .dropDuplicates()\
    ##                              .toPandas()['subclass_code'])
    ##
    ##use_ai_df       = prod_mfr.where(prod_mfr.sclass_cd.isin(use_ai_sclass))   ## all product in aisles group
    ##
    ##use_ai_sec_list = list(use_ai_df.select(use_ai_df.sec_nm)\
    ##                              .dropDuplicates()\
    ##                              .toPandas()['sec_nm'])
    ##
    
    ## get class & Subclass DataFrame    
    class_df         = prod_mfr.where(prod_mfr.class_cd.isin(class_cd_list))
    sclass_df        = prod_mfr.where(prod_mfr.sclass_cd.isin(sclass_cd_list))    
    
    brand_at_cls_df  = prod_mfr.where(prod_mfr.brand_nm.isin(brand_list) & prod_mfr.class_cd.isin(class_cd_list))
    brand_at_scls_df = prod_mfr.where(prod_mfr.brand_nm.isin(brand_list) & prod_mfr.sclass_cd.isin(sclass_cd_list))
    
    # check category_level for brand definition
    if cate_lvl == 'subclass':
        brand_df  = brand_at_scls_df
        cate_df   = prod_mfr.where  ( prod_mfr.sclass_cd.isin(sclass_cd_list))
        cate_cd_list = sclass_cd_list
    elif cate_lvl == 'class':
        brand_df  = brand_at_cls_df
        cate_df   = prod_mfr.where (  prod_mfr.class_cd.isin(class_cd_list))
        cate_cd_list = class_cd_list
    else:
        raise Exception("Incorrect category Level")
    ## end if
                       
    return prod_mfr, feat_df, brand_df, class_df, sclass_df, cate_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list

## End def



##feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, sclass_cd_list, sclass_nm_list, mfr_nm_list = _get_prod_df(feat_list
##                                                                                                                                                                                ,cate_lvl
##                                                                                                                                                                                ,std_ai_df
##                                                                                                                                                                                ,cross_cate_flag
##                                                                                                                                                                                ,cross_cate_cd)

# COMMAND ----------

# MAGIC %md ## adjacency product

# COMMAND ----------

def _get_adjacency_product_id(promoted_upc_id: List, exposure_group_file: str, 
                             sel_sec: str, sel_class: str, sel_subcl: str = None,
                             adjacency_type: str = 'SECTION_CLASS', cross_adjacency_group: str = None) -> sparkDataFrame:
    """If adjacency type = 'PARENT_CATEGORY', will use select section, class, subclass to find adjacency product group and product id
    Otherwise will use cross_adjacency_group to find adjacency product id
    """
    print(f'Adjacency type : {adjacency_type}')
    if adjacency_type == 'PARENT_CATEGORY':
        out, exposure_gr = _get_parent_product_id(promoted_upc_id, exposure_group_file)
    elif adjacency_type == 'SECTION_CLASS':
        out, exposure_gr = _get_hier_group_prod_id(sel_sec, sel_class, exposure_group_file)
    elif cross_adjacency_group == 'CROSS_CATEGORY':
        print(f'Cross category type to group : {cross_adjacency_group}')
        out = _get_group_product_id(cross_adjacency_group, exposure_group_file)
        exposure_gr = spark.createDataFrame([(cross_adjacency_group,)], ['group',])
    else:
        print(f'Could not find adjacency type {adjacency_type} (with {cross_adjacency_group}')
        
    return out, exposure_gr

# COMMAND ----------

def _get_hier_group_prod_id(sel_sec, sel_class, exposure_group_file):
    """From promoted section, class, subclass find exposure group and exposure ucp id
    """
    from pyspark.sql import functions as F
    
    exposure_group = spark.read.csv(exposure_group_file, header="true", inferSchema="true")
    
    selected_exposure_group = \
    (exposure_group
     .filter(F.col('section_name')==sel_sec)
     .filter(F.col('class_name')==sel_class)
     .select('group')
     .drop_duplicates()
    )
    
    sec_id_class_id_exposure_group = \
    (exposure_group
     .join(selected_exposure_group, 'group')
     .select('section_id', 'class_id', 'group')
     .drop_duplicates()
    )
    
    prod_exposure_group = \
    (spark.table('tdm.v_prod_dim')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .join(sec_id_class_id_exposure_group, ['section_id', 'class_id'])
     .select('upc_id', 'section_id', 'class_id', 'section_name', 'class_name', 'group')
     .drop_duplicates()
    )
    
#     print('Section, Class under exposure group :')
#     (prod_exposure_group
#      .select('group', 'class_name', 'section_name')
#      .drop_duplicates()
#      .orderBy('group', 'section_name', 'class_name')
#     ).show(truncate=False)
    
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
    
    exposure_group_name = prod_exposure_group.select('group').drop_duplicates()
    
    return upc_id_exposure_group, exposure_group_name

# COMMAND ----------

def _get_parent_product_id(promoted_upc_id, exposure_group_file):
    """From promoted product, find exposure group and exposure ucp id
    """
    from pyspark.sql import functions as F
    
    exposure_group = spark.read.csv(exposure_group_file, header="true", inferSchema="true")

    sec_id_class_id_promoted_product = \
    (spark.table('tdm.v_prod_dim')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .filter(F.col('upc_id').isin(promoted_upc_id))
     .select('section_id', 'section_name', 'class_id', 'class_name')
     .drop_duplicates()
    )
    print('Promoted product category :')
    sec_id_class_id_promoted_product.show(truncate=False)
    
    exposure_group_promoted_product = \
    (exposure_group
     .join(sec_id_class_id_promoted_product, ['section_id', 'class_id'])
     .select('group')
     .drop_duplicates()
    )
    print('Exposure group of promoted products :')
    exposure_group_promoted_product.show(truncate=False)
    
    sec_id_class_id_exposure_group = \
    (exposure_group
     .join(exposure_group_promoted_product, 'group')
     .select('section_id', 'class_id', 'group')
     .drop_duplicates()
    )
    
    prod_exposure_group = \
    (spark.table('tdm.v_prod_dim')
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

    return upc_id_exposure_group, exposure_group_promoted_product

# COMMAND ----------

def _get_group_product_id(cross_adjacency_group, exposure_group_file):
    """Idenfity exposure group from group name
    """
    from pyspark.sql import functions as F
    
    exposure_group = spark.read.csv(exposure_group_file, header="true", inferSchema="true")

    sec_id_class_id_exposure_group = \
    (exposure_group
     .filter(F.col('group') == cross_adjacency_group)
     .select('section_id', 'class_id', 'group')
     .drop_duplicates()
    )
    
    prod_exposure_group = \
    (spark.table('tdm.v_prod_dim')
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

# COMMAND ----------

def _get_factor_product_id(promoted_upc_id: List) -> sparkDataFrame:
    """From promoted product -> find adjacency upc_id
    """
    from pyspark.sql import functions as F
    
    adjacency_list = spark.read.csv("/FileStore/Dip/Upload/underlying_category_16092021.csv", header="true", inferSchema="true")

    prod_with_factor_n = \
    (spark.table('tdm.v_prod_dim')
     .filter(F.col('division_id').isin([1,2,3,4,9,10,13]))
     .withColumn('section_name', F.regexp_replace('section_name','\W',''))
     .withColumn('class_name', F.regexp_replace('class_name','\W',''))
     .withColumn('Category', F.concat_ws('_', 'section_name', 'class_name'))
     .drop_duplicates()
     .join(adjacency_list, 'Category', 'left')
    )
    
    promoted_prod_factor_n = prod_with_factor_n.filter(F.col('upc_id').isin(promoted_upc_id)).drop_duplicates()
    
    print('Promoted products, factor_n')
    promoted_prod_factor_n.select('factor_n', 'Category', 'section_name', 'class_name').drop_duplicates().show(truncate=False)
    
    print('Factor_n to underlying categories')
    promoted_factor_n = promoted_prod_factor_n.filter(F.col('factor_n').isNotNull()).select('factor_n', 'Category').drop_duplicates()
    promoted_factor_n.show(truncate=False)
    
    print('Without factor_n, use section as underlying categories')
    non_factor_n_section = promoted_prod_factor_n.filter(F.col('factor_n').isNull()).select('section_name').drop_duplicates()
    non_factor_n_section.show(truncate=False)
    
    upc_id_with_factor_n = prod_with_factor_n.join(promoted_factor_n, 'factor_n').select('upc_id').drop_duplicates()
    upc_id_without_factor_n = prod_with_factor_n.join(non_factor_n_section, 'section_name').select('upc_id').drop_duplicates()
    upc_id_adjacency = upc_id_with_factor_n.unionByName(upc_id_without_factor_n).drop_duplicates()
    
    print('Count of adjacency upc_id')
    (prod_with_factor_n
     .join(upc_id_adjacency, 'upc_id')
     .groupBy('factor_n', 'division_name', 'department_name', 'section_name', 'class_name', 'Category')
     .agg(F.count('upc_id'))
     .orderBy('factor_n', 'division_name', 'department_name', 'section_name', 'class_name', 'Category')
    ).display()
    
    return upc_id_adjacency

# COMMAND ----------

# MAGIC %md ## Get Aisle Penetration during period

# COMMAND ----------

def get_aisle_penetration_all(txn, ai_sku_df, wk_type, store_fmt) :

    """Column name for period week identification """
    
    if wk_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif wk_type in ["promozone"]:
        period_wk_col_nm = "period_promo_mv_wk"
    else:
        period_wk_col_nm = "period_fis_wk"
    ## end if
 
    txn_dur       = txn.where ( (F.col(period_wk_col_nm) == 'cmp') &
                                (lower(txn.store_format_group) == store_fmt.lower())
                              )
                       
                               
    txn_dur_agg   = txn_dur.agg ( lit(store_fmt).alias('level') 
                                 ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                 ,countDistinct(txn_dur.household_id).alias('custs_dur')                                 
                                )
    
    txn_ai_agg    = txn_dur.join  ( ai_sku_df, txn.upc_id == ai_sku_df.upc_id, 'left_semi')\
                           .agg  ( lit('aisle').alias('level')
                                  ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                  ,countDistinct(txn_dur.household_id).alias('custs_dur')                                  
                                 )
                                 
    all_agg       = txn_ai_agg.unionByName(txn_dur_agg, allowMissingColumns=True)
    
    return all_agg
 
 ## end def

# COMMAND ----------

def get_aisle_penetration_trg(txn, ai_sku_df, wk_type, store_fmt, trg_sf) :

    """Column name for period week identification """
    
    if wk_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif wk_type in ["promozone"]:
        period_wk_col_nm = "period_promo_mv_wk"
    else:
        period_wk_col_nm = "period_fis_wk"
    ## end if
    
    trg_sf_u      = trg_sf.select('store_id').dropDuplicates()

    txn_dur       = txn.where ( (F.col(period_wk_col_nm) == 'cmp') &
                                (lower(txn.store_format_group) == store_fmt.lower())
                              )\
                       .join  (trg_sf_u, txn.store_id == trg_sf_u.store_id, 'left_semi')
                       
                               
    txn_dur_agg   = txn_dur.agg ( lit(store_fmt).alias('level') 
                                 ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                 ,countDistinct(txn_dur.household_id).alias('custs_dur')                                 
                                )
    
    txn_ai_agg    = txn_dur.join  ( ai_sku_df, txn.upc_id == ai_sku_df.upc_id, 'left_semi')\
                           .agg  ( lit('aisle').alias('level')
                                  ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                  ,countDistinct(txn_dur.household_id).alias('custs_dur')                                  
                                 )
                                 
    all_agg       = txn_ai_agg.unionByName(txn_dur_agg, allowMissingColumns=True)
    
    return all_agg
 
 ## end def

# COMMAND ----------

def get_aisle_penetration_trgall(txn, ai_sku_df, wk_type, store_fmt, trg_sf) :

    """Column name for period week identification """
    
    if wk_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif wk_type in ["promozone"]:
        period_wk_col_nm = "period_promo_mv_wk"
    else:
        period_wk_col_nm = "period_fis_wk"
    ## end if
    
    trg_sf_u      = trg_sf.select('store_id').dropDuplicates()

    ## using Aisle defintion for both media store & total media format in Lotuss

    txn_dur       = txn.where ( (F.col(period_wk_col_nm) == 'cmp') &
                                (lower(txn.store_format_group) == store_fmt.lower())
                              )\
                       .join  ( ai_sku_df, txn.upc_id == ai_sku_df.upc_id, 'left_semi')
                       
                               
    txn_dur_agg   = txn_dur.agg ( lit(store_fmt).alias('level') 
                                 ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                 ,countDistinct(txn_dur.household_id).alias('custs_dur')                                 
                                )
    
    txn_ai_agg    = txn_dur.join  ( trg_sf_u, txn.store_id == trg_sf_u.store_id, 'left_semi')\
                           .agg   ( lit('aisle_in_' + store_fmt).alias('level')
                                  ,countDistinct(txn_dur.transaction_uid).alias('visits_dur')
                                  ,countDistinct(txn_dur.household_id).alias('custs_dur')                                  
                                  )
                                 
    all_agg       = txn_ai_agg.unionByName(txn_dur_agg, allowMissingColumns=True)
    
    return all_agg
 

# COMMAND ----------

# MAGIC %md ##Exposure report : Awareness

# COMMAND ----------

# Comment out entire function as it is superseded in utils_2
# def get_awareness(txn, cp_start_date, cp_end_date, test_store_sf, adj_prod_id,
#                   family_size, media_spend):
#     """Exposure report for Awareness
    
#     """
#     #get only txn in exposure area in test store
#     print('='*80)
#     print('Exposure v.2 - add media mechanics multiplyer by store')
#     print('Exposed customer (test store) from "OFFLINE" channel only')
#     print('Count check number of store by media mechanics')
#     test_store_sf.groupBy('media_mechanic').count().show()
    
#     txn_exposed = \
#     (txn
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('offline_online_other_channel')=='OFFLINE')
#      .join(test_store_sf,'store_id','inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
#     )
    
#     by_store_impression = \
#     (txn_exposed
#      .groupBy('store_id', 'media_mechanic')
#      .agg(
#        F.countDistinct('transaction_uid').alias('epos_visits'),
#        F.countDistinct( (F.when(F.col('customer_id').isNotNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('carded_visits'),
#        F.countDistinct( (F.when(F.col('customer_id').isNull(), F.col('transaction_uid') ).otherwise(None) ) ).alias('non_carded_visits')
#        )
#      .withColumn('epos_impression', F.col('epos_visits')*family_size*F.col('media_mechanic')) 
#      .withColumn('carded_impression', F.col('carded_visits')*family_size*F.col('media_mechanic')) 
#      .withColumn('non_carded_impression', F.col('non_carded_visits')*family_size*F.col('media_mechanic'))
#      )
    
#     all_impression = \
#     (by_store_impression
#      .agg(F.sum('epos_visits').alias('epos_visits'),
#           F.sum('carded_visits').alias('carded_visits'),
#           F.sum('non_carded_visits').alias('non_carded_visits'),
#           F.sum('epos_impression').alias('epos_impression'),
#           F.sum('carded_impression').alias('carded_impression'),
#           F.sum('non_carded_impression').alias('non_carded_impression')
#          )
#     )
    
#     all_store_customer = txn_exposed.agg(F.countDistinct( F.col('household_id') ).alias('carded_customers') ).collect()[0][0]
    
#     exposure_result = \
#     (all_impression
#      .withColumn('carded_reach', F.lit(all_store_customer))
#      .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach') )
#      .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
#      .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
#      .withColumn('media_spend', F.lit(media_spend))
#      .withColumn('CPM', F.lit(media_spend) / ( F.col('epos_impression') / 1000 ) )
#     )         
# #      .withColumn('carded_reach', F.col('carded_customers'))
# #      .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_customers') )
# #      .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq') )
    
#     return exposure_result

# COMMAND ----------

# MAGIC %md ##customer uplift

# COMMAND ----------

# Comment out function due to superseding in utils_2
# def get_customer_uplift(txn, cp_start_date, cp_end_date, prior_start_date, prior_end_date, pre_start_date, pre_end_date,
#                         ctr_store_list, test_store_sf, adj_prod_id, cust_uplift_lv, sel_sec, sel_class, sel_brand, sel_sku):
#     """customer id that expose (shop adjacency product during campaing in test store)
#     unexpose (shop adjacency product during campaing in control store)
#     expose superseded unexpose
#     """
#     from pyspark.sql import functions as F
    
#     #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
#     txn_exposed = \
#     (txn
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('offline_online_other_channel')=='OFFLINE') # for offline media
#      .join(test_store_sf, 'store_id', 'inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
#     )
#     print('Exposed customer (test store) from "OFFLINE" channel only')
    
#     txn_unexposed = \
#     (txn
#      .filter(F.col('offline_online_other_channel')=='OFFLINE') # for offline media
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('store_id').isin(ctr_store_list))
#      .join(adj_prod_id, 'upc_id', 'inner')
#     )
#     print('Unexposed customer (control store) from "OFFLINE" channel only')
    
#     exposed_cust = \
#     (txn_exposed
#      .filter(F.col('household_id').isNotNull())
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('1st_exposed_date'))
#      .withColumn('exposed_flag', F.lit(1))
#     )

#     unexposed_cust = \
#     (txn_unexposed
#      .filter(F.col('household_id').isNotNull())
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('1st_unexposed_date'))
#      .withColumn('unexposed_flag', F.lit(1))
#     )

#     exposure_cust_table = exposed_cust.join(unexposed_cust, 'household_id', 'outer').fillna(0)
    
#     #----------------------
#     #---- uplift
#     # brand buyer, during campaign
#     if cust_uplift_lv == 'brand':
#         print('-'*30)
#         print(f'{cust_uplift_lv.upper()} buyer from "OFFLINE" & "ONLINE"')
#         print('Customer uplift at BRAND level')
        
#         buyer_dur_campaign = \
#         (txn.filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('brand_name')== sel_brand)
#          .groupBy('household_id')
#          .agg(F.sum('net_spend_amt').alias('spending'),
#               F.min('date_id').alias('1st_buy_date'))
#         )
        
#         # customer movement brand : prior - pre
#         cust_prior = \
#         (txn.filter(F.col('date_id').between(prior_start_date, prior_end_date))
#          .filter(F.col('section_name') == sel_sec)
#          .filter(F.col('brand_name')== sel_brand)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('household_id').isNotNull())
#          .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
#         )

#         cust_pre = \
#         (txn.filter(F.col('date_id').between(pre_start_date, pre_end_date))
#          .filter(F.col('section_name') == sel_sec)
#          .filter(F.col('brand_name')== sel_brand)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('household_id').isNotNull())
#          .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
#         )
        
#     elif cust_uplift_lv == 'sku':
#         print('-'*30)
#         print(f'{cust_uplift_lv.upper()} buyer from "OFFLINE" & "ONLINE"')
#         print('Customer uplift at SKU level')
#         buyer_dur_campaign = \
#         (txn.filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('upc_id').isin(sel_sku))
#          .groupBy('household_id')
#          .agg(F.sum('net_spend_amt').alias('spending'),
#               F.min('date_id').alias('1st_buy_date'))
#         )
#         # customer movement brand : prior - pre
#         cust_prior = \
#         (txn.filter(F.col('date_id').between(prior_start_date, prior_end_date))
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('upc_id').isin(sel_sku))
#          .groupBy('household_id').agg(F.sum('net_spend_amt').alias('prior_spending'))
#         )

#         cust_pre = \
#         (txn.filter(F.col('date_id').between(pre_start_date, pre_end_date))
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('upc_id').isin(sel_sku))
#          .groupBy('household_id').agg(F.sum('net_spend_amt').alias('pre_spending'))
#         )
        
#     # brand buyer, exposed-unexposed cust
#     dur_camp_exposure_cust_and_buyer = \
#     (exposure_cust_table
#      .join(buyer_dur_campaign, 'household_id', 'left')
#      .withColumn('exposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNotNull() ) & \
#                                                  (F.col('1st_buy_date').isNotNull() ) & \
#                                                  (F.col('1st_exposed_date') <= F.col('1st_buy_date')), '1').otherwise(0))
#      .withColumn('unexposed_and_buy_flag', F.when( (F.col('1st_exposed_date').isNull()) & \
#                                                    (F.col('1st_unexposed_date').isNotNull()) & \
#                                                    (F.col('1st_buy_date').isNotNull()) & \
#                                                    (F.col('1st_unexposed_date') <= F.col('1st_buy_date')), '1').otherwise(0)) 
#     )
#     dur_camp_exposure_cust_and_buyer.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().show()
    
#     prior_pre_cust_buyer = cust_prior.join(cust_pre, 'household_id', 'outer').fillna(0)
    
#     # brand buyer, exposed-unexposed cust x customer movement
#     movement_and_exposure = \
#     (dur_camp_exposure_cust_and_buyer
#      .join(prior_pre_cust_buyer,'household_id', 'left')
#      .withColumn('customer_group', F.when(F.col('pre_spending')>0,'existing')
#                  .when(F.col('prior_spending')>0,'lapse').otherwise('new'))
#     )
    
#     movement_and_exposure.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.countDistinct('household_id')).show()
    
#     # BRAND : uplift
#     cust_uplift = \
#     (movement_and_exposure
#      .groupby('customer_group','exposed_flag','unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag')
#      .agg(F.countDistinct('household_id').alias('customers'))
#     )
#     # find customer by group
#     group_expose = cust_uplift.filter(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_customers'))
#     group_expose_buy = \
#     (cust_uplift.filter(F.col('exposed_and_buy_flag')==1).groupBy('customer_group').agg(F.sum('customers').alias('exposed_shoppers')))

#     group_unexpose = \
#     (cust_uplift
#      .filter( (F.col('exposed_flag')==0) & (F.col('unexposed_flag')==1) )
#      .groupBy('customer_group').agg(F.sum('customers').alias('unexposed_customers'))
#     )
#     group_unexpose_buy = \
#     (cust_uplift
#      .filter(F.col('unexposed_and_buy_flag')==1)
#      .groupBy('customer_group')
#      .agg(F.sum('customers').alias('unexposed_shoppers'))
#     )

#     uplift = group_expose.join(group_expose_buy,'customer_group') \
#                                .join(group_unexpose,'customer_group') \
#                                .join(group_unexpose_buy,'customer_group')

#     uplift_result = uplift.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
#                           .withColumn('cvs_rate_test', F.col('exposed_shoppers')/F.col('exposed_customers'))\
#                           .withColumn('cvs_rate_ctr', F.col('unexposed_shoppers')/F.col('unexposed_customers'))\
#                           .withColumn('pct_uplift', F.col('cvs_rate_test')/F.col('cvs_rate_ctr') - 1 )\
#                           .withColumn('uplift_cust',(F.col('cvs_rate_test')-F.col('cvs_rate_ctr'))*F.col('exposed_customers'))

    
#     return uplift_result

# COMMAND ----------

# MAGIC %md ##customer lifetime value

# COMMAND ----------

# Comment out function due to superseding in utils_2
# def get_customer_cltv(txn, cp_start_date, cp_end_date, test_store_sf, adj_prod_id, 
#                       sel_sec, sel_class, sel_brand, lv_cltv,
#                       uplift_brand, media_spend):
#     """customer id that expose (shop adjacency product during campaing in test store)
#     unexpose (shop adjacency product during campaing in control store)
#     expose superseded unexpose
#     """
#     from pyspark.sql import functions as F
    
#     #---- During Campaign : Exposed / Unexposed transaction, Expose / Unexposed household_id
#     # Change filter to offline_online_other_channel - Dec 2022 - Ta
#     print('Customer Life Time Value, based on "OFFLINE" exposed customer')
#     txn_exposed = \
#     (txn
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('offline_online_other_channel')=='OFFLINE') # for offline media
#      .join(test_store_sf, 'store_id', 'inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
#     )
    
#     exposed_cust = \
#     (txn_exposed
#      .filter(F.col('household_id').isNotNull())
#      .select('household_id')
#      .drop_duplicates()
#     )
    
#     # During campaign brand buyer and exposed
#     dur_campaign_brand_buyer_and_exposed = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('section_name')== sel_sec)
#          .filter(F.col('class_name')== sel_class)
#          .filter(F.col('brand_name')== sel_brand)
#          .join(exposed_cust, 'household_id', 'inner')
#          .groupBy('household_id')
#          .agg(F.sum('net_spend_amt').alias('spending'),
#               F.countDistinct('transaction_uid').alias('visits'))
#         )
    
#     # get spend per customer for both multi and one time buyer
#     spc_brand_mulibuy = \
#     (dur_campaign_brand_buyer_and_exposed
#      .filter(F.col('visits') > 1)
#      .agg(F.avg('spending').alias('spc_multi'))
#     )
    
#     spc_brand_onetime = \
#     (dur_campaign_brand_buyer_and_exposed
#      .filter(F.col('visits') == 1)
#      .agg(F.avg('spending').alias('spc_onetime'))
#     )
    
#     # get metrics
#     spc_multi = spc_brand_mulibuy.select('spc_multi').collect()[0][0]
#     spc_onetime = spc_brand_onetime.select('spc_onetime').collect()[0][0]
#     print(f'Spend per customers (multi): {spc_multi}')
#     print(f'Spend per customers (one time): {spc_onetime}')
    
#     #----------------------
#     #---- Level of CLVT, Purchase cycle
    
#     # import Customer survival rate
#     if lv_cltv.lower() == 'class':
#         brand_csr = to_pandas(spark.table('tdm_seg.brand_survival_rate_class_level_20211031')\
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class))
#     elif lv_cltv.lower() == 'subclass':
#         brand_csr = to_pandas(spark.table('tdm_seg.brand_survival_rate_subclass_level_20211031')\
#                             .filter(F.col('brand_name')==sel_brand)\
#                             .filter(F.col('class_name')==sel_class)\
#                             .filter(F.col('subclass_name')==sel_subclass))

#     # import purchase_cycle table
#     if lv_cltv.lower() == 'class':
#         pc_table = to_pandas(spark.table('tdm_seg.purchase_cycle_class_level_20211031')\
#                             .filter(F.col('class_name')==sel_class))

#     elif lv_cltv.lower() == 'subclass':
#         pc_table = to_pandas(spark.table('tdm_seg.purchase_cycle_subclass_level_20211031')\
#                             .filter(F.col('class_name')==sel_class)\
#                             .filter(F.col('subclass_name')==sel_subclass))
    
#     # CSR details
#     brand_csr_graph = brand_csr[[c for c in brand_csr.columns if 'CSR' in c]].T
#     brand_csr_graph.columns = ['survival_rate']
#     brand_csr_graph.display()
#     pandas_to_csv_filestore(brand_csr_graph, 'brand_survival_rate.csv', prefix=os.path.join(dbfs_project_path, 'result'))
    
#     # ---- Uplift calculation
#     total_uplift = uplift_brand.agg(F.sum('uplift_cust')).collect()[0][0]
#     one_time_ratio = brand_csr.one_time_ratio[0]
#     auc = brand_csr.AUC[0]
#     cc_pen = pc_table.cc_penetration[0]
#     spc_per_day = brand_csr.spc_per_day[0]
    
#     # calculate CLTV 
#     dur_cp_value = total_uplift*one_time_ratio*float(spc_onetime) + total_uplift*(1-one_time_ratio)*float(spc_multi)
#     post_cp_value = total_uplift*auc*spc_per_day
#     cltv = dur_cp_value+post_cp_value
#     epos_cltv = cltv/cc_pen
#     print("EPOS CLTV: ",np.round(epos_cltv,2))
    
#     #----- Break-even
#     breakeven_df = brand_csr_graph.copy()
#     # change first point to be one time ratio
#     breakeven_df.loc['CSR_0_wks'] = one_time_ratio

#     # calculate post_sales at each point of time in quarter value
#     breakeven_df['post_sales'] = breakeven_df.survival_rate * total_uplift * spc_per_day * 13*7  ## each quarter has 13 weeks and each week has 7 days
    
#     breakeven_df.display()
    
#     # calculate area under curve using 1/2 * (x+y) * a
#     area = []
#     for i in range(breakeven_df.shape[0]-1):
#         area.append(0.5*(breakeven_df.iloc[i,1]+breakeven_df.iloc[i+1,1]))

#     #create new data frame with value of sales after specific weeks
#     breakeven_df2 = pd.DataFrame(area,index=[13,26,39,52],columns=['sales'])

#     print("Display breakeven df area for starter : \n ")
    
#     breakeven_df2.display()
    
#     #set first day to be 0
#     breakeven_df2.loc[0,'sales'] = 0
#     breakeven_df2 = breakeven_df2.sort_index()

#     #adding during campaign to every point in time
#     breakeven_df2['sales+during'] = breakeven_df2+(dur_cp_value)

#     #create accumulative columns
#     breakeven_df2['acc_sales'] = breakeven_df2['sales+during'].cumsum()

#     ## pat add
#     print("Display breakeven table after add cumulative sales : \n ")
#     breakeven_df2.display()
    
#     #create epos value
#     breakeven_df2['epos_acc_sales'] = breakeven_df2['acc_sales']/cc_pen.astype('float')
#     breakeven_df2 = breakeven_df2.reset_index()
#     breakeven_df2.round(2)
    
#     #if use time more than 1 year, result show 1 year
#     if media_spend > breakeven_df2.loc[4,'epos_acc_sales']:
#         day_break = 'over one year'
#     #else find point where it is breakeven
#     else:
#         index_break = breakeven_df2[breakeven_df2.epos_acc_sales>media_spend].index[0]
        
#         # if it's in first period -> breakeven during campaing
#         if index_break == 0:
#             day_break = 0
#             # else find lower bound and upper bound to calculate day left before break as a straight line  
#         else:
#             low_bound = breakeven_df2.loc[index_break-1,'epos_acc_sales']
#             up_bound = breakeven_df2.loc[index_break,'epos_acc_sales']
#             low_bound_day =  breakeven_df2.loc[index_break-1,'index']*13*7
#             day_break = low_bound_day+((media_spend - low_bound) *13*7 / (up_bound-low_bound))

#     if type(day_break) == int:
#         breakeven_time_month = day_break//30
#         breakeven_time_day = day_break%30
#         print(f"Breakeven time: {int(breakeven_time_month)} months {int(breakeven_time_day)} days")
#     else:
#         print(f"Breakeven time: More than a year")
#         breakeven_time_month = 'More than a year'
#         breakeven_time_day = 'More than a year'
        
#     #create data frame for save
#     df_cltv = pd.DataFrame({'measures':['Total Uplift Customers','One time ratio','AUC','Spend per Customer per Day',
#                                         'CLTV', 'CC Penetration', 'EPOS CLTV', 'Breakeven Month', 'Breakeven Day'],
#                            'value':[total_uplift,one_time_ratio,auc,spc_per_day,cltv,cc_pen,epos_cltv,breakeven_time_month,
#                                     breakeven_time_day]})
#     df_cltv.round(2)

#     return df_cltv

# COMMAND ----------

# MAGIC %md ##sales uplift

# COMMAND ----------

# Comment out function due to superseding in utils_2
# def sales_uplift(txn, cp_start_date, cp_end_date, prior_start_date, pre_end_date,
#                  sales_uplift_lv, sel_sec, sel_class, sel_brand, sel_sku, 
#                  matching_df, matching_methodology: str = 'varience'):
#     """
    
#     """
#     from pyspark.sql import functions as F
    
#     #--- Map Promoweekid
# #     promoweek_id = \
# #     (spark
# #      .table('tdm.v_th_date_dim')
# #      .select('date_id', 'promoweek_id')
# #     ).drop_duplicates()
     
# #     txn = txn.join(F.broadcast(promoweek_id), on='date_id', how='left')
    
#     #----- Load matching store 
#     mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
#     try:
#         col_for_store_id = mapping_dict_method_col_name[matching_methodology]
#         matching_df_2 = matching_df[['store_id',col_for_store_id]].copy()
#         matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
#         # matching_df_2.display()
#         test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr']))
        
#     except:
#         print('Unrecognized store matching methodology')

#     #----------------------
#     #---- Uplift
#     # brand buyer, during campaign
#     # Changed channel column to offline_online_other_channel - Dec 2022 - Ta
#     if sales_uplift_lv == 'brand':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at BRAND level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subclass)
#          .filter(F.col('brand_name')==sel_brand)
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subclass)
#          .filter(F.col('brand_name')==sel_brand)
#         )
                 
#     elif sales_uplift_lv == 'sku':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at SKU level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )

#     #---- sales by store by period    
#     sales_pre = prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
#     sales_dur = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
#     sales_pre_df = to_pandas(sales_pre)
#     sales_dur_df = to_pandas(sales_dur)
    
#     #create weekly sales for each item
#     wk_sales_pre = to_pandas(prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
#     wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
#     #---- Matchingsales_pre_df = to_pandas(sales_pre)

#     # Prior-Pre sales
# #     matching_df = matching_df_2.copy()
# #     matching_df['test_pre_sum'] = matching_df_2[['store_id_test']].merge(wk_sales_pre,left_on='store_id_test',right_on='store_id').iloc[:,2:].sum(axis=1)
# #     matching_df['ctr_pre_sum'] = matching_df_2[['store_id_ctr']].merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id').iloc[:,2:].sum(axis=1)
# #     matching_df['ctr_factor'] = matching_df['test_pre_sum'] / matching_df['ctr_pre_sum']
# #     matching_df.fillna({'ctr_factor':1},inplace=True)
# #     matching_df.display()
#     # Dur sales
# #     matching_df['test_dur_sum'] = matching_df_2[['store_id_test']].merge(wk_sales_dur,left_on='store_id_test',right_on='store_id').iloc[:,2:].sum(axis=1)
# #     matching_df['ctr_dur_sum'] = matching_df_2[['store_id_ctr']].merge(wk_sales_dur,left_on='store_id_ctr',right_on='store_id').iloc[:,2:].sum(axis=1)
# #     matching_df['ctr_dur_sum_adjust'] = matching_df['ctr_dur_sum'] *  matching_df['ctr_factor']
# #     matching_df
    
#     matching_df = \
#     (matching_df_2
#      .merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
#      .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .assign(ctr_factor = lambda x : x['test_pre_sum'] / x['ctr_pre_sum'])
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
#      .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
#      .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum']*x['ctr_factor'])
#     ).loc[:, ['store_id_test', 'store_id_ctr', 'test_pre_sum', 'ctr_pre_sum', 'ctr_factor', 'test_dur_sum', 'ctr_dur_sum', 'ctr_dur_sum_adjust']]
    
#     sales_uplift = matching_df.test_dur_sum.sum() - matching_df.ctr_dur_sum_adjust.sum()
#     pct_sales_uplift = (sales_uplift / matching_df.ctr_dur_sum_adjust.sum()) 
#     print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    
#     uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
#                                 index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
#     #---- Weekly sales for plotting
#     # Test store
#     test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]
#                   .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')
#                   .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
# #     test_for_graph.display()
    
#     # Control store , control factor
#     ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']].merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id').merge(wk_sales_dur,on='store_id')
# #     ctr_for_graph.head()
    
#     # apply control factor for each week and filter only that
#     new_c_list=[]
    
#     for c in ctr_for_graph.columns[3:]:
#         new_c = c+'_adj'
#         ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
#         new_c_list.append(new_c)
        
#     ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
#     #create df with agg value
#     ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
#     #change column name to be the same with test_feature_for_graph
#     ctr_for_graph_final.columns = test_for_graph.columns

# #     ctr_for_graph_final.display()
    
#     sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
#     sales_uplift.reset_index().display()
    
#     return matching_df, uplift_table, sales_uplift

# COMMAND ----------

# Comment out function due to superseding in utils_2
# def sales_uplift_promo_wk(txn, cp_start_date, cp_end_date, prior_start_date, pre_end_date,
#                  sales_uplift_lv, sel_sec, sel_class, sel_brand, sel_sku, 
#                  matching_df, matching_methodology: str = 'varience'):
#     """
    
#     """
#     from pyspark.sql import functions as F
    
#     #--- Map Promoweekid
#     promoweek_id = \
#     (spark
#      .table('tdm.v_th_date_dim')
#      .select('date_id', 'promoweek_id')
#     ).drop_duplicates()
     
#     txn = txn.join(F.broadcast(promoweek_id), on='date_id', how='left')
    
#     #----- Load matching store 
#     mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
#     try:
#         col_for_store_id = mapping_dict_method_col_name[matching_methodology]
#         matching_df_2 = matching_df[['store_id',col_for_store_id]].copy()
#         matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
#         # matching_df_2.display()
#         test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr']))
        
#     except:
#         print('Unrecognized store matching methodology')

#     #----------------------
#     #---- Uplift
#     # brand buyer, during campaign
#     # Changed filter to offline_online_other_channel - Dec 2022 - Ta
#     if sales_uplift_lv == 'brand':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at BRAND level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subclass)
#         .filter(F.col('brand_name')==sel_brand)
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subclass)
#          .filter(F.col('brand_name')==sel_brand)
#         )
                 
#     elif sales_uplift_lv == 'sku':
#         print('-'*30)
#         print('Sales uplift based on "OFFLINE" channel only')
#         print('Sales uplift at SKU level')
#         prior_pre_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )

#         dur_txn_selected_prod_test_ctr_store = \
#         (txn
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('store_id').isin(test_ctr_store_id))
#          .filter(F.col('upc_id').isin(sel_sku))
#         )

#     #---- sales by store by period    
#     sales_pre = prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
#     sales_dur = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
#     sales_pre_df = to_pandas(sales_pre)
#     sales_dur_df = to_pandas(sales_dur)
    
#     #create weekly sales for each item
#     wk_sales_pre = to_pandas(prior_pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
#     wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('promoweek_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
#     matching_df = \
#     (matching_df_2
#      .merge(sales_pre_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_pre_sum'})
#      .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_pre_sum'})
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .assign(ctr_factor = lambda x : x['test_pre_sum'] / x['ctr_pre_sum'])
#      .fillna({'test_pre_sum':0, 'ctr_pre_sum':0})
#      .merge(sales_dur_df, left_on='store_id_test', right_on='store_id').rename(columns={'sales':'test_dur_sum'})
#      .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id').rename(columns={'sales':'ctr_dur_sum'})
#      .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum']*x['ctr_factor'])
#     ).loc[:, ['store_id_test', 'store_id_ctr', 'test_pre_sum', 'ctr_pre_sum', 'ctr_factor', 'test_dur_sum', 'ctr_dur_sum', 'ctr_dur_sum_adjust']]
    
#     sales_uplift = matching_df.test_dur_sum.sum() - matching_df.ctr_dur_sum_adjust.sum()
#     pct_sales_uplift = (sales_uplift / matching_df.ctr_dur_sum_adjust.sum()) 
#     print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    
#     uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
#                                 index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
#     #---- Weekly sales for plotting
#     # Test store
#     test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]
#                   .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')
#                   .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
    
#     # Control store , control factor
#     ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']].merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id').merge(wk_sales_dur,on='store_id')
    
#     # apply control factor for each week and filter only that
#     new_c_list=[]
    
#     for c in ctr_for_graph.columns[3:]:
#         new_c = c+'_adj'
#         ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
#         new_c_list.append(new_c)
        
#     ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
#     #create df with agg value
#     ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,columns=[f'{sales_uplift_lv}_ctr']).T
#     #change column name to be the same with test_feature_for_graph
#     ctr_for_graph_final.columns = test_for_graph.columns
    
#     sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
#     sales_uplift.reset_index().display()
    
#     return matching_df, uplift_table, sales_uplift

# COMMAND ----------

# MAGIC %md ##purchase cycle

# COMMAND ----------

def purchase_cycle(txn,category):
    #filter at category level
    txn_filter = txn.filter(F.col('class_name')==category)
    #create customer level, sales by date
    cust_table = txn_filter.select('customer_id','business_date').drop_duplicates()
    #calculate date diff, filter only 2nd time purchase up
    cust_table  = cust_table.withColumn('prev_date',F.lag('business_date').over(Window.partitionBy('customer_id').orderBy('business_date')))\
                          .filter(F.col('prev_date').isNotNull())\
                          .withColumn('date_diff', F.datediff(col('business_date'),col('prev_date')))
    #calculate average purchase cycle at customer level
    cust_pc = cust_table.groupBy('customer_id').agg(F.avg('date_diff').alias('purchase_cycle'))
    #calculate purchase cycle at category level
    mean_pc = cust_pc.groupBy().agg(F.avg('purchase_cycle').alias('mean_pc')).collect()[0][0]
    med_pc = cust_pc.groupBy().agg(F.percentile_approx('purchase_cycle',0.5).alias('med_pc')).collect()[0][0]
    
    return [mean_pc,med_pc]

# COMMAND ----------

# MAGIC %md ##Create Control store list from target class

# COMMAND ----------

def create_control_store(format_group_name: str, test_store_sf: sparkDataFrame, 
                         store_universe: str = None) -> pandasDataFrame:
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
        dummy_reserved_store = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'TALAD':
        universe_store_id = all_store_id.filter(F.col('format_id').isin([4])).select('store_id')
        dummy_reserved_store = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'GoFresh':
        store_universe = 'dbfs:/FileStore/media/reserved_store/20211226/media_store_universe_20220309_GoFresh.csv'
        print(f'For GoFresh store use network stores (1200 branched) universe \n Loading from {store_universe}')
        network_gofresh = spark.read.csv(store_universe, header=True, inferSchema=True)
        dummy_reserved_store = network_gofresh.join(test_store_sf, 'store_id', 'leftanti')
        
    else:
        print('Count not create dummy reserved store')
    
    dummy_reserved_store_df = to_pandas(dummy_reserved_store)
    
    return dummy_reserved_store_df

# COMMAND ----------

# MAGIC %md ##generate dummy reserved store

# COMMAND ----------

def create_dummy_reserved_store(format_group_name: str, test_store_sf: sparkDataFrame) -> pandasDataFrame:
    """
    Parameters
    ----------
    format_group_name:
        'HDE' / 'Talad' / 'GoFresh'.
        
    test_store_sf: sparkDataFrame
        Media features store list.
    """
    from pyspark.sql import functions as F
    
    all_store_id = spark.table(TBL_STORE).select('format_id', 'store_id').filter(~F.col('store_id').like('8%')).drop_duplicates()
    
    if format_group_name == 'HDE':
        universe_store_id = all_store_id.filter(F.col('format_id').isin([1,2,3])).select('store_id')
        dummy_reserved_store = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'TALAD':
        universe_store_id = all_store_id.filter(F.col('format_id').isin([4])).select('store_id')
        dummy_reserved_store = universe_store_id.join(test_store_sf, 'store_id', 'leftanti')
        
    elif format_group_name == 'GoFresh':
        print('For GoFresh store use network stores (1200 branched) universe')
        network_gofresh = spark.read.csv('dbfs:/FileStore/media/campaign_eval/GoFresh_network_store/GoFresh_storeList1200_20220207.csv', header=True, inferSchema=True)
        dummy_reserved_store = network_gofresh.join(test_store_sf, 'store_id', 'leftanti')
        
    else:
        print('Count not create dummy reserved store')
    
    dummy_reserved_store_df = to_pandas(dummy_reserved_store)
    
    return dummy_reserved_store_df

# COMMAND ----------

# MAGIC %md ##store matching

# COMMAND ----------

# Comment out function due to superseding in utils_2
# def get_store_matching(txn, prior_st_date, pre_end_date, store_matching_lv: str, sel_brand:str, sel_sku: List,
#                        test_store_sf: sparkDataFrame, reserved_store_sf: sparkDataFrame, matching_methodology: str = 'varience') -> List:
#     """
#     Parameters
#     ----------
#     txn: sparkDataFrame
        
#     prior_st_date: date_id
#         Prior start date, 26 week before campaign start date
    
#     pre_end_date: date_id
#         Pre end date, 1 day before start campaing start date
    
#     store_matching_lv: str
#         'brand' or 'sku'
    
#     sel_brand: str
#         Selected brand for analysis
    
#     sel_sku: List
#         List of promoted upc_id

#     test_store_sf: sparkDataFrame
#         Media features store list
        
#     reserved_store_sf: sparkDataFrame
#         Customer picked reserved store list, for finding store matching -> control store
        
#     matching_methodology: str, default 'varience'
#         'variance', 'euclidean', 'cosine_similarity'
#     """
#     from pyspark.sql import functions as F
    
#     from sklearn.metrics import auc
#     from sklearn.preprocessing import StandardScaler

#     from scipy.spatial import distance
#     import statistics as stats
#     from sklearn.metrics.pairwise import cosine_similarity
    
#     #---- filter txn at matching level
#     # Change filter to offline_online_other_channel
#     if store_matching_lv.lower() == 'brand':
#         print('Matching performance only "OFFLINE"')
#         print('Store matching at BRAND lv')
#         txn_matching = \
#         (txn
#          .filter(F.col('date_id').between(prior_st_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('brand_name')==sel_brand)
#         )
#     else:
#         print('Matching performance only "OFFLINE"')
#         print('Store matching at SKU lv')
#         txn_matching = \
#         (txn
#          .filter(F.col('date_id').between(prior_st_date, pre_end_date))
#          .filter(F.col('offline_online_other_channel')=='OFFLINE')
#          .filter(col('upc_id').isin(sel_sku))
#         )
#     #---- Map weekid
# #     date_id_week_id_mapping = \
# #     (spark
# #      .table('tdm.v_th_date_dim')
# #      .select('date_id', 'week_id')
# #      .drop_duplicates()
# #     )
    
# #     txn_matching = txn_matching.join(date_id_week_id_mapping, 'date_id')
    
#     #----- create test / resereved txn 
#     test_txn_matching = txn_matching.join(test_store_list,'store_id','inner')
#     rs_txn_matching = txn_matching.join(rs_store_list,'store_id','inner')

#     #---- get weekly feature by store
#     test_wk_matching = test_txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
#     rs_wk_matching = rs_txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))
#     all_store_wk_matching = txn_matching.groupBy('store_id','store_region').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales'))

#     #convert to pandas
#     test_df = to_pandas(test_wk_matching).fillna(0)
#     rs_df = to_pandas(rs_wk_matching).fillna(0)
#     all_store_df = to_pandas(all_store_wk_matching).fillna(0)
    
#     #get matching features
#     f_matching = all_store_df.columns[3:]

#     #using Standardscaler to scale features first
#     ss = StandardScaler()
#     ss.fit(all_store_df[f_matching])
#     test_ss = ss.transform(test_df[f_matching])
#     reserved_ss = ss.transform(rs_df[f_matching])

#     #setup dict to collect info
#     dist_dict = {}
#     var_dict = {}
#     cos_dict = {}

#     #for each test store
#     for i in range(len(test_ss)):

#         #set standard euc_distance
#         dist0 = 10**9
#         #set standard var
#         var0 = 100
#         #set standard cosine
#         cos0 = -1

#         #finding its region & store_id
#         test_region = test_df.iloc[i].store_region
#         test_store_id = test_df.iloc[i].store_id
#         #get value from that test store
#         test = test_ss[i]

#         #get index for reserved store
#         ctr_index = rs_df[rs_df['store_region'] == test_region].index

#         #loop in that region
#         for j in ctr_index:
#             #get value of res store & its store_id
#             res = reserved_ss[j]
#             res_store_id = rs_df.iloc[j].store_id

#     #-----------------------------------------------------------------------------
#             #finding min distance
#             dist = distance.euclidean(test,res)
#             if dist < dist0:
#                 dist0 = dist
#                 dist_dict[test_store_id] = [res_store_id,dist]

#     #-----------------------------------------------------------------------------            
#             #finding min var
#             var = stats.variance(np.abs(test-res))
#             if var < var0:
#                 var0 = var
#                 var_dict[test_store_id] = [res_store_id,var]

#     #-----------------------------------------------------------------------------  
#             #finding highest cos
#             cos = cosine_similarity(test.reshape(1,-1),res.reshape(1,-1))[0][0]
#             if cos > cos0:
#                 cos0 = cos
#                 cos_dict[test_store_id] = [res_store_id,cos]
    
#     #---- create dataframe            
#     dist_df = pd.DataFrame(dist_dict,index=['ctr_store_dist','euc_dist']).T.reset_index().rename(columns={'index':'store_id'})
#     var_df = pd.DataFrame(var_dict,index=['ctr_store_var','var']).T.reset_index().rename(columns={'index':'store_id'})
#     cos_df = pd.DataFrame(cos_dict,index=['ctr_store_cos','cos']).T.reset_index().rename(columns={'index':'store_id'})
    
#     #join to have ctr store by each method
#     matching_df = test_df[['store_id','store_region']].merge(dist_df[['store_id','ctr_store_dist']],on='store_id',how='left')\
#                                                   .merge(var_df[['store_id','ctr_store_var']],on='store_id',how='left')\
#                                                   .merge(cos_df[['store_id','ctr_store_cos']],on='store_id',how='left')

#     #change data type to int
#     matching_df.ctr_store_dist = matching_df.ctr_store_dist.astype('int')
#     matching_df.ctr_store_var = matching_df.ctr_store_var.astype('int')
#     matching_df.ctr_store_cos = matching_df.ctr_store_cos.astype('int')
    
#     #----select control store using var method
#     if matching_methodology == 'varience':
#         ctr_store_list = list(set([s for s in matching_df.ctr_store_var]))
#         return ctr_store_list, matching_df
#     elif matching_methodology == 'euclidean':
#         ctr_store_list = list(set([s for s in matching_df.ctr_store_dist]))
#         return ctr_store_list, matching_df
#     elif matching_methodology == 'cosine_similarity':
#         ctr_store_list = list(set([s for s in matching_df.ctr_store_cos]))
#         return ctr_store_list, matching_df
#     else:
#         print('Matching metodology not in scope list : varience, euclidean, cosine_similarity')
#         return None

# COMMAND ----------

# MAGIC %md ##customer funnel

# COMMAND ----------

def cust_funnel(txn, format_group_name, start_date, end_date, 
                sel_dep, sel_sec, sel_class, sel_subcl, 
                sel_brand, sel_sku,
                switching_lv):
    """Customer funnel 
    OFFLINE ONLY
    """
    
    import pandas as pd
    
    print('-'*50)
    print('Customer Funnel v.2')
    print('Only "OFFLINE" channel')    
    print(f'Brand vs Multi - Class / Subclass, based on feature skus and switching level : {switching_lv}')
    
    if switching_lv.lower() == 'subclass':
        brand_scope_columns = ['division_id', 'department_id', 'section_id', 'class_id', 'subclass_id']
    else:
        brand_scope_columns = ['division_id', 'department_id', 'section_id', 'class_id']
        
    features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    features_sku_sf = spark.createDataFrame(features_sku_df)
    features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select(brand_scope_columns).drop_duplicates()
    sel_hierarchy_df = \
    (spark
     .table(TBL_PROD)
     .join(features_sec_class_subcl, brand_scope_columns)
     .select('division_id', 'division_name', 'department_id', 'department_name', 
             'section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
     .drop_duplicates()
     .orderBy('division_id', 'division_name', 'department_id', 'department_name', 
              'section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    )
    sel_hierarchy_df.show(truncate=False)
    print('-'*50)
    
    sel_div = list(sel_hierarchy_df.select('division_name').drop_duplicates().toPandas()['division_name'])
    sel_dep = list(sel_hierarchy_df.select('department_name').drop_duplicates().toPandas()['department_name'])
    sel_sec = list(sel_hierarchy_df.select('section_name').drop_duplicates().toPandas()['section_name'])
    sel_class = list(sel_hierarchy_df.select('class_name').drop_duplicates().toPandas()['class_name'])
    sel_subcl = list(sel_hierarchy_df.select('subclass_name').drop_duplicates().toPandas()['subclass_name'])
    
    # Change filter to offline_online_other_channel - Dec 2022 - Ta
    prior_pre_cc_txn = \
    (txn
     .filter(F.col('store_format_group')==format_group_name)
     .filter(F.col('offline_online_other_channel')=='OFFLINE')     
     .filter(F.col('date_id').between(start_date, end_date))
     .filter(F.col('household_id').isNotNull())
     .select('household_id', 'division_name', 'department_name', 
             'section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name', 
             'brand_name', 'upc_id')
     .drop_duplicates()
    )
    
    prior_pre_cc_txn.persist()
    
    total_store_cust_count = prior_pre_cc_txn.agg(F.countDistinct('household_id')).collect()[0][0]
    division_cust_count = prior_pre_cc_txn.join(sel_hierarchy_df, 'division_name', 'inner').agg(F.countDistinct('household_id')).collect()[0][0]
    
    dept_cust_count = \
    (prior_pre_cc_txn.join(sel_hierarchy_df, ['division_name', 'department_name'], 'inner')
     .agg(F.countDistinct('household_id'))).collect()[0][0]

    section_cust_count = \
    (prior_pre_cc_txn.join(sel_hierarchy_df, ['section_id'], 'inner')
     .agg(F.countDistinct('household_id'))).collect()[0][0]
    
    class_cust_count = \
    (prior_pre_cc_txn.join(sel_hierarchy_df, ['section_id', 'class_id'], 'inner')
     .agg(F.countDistinct('household_id'))).collect()[0][0]
    
    subclass_cust_count = \
    (prior_pre_cc_txn.join(sel_hierarchy_df, ['section_id', 'class_id', 'subclass_id'], 'inner')
     .agg(F.countDistinct('household_id'))).collect()[0][0]
    
    if switching_lv.lower() == 'subclass':
        brand_cust_count = \
        (prior_pre_cc_txn
         .join(sel_hierarchy_df, ['section_id', 'class_id', 'subclass_id'], 'inner')
         .filter(F.col('brand_name')==sel_brand)
         .agg(F.countDistinct('household_id'))
        ).collect()[0][0]
    else:
        brand_cust_count = \
        (prior_pre_cc_txn
         .join(sel_hierarchy_df, ['section_id', 'class_id'], 'inner')
         .filter(F.col('brand_name')==sel_brand)
         .agg(F.countDistinct('household_id'))
        ).collect()[0][0]
    
    sku_cust_count = prior_pre_cc_txn.join(features_sku_sf, 'upc_id').agg(F.countDistinct('household_id')).collect()[0][0]
    
    prior_pre_cc_txn.unpersist()
    if switching_lv.lower() == 'subclass':
        df = pd.DataFrame({'hierarchy_level' : ['total_store', 'division', 'department', 'section', 'class', 'subclass', 'brand', 'sku'],
                       'hierarchy_detail' : ['key7divisions', str(sel_div), str(sel_dep), str(sel_sec), str(sel_class), 
                                             str(sel_subcl), str(sel_brand), str(sel_sku)],
                       'customer' : [total_store_cust_count, division_cust_count, dept_cust_count, 
                                     section_cust_count,class_cust_count,subclass_cust_count,
                                     brand_cust_count, sku_cust_count]})
    else:
        df = pd.DataFrame({'hierarchy_level' : ['total_store', 'division', 'department', 'section', 'class', 'brand', 'sku'],
                       'hierarchy_detail' : ['key7divisions', str(sel_div), str(sel_dep), str(sel_sec), str(sel_class), 
                                             str(sel_brand), str(sel_sku)],
                       'customer' : [total_store_cust_count, division_cust_count, dept_cust_count, 
                                     section_cust_count,class_cust_count,
                                     brand_cust_count, sku_cust_count]})
            
    df_pen = \
    (pd.concat([df, df[['hierarchy_detail', 'customer']].shift(-1).rename(columns={'hierarchy_detail':'lead_row_hierarchy', 
                                                                                   'customer':'lead_row_customer'})], 
               axis=1)
     .assign(lead_row_penetration = lambda x : x['lead_row_customer']/x['customer'])
    )
    
    return df_pen

# COMMAND ----------

# MAGIC %md ##customer share

# COMMAND ----------

def cust_share(txn, format_group_name, 
               prior_start_date, pre_end_date,
               cp_start_date, cp_end_date, 
               ctr_store_list, test_store_sf,
               sel_dep, sel_sec, sel_class, 
               sel_subcl, sel_brand, sel_sku):
    """Customer share dur period test vs control store
    OFFLINE ONLY
    """
    print('Customer share for "OFFLINE" only')
    # Change filter to offline_online_other_channel - Dec 2022 - Ta
    cc_class_txn = \
    (txn
     .filter(F.col('date_id').between(prior_start_date, cp_end_date))
     .filter(F.col('store_format_group')==format_group_name)
     .filter(F.col('offline_online_other_channel')=='OFFLINE')
     .filter(F.col('household_id').isNotNull())
     .filter(F.col('department_name')==sel_dep)
     .filter(F.col('section_name')==sel_sec)
     .filter(F.col('class_name')==sel_class)
    )
    
    test_txn = cc_class_txn.join(test_store_sf.select('store_id'), 'store_id').withColumn('test_ctrl_flag', F.lit('test'))
    ctrl_txn = cc_class_txn.filter(F.col('store_id').isin(ctr_store_list)).withColumn('test_ctrl_flag', F.lit('ctrl'))
    
    test_ctrl_txn = test_txn.unionByName(ctrl_txn)
    
    result = \
    (test_ctrl_txn
     .withColumn('period_flag', F.when( F.col('date_id')<=pre_end_date, F.lit('pre') ).otherwise(F.lit('dur')) )
     .withColumn('subclass_hh_id', F.when(F.col('subclass_name')==sel_subcl, F.col('household_id')).otherwise(None) )
     .withColumn('brand_hh_id', F.when( (F.col('subclass_name')==sel_subcl) & (F.col('brand_name')==sel_brand),
                                          F.col('household_id')).otherwise(None) )
                 
     .groupby('test_ctrl_flag', 'period_flag')
     .agg(
         #F.countDistinct('week_id').alias('n_week_sold'),
          F.countDistinct('store_id').alias('n_store_sold'),
          F.countDistinct('household_id').alias('class_customers'),
          F.countDistinct('subclass_hh_id').alias('subclass_customers'),
          F.countDistinct('brand_hh_id').alias('brand_customers'))
     .withColumn('brand_pen_class', F.col('brand_customers')/F.col('class_customers'))
     .withColumn('brand_pen_subcl', F.col('brand_customers')/F.col('subclass_customers'))
    )
    
    result_df = to_pandas(result)
    
    return result_df

# COMMAND ----------

# MAGIC %md #customer movement & switching

# COMMAND ----------

# MAGIC %md ## customer movement

# COMMAND ----------

# Comment out entire function as superseded by function in utils_2
# def cust_movement(switching_lv, 
#                   txn, cp_start_date, cp_end_date, prior_start_date, prior_end_date, pre_start_date, pre_end_date,
#                   test_store_sf, adj_prod_id, 
#                   sel_class, sel_subcl, sel_brand, 
#                   feat_list):
#     """Media evaluation solution, customer movement and switching
#     """
#     spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    
#     print('Customer movement for "OFFLINE" + "ONLINE"')
#     #---- From Exposed customer, find Existing and New SKU buyer (movement at micro level)
#     dur_campaign_exposed_cust = \
#     (txn
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('household_id').isNotNull())
#      .join(test_store_sf, 'store_id', 'inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('first_exposed_date'))
#     )
    
#     dur_campaign_sku_shopper = \
#     (txn
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('household_id').isNotNull())
#      .filter(F.col('upc_id').isin(sel_sku))
# #      .filter(F.col('brand_name')== sel_brand)
# #      .filter(F.col('class_name')== sel_class)
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('first_sku_buy_date'))
#      .drop_duplicates()
#     )
    
#     dur_campaign_exposed_cust_and_sku_shopper = \
#     (dur_campaign_exposed_cust
#      .join(dur_campaign_sku_shopper, 'household_id', 'inner')
#      .filter(F.col('first_exposed_date').isNotNull())
#      .filter(F.col('first_sku_buy_date').isNotNull())
#      .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
#      .select('household_id')
#     )
#     n = dur_campaign_exposed_cust_and_sku_shopper.count()
#     print(f'Total exposed customer and sku shopper {n}')
    
#     prior_pre_sku_shopper = \
#     (txn.filter(F.col('date_id').between(prior_start_date, pre_end_date))
#      .filter(F.col('household_id').isNotNull())
#      .filter(F.col('upc_id').isin(sel_sku))
#      .select('household_id')
#      .drop_duplicates()
#     )
    
#     existing_exposed_cust_and_sku_shopper = \
#     (dur_campaign_exposed_cust_and_sku_shopper
#      .join(prior_pre_sku_shopper, 'household_id', 'inner')
#      .withColumn('customer_macro_flag', F.lit('existing'))
#      .withColumn('customer_micro_flag', F.lit('existing_sku'))
#     )
#     existing_exposed_cust_and_sku_shopper = existing_exposed_cust_and_sku_shopper.checkpoint()
    
#     new_exposed_cust_and_sku_shopper = \
#     (dur_campaign_exposed_cust_and_sku_shopper
#      .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
#      .withColumn('customer_macro_flag', F.lit('new'))
#     )
#     new_exposed_cust_and_sku_shopper = new_exposed_cust_and_sku_shopper.checkpoint()
        
#     #---- Movement macro level for New to SKU
#     prior_pre_cc_txn = txn.filter(F.col('household_id').isNotNull()).filter(F.col('date_id').between(prior_start_date, pre_end_date))
    
#     prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()
#     prior_pre_class_shopper = prior_pre_cc_txn.filter(F.col('class_name')==sel_class).select('household_id').drop_duplicates()
#     prior_pre_subclass_shopper = \
#     (prior_pre_cc_txn
#      .filter( F.col('class_name')==sel_class )
#      .filter( F.col('subclass_name')==sel_subcl )
#      .select('household_id')
#     ).drop_duplicates()
    
#     prior_pre_brand_shopper = \
#     (prior_pre_cc_txn
#      .filter(F.col('class_name')==sel_class)
#      .filter(F.col('subclass_name')==sel_subcl)
#      .filter(F.col('brand_name')==sel_brand)
#      .select('household_id')
#     ).drop_duplicates()
    
#     #---- Grouping, flag customer macro flag
#     new_sku_new_store = \
#     (new_exposed_cust_and_sku_shopper
#      .join(prior_pre_store_shopper, 'household_id', 'leftanti')
#      .select('household_id', 'customer_macro_flag')
#      .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
#     )
    
#     new_sku_new_class = \
#     (new_exposed_cust_and_sku_shopper
#      .join(prior_pre_store_shopper, 'household_id', 'inner')
#      .join(prior_pre_class_shopper, 'household_id', 'leftanti')
#      .select('household_id', 'customer_macro_flag')
#      .withColumn('customer_micro_flag', F.lit('new_to_class'))
#     )
    
#     if switching_lv == 'subclass':
#         new_sku_new_subclass = \
#         (new_exposed_cust_and_sku_shopper
#          .join(prior_pre_store_shopper, 'household_id', 'inner')
#          .join(prior_pre_class_shopper, 'household_id', 'inner')
#          .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
#          .select('household_id', 'customer_macro_flag')
#          .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
#         )
        
#         #---- Current subclass shopper , new to brand : brand switcher within sublass
#         new_sku_new_brand_shopper = \
#         (new_exposed_cust_and_sku_shopper
#          .join(prior_pre_store_shopper, 'household_id', 'inner')
#          .join(prior_pre_class_shopper, 'household_id', 'inner')
#          .join(prior_pre_subclass_shopper, 'household_id', 'inner')
#          .join(prior_pre_brand_shopper, 'household_id', 'leftanti')
#          .select('household_id', 'customer_macro_flag')
#          .withColumn('customer_micro_flag', F.lit('new_to_brand'))
#         )
        
#         new_sku_within_brand_shopper = \
#         (new_exposed_cust_and_sku_shopper
#          .join(prior_pre_store_shopper, 'household_id', 'inner')
#          .join(prior_pre_class_shopper, 'household_id', 'inner')
#          .join(prior_pre_subclass_shopper, 'household_id', 'inner')
#          .join(prior_pre_brand_shopper, 'household_id', 'inner')
#          .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
#          .select('household_id', 'customer_macro_flag')
#          .withColumn('customer_micro_flag', F.lit('new_to_sku'))
#         )
        
#         result_movement = \
#         (existing_exposed_cust_and_sku_shopper
#          .unionByName(new_sku_new_store)
#          .unionByName(new_sku_new_class)
#          .unionByName(new_sku_new_subclass)
#          .unionByName(new_sku_new_brand_shopper)
#          .unionByName(new_sku_within_brand_shopper)
#         )
#         result_movement = result_movement.checkpoint()
        
#         return result_movement, new_exposed_cust_and_sku_shopper

#     elif switching_lv == 'class':
#         #---- Current subclass shopper , new to brand : brand switcher within class
#         new_sku_new_brand_shopper = \
#         (new_exposed_cust_and_sku_shopper
#          .join(prior_pre_store_shopper, 'household_id', 'inner')
#          .join(prior_pre_class_shopper, 'household_id', 'inner')
#          .join(prior_pre_brand_shopper, 'household_id', 'leftanti')
#          .select('household_id', 'customer_macro_flag')
#          .withColumn('customer_micro_flag', F.lit('new_to_brand'))
#         )
        
#         new_sku_within_brand_shopper = \
#         (new_exposed_cust_and_sku_shopper
#          .join(prior_pre_store_shopper, 'household_id', 'inner')
#          .join(prior_pre_class_shopper, 'household_id', 'inner')
#          .join(prior_pre_brand_shopper, 'household_id', 'inner')
#          .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
#          .select('household_id', 'customer_macro_flag')
#          .withColumn('customer_micro_flag', F.lit('new_to_sku'))
#         )
        
#         result_movement = \
#         (existing_exposed_cust_and_sku_shopper
#          .unionByName(new_sku_new_store)
#          .unionByName(new_sku_new_class)
#          .unionByName(new_sku_new_brand_shopper)
#          .unionByName(new_sku_within_brand_shopper)
#         )
#         result_movement = result_movement.checkpoint()
#         return result_movement, new_exposed_cust_and_sku_shopper

#     else:
#         print('Not recognized Movement and Switching level param')
#         return None

# COMMAND ----------

# Comment out entire function as superseded by function in utils_2
# def cust_switching(switching_lv, cust_movement_sf,
#                   txn, cp_start_date, cp_end_date, prior_start_date, prior_end_date, pre_start_date, pre_end_date,
#                   ctr_store_list, test_store_sf, adj_prod_id, 
#                   sel_sec, sel_class, sel_subcl, sel_brand, sel_sku):
    
#     from typing import List
#     from pyspark.sql import DataFrame as SparkDataFrame
    
#     """Media evaluation solution, customer switching
#     """
#     spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
#     print('Customer switching for "OFFLINE" + "ONLINE"')
#     #---- switching fn from Sai
#     def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: sparkDataFrame, prod_trans: SparkDataFrame, grp: List, 
#                    prod_lev: str, full_prod_lev: str , col_rename: str, period: str) -> SparkDataFrame:
#         """Customer switching from Sai
#         """
#         print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        
#         # List of customer movement at analysis micro level
#         cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
# #         print('cust_micro_df')
# #         cust_micro_df.display()
#         # Transaction of focus customer group
#         prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
#                                                 , on='household_id', how='inner')
#         cust_micro_kpi_prod_lv = \
#         (prod_trans_cust_micro
#          .groupby(grp)
#          .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
#               F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
#          .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
#         )
# #         print('cust_mirco_kpi_prod_lv')
# #         cust_micro_kpi_prod_lv.display()
#         # total of other brand/subclass/class spending
#         total_oth = \
#         (cust_micro_kpi_prod_lv
#          .select(F.sum('oth_'+prod_lev+'_spend').alias('total_oth_'+prod_lev+'_spend'))
#          .toPandas().fillna(0)
#         )['total_oth_'+prod_lev+'_spend'][0]
        
#         cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(int(total_oth)))
        
# #         print('cust_micro_kpi_prod_lv')
# #         cust_micro_kpi_prod_lv.display()
        
#         print("\t\t\t\t\t\t**Running micro df2")

# #         print('cust_micro_df2')
# #         cust_micro_df2.display()

#         # Join micro df with prod trans
#         if (prod_lev == 'brand') & (switching_lv == 'subclass'):
#             cust_micro_df2 = \
#             (cust_micro_df
#              .groupby('division_name','department_name','section_name','class_name','subclass_name',
#                       F.col('brand_name').alias('original_brand'),
#                       'customer_macro_flag','customer_micro_flag')
#              .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
#                   F.countDistinct('household_id').alias('total_ori_brand_cust'))
#             )
#             micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='subclass_name', how='inner')
            
#         elif (prod_lev == 'brand') & (switching_lv == 'class'):
#             cust_micro_df2 = \
#             (cust_micro_df
#              .groupby('division_name','department_name','section_name','class_name',
#                       F.col('brand_name').alias('original_brand'),
#                       'customer_macro_flag','customer_micro_flag')
#              .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
#                   F.countDistinct('household_id').alias('total_ori_brand_cust'))
#             )            
#             micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')
            
#         elif prod_lev == 'class':
#             micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        
#         print("\t\t\t\t\t\t**Running Summary of micro df")
#         switching_result = \
#         (micro_df_summ
#          .select('division_name','department_name','section_name','class_name','original_brand',
#                  'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
#                  'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
#          .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
#          .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
#          .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(), F.col('pct_spend_oth_'+full_prod_lev).desc())
#         )
#         # display(micro_df_summ)
#         switching_result = switching_result.checkpoint()
        
#         return switching_result
    
#     #---- Main
#     if switching_lv == 'subclass':
#         #---- Create subclass-brand spend by cust movement
#         prior_pre_cc_txn_subcl = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subcl)
#         )
#         prior_pre_cc_txn_subcl_sel_brand = prior_pre_cc_txn_subcl.filter(F.col('brand_name')==sel_brand)
#         prior_pre_subcl_sel_brand_kpi = \
#         (prior_pre_cc_txn_subcl_sel_brand
#          .groupBy('division_name','department_name','section_name',
#                   'class_name','subclass_name','brand_name','household_id')
#          .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
#         )
#         dur_cc_txn_subcl = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('section_name')==sel_sec)         
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subcl)
#         )
#         dur_cc_txn_subcl_sel_brand = dur_cc_txn_subcl.filter(F.col('brand_name')==sel_brand)
#         dur_subcl_sel_brand_kpi = \
#         (dur_cc_txn_subcl_sel_brand
#          .groupBy('division_name','department_name','section_name',
#                   'class_name','subclass_name','brand_name', 'household_id')
#          .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
#         )
#         pre_dur_band_spend = \
#         (prior_pre_subcl_sel_brand_kpi
#          .join(dur_subcl_sel_brand_kpi, ['division_name','department_name','section_name',
#                   'class_name','subclass_name','brand_name','household_id'], 'outer')
#         )
        
#         cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
#         new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_subcl, 
#                                                  ['subclass_name', 'brand_name'], 
#                                                  'brand', 'brand_in_subclass', 'brand_name', 'dur')
#         # Brand penetration within subclass
#         subcl_cust = dur_cc_txn_subcl.agg(F.countDistinct('household_id')).collect()[0][0]
#         brand_cust_pen = \
#         (dur_cc_txn_subcl
#          .groupBy('brand_name')
#          .agg(F.countDistinct('household_id').alias('brand_cust'))
#          .withColumn('subcl_cust', F.lit(subcl_cust))
#          .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('subcl_cust'))
#         )
        
#         return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen
    
#     elif switching_lv == 'class':
#         #---- Create class-brand spend by cust movment
#         prior_pre_cc_txn_class = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, pre_end_date))
#          .filter(F.col('section_name')==sel_sec)         
#          .filter(F.col('class_name')==sel_class)
#         )
#         prior_pre_cc_txn_class_sel_brand = prior_pre_cc_txn_class.filter(F.col('brand_name')==sel_brand)
#         prior_pre_class_sel_brand_kpi = \
#         (prior_pre_cc_txn_class_sel_brand
#          .groupBy('division_name','department_name','section_name',
#                   'class_name','brand_name','household_id')
#          .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
#         )
#         dur_cc_txn_class = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#          .filter(F.col('section_name')==sel_sec)         
#          .filter(F.col('class_name')==sel_class)
#         )
#         dur_cc_txn_class_sel_brand = dur_cc_txn_class.filter(F.col('brand_name')==sel_brand)
#         dur_class_sel_brand_kpi = \
#         (dur_cc_txn_class_sel_brand
#          .groupBy('division_name','department_name','section_name',
#                   'class_name','brand_name','household_id')
#          .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
#         )
#         pre_dur_band_spend = \
#         (prior_pre_class_sel_brand_kpi
#          .join(dur_class_sel_brand_kpi, ['division_name','department_name','section_name',
#                   'class_name','brand_name','household_id'], 'outer')
#         )
        
#         cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
#         new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand', cust_movement_pre_dur_spend, prior_pre_cc_txn_class,
#                                                  ['class_name', 'brand_name'], 
#                                                  'brand', 'brand_in_class', 'brand_name', 'dur')
#         # Brand penetration within subclass
#         cl_cust = dur_cc_txn_class.agg(F.countDistinct('household_id')).collect()[0][0]
#         brand_cust_pen = \
#         (dur_cc_txn_class
#          .groupBy('brand_name')
#          .agg(F.countDistinct('household_id').alias('brand_cust'))
#          .withColumn('class_cust', F.lit(cl_cust))
#          .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('class_cust'))
#         )

#         return new_to_brand_switcching_from, cust_movement_pre_dur_spend, brand_cust_pen
#     else:
#         print('Unrecognized switching level')
#         return None

# COMMAND ----------

# MAGIC %md ##SKU switching

# COMMAND ----------

# Comment out entire function as superseded by utils_2
# def cust_sku_switching(switching_lv,
#                        txn, cp_start_date, cp_end_date, prior_start_date, prior_end_date, pre_start_date, pre_end_date,
#                        ctr_store_list, test_store_sf, adj_prod_id, 
#                        sel_sec, sel_class, sel_subcl, sel_brand, sel_sku):
    
#     from typing import List
#     from pyspark.sql import DataFrame as SparkDataFrame
    
#     """Media evaluation solution, customer switching
#     """
#     spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
#     prod_desc = spark.table(TBL_PROD).select('upc_id', 'product_en_desc').drop_duplicates()
    
#     print('Customer switching SKU for "OFFLINE" + "ONLINE"')
#     #---- From Exposed customer, find Existing and New SKU buyer (movement at micro level)
#     dur_campaign_exposed_cust = \
#     (txn
#      .filter(F.col('household_id').isNotNull())
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .join(test_store_sf, 'store_id', 'inner')
#      .join(adj_prod_id, 'upc_id', 'inner')
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('first_exposed_date'))
#     )
    
#     dur_campaign_sku_shopper = \
#     (txn
#      .filter(F.col('household_id').isNotNull())
#      .filter(F.col('date_id').between(cp_start_date, cp_end_date))
#      .filter(F.col('section_name')==sel_sec)
#      .filter(F.col('class_name')==sel_class)
#      .filter(F.col('brand_name')==sel_brand)
#      .filter(F.col('upc_id').isin(sel_sku))
#      .groupBy('household_id')
#      .agg(F.min('date_id').alias('first_sku_buy_date'))
#      .drop_duplicates()
#     )
    
#     dur_campaign_exposed_cust_and_sku_shopper = \
#     (dur_campaign_exposed_cust
#      .join(dur_campaign_sku_shopper, 'household_id', 'inner')
#      .filter(F.col('first_exposed_date').isNotNull())
#      .filter(F.col('first_sku_buy_date').isNotNull())
#      .filter(F.col('first_exposed_date') <= F.col('first_sku_buy_date'))
#      .select('household_id')
#     )
    
#     #----
    
#     if switching_lv == 'subclass':
#         #---- Create subclass-brand spend by cust movement / offline+online
#         txn_sel_subcl_cc = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .filter(F.col('subclass_name')==sel_subcl)
#          .withColumn('pre_subcl_sales', F.when( F.col('date_id')<=pre_end_date, F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('dur_subcl_sales', F.when( F.col('date_id')>=cp_start_date, F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('cust_tt_pre_subcl_sales', F.sum(F.col('pre_subcl_sales')).over(Window.partitionBy('household_id') ))
#          .withColumn('cust_tt_dur_subcl_sales', F.sum(F.col('dur_subcl_sales')).over(Window.partitionBy('household_id') ))    
#         )
        
#         txn_cust_both_period_subcl = txn_sel_subcl_cc.filter( (F.col('cust_tt_pre_subcl_sales')>0) & (F.col('cust_tt_dur_subcl_sales')>0) )
        
#         txn_cust_both_period_subcl_not_pre_but_dur_sku = \
#         (txn_cust_both_period_subcl
#          .withColumn('pre_sku_sales', 
#                      F.when( (F.col('date_id')<=pre_end_date) & (F.col('upc_id').isin(sel_sku)), F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('dur_sku_sales', 
#                      F.when( (F.col('date_id')>=cp_start_date) & (F.col('upc_id').isin(sel_sku)), F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
#          .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
#          .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
#         )
#         n_cust_both_subcl_switch_sku = \
#         (txn_cust_both_period_subcl_not_pre_but_dur_sku
#          .filter(F.col('pre_subcl_sales')>0) # only other products
#          .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
#          .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
#          .join(prod_desc, 'upc_id', 'left')
#          .orderBy('custs', ascending=False)
#         )
        
#         return n_cust_both_subcl_switch_sku
        
#     elif switching_lv == 'class':
#         #---- Create subclass-brand spend by cust movement / offline+online
#         txn_sel_cl_cc = \
#         (txn
#          .filter(F.col('household_id').isNotNull())
#          .filter(F.col('date_id').between(prior_start_date, cp_end_date))
#          .filter(F.col('section_name')==sel_sec)
#          .filter(F.col('class_name')==sel_class)
#          .withColumn('pre_cl_sales', F.when( F.col('date_id')<=pre_end_date, F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('dur_cl_sales', F.when( F.col('date_id')>=cp_start_date, F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('cust_tt_pre_cl_sales', F.sum(F.col('pre_cl_sales')).over(Window.partitionBy('household_id') ))
#          .withColumn('cust_tt_dur_cl_sales', F.sum(F.col('dur_cl_sales')).over(Window.partitionBy('household_id') ))    
#         )
        
#         txn_cust_both_period_cl = txn_sel_cl_cc.filter( (F.col('cust_tt_pre_cl_sales')>0) & (F.col('cust_tt_dur_cl_sales')>0) )
        
#         txn_cust_both_period_cl_not_pre_but_dur_sku = \
#         (txn_cust_both_period_cl
#          .withColumn('pre_sku_sales', 
#                      F.when( (F.col('date_id')<=pre_end_date) & (F.col('upc_id').isin(sel_sku)), F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('dur_sku_sales', 
#                      F.when( (F.col('date_id')>=cp_start_date) & (F.col('upc_id').isin(sel_sku)), F.col('net_spend_amt') ).otherwise(0) )
#          .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
#          .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
#          .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
#         )
#         n_cust_both_cl_switch_sku = \
#         (txn_cust_both_period_cl_not_pre_but_dur_sku
#          .filter(F.col('pre_cl_sales')>0)
#          .join(dur_campaign_exposed_cust_and_sku_shopper, 'household_id', 'inner')
#          .groupBy('upc_id').agg(F.countDistinct('household_id').alias('custs'))
#          .join(prod_desc, 'upc_id', 'left')         
#          .orderBy('custs', ascending=False)
#         )
        
#         return n_cust_both_cl_switch_sku
        
#     else:
#         print('Unrecognized switching level')
#         return None

# COMMAND ----------

# MAGIC %md ## Category of new to category customer info

# COMMAND ----------

# tdm_seg.media_camp_eval_2022_0124_m01e_cust_mv
# tdm_seg.media_campaign_eval_txn_data_2022_0124_m01e
## Need 2 input tables from prior step => cust_mv, txn_all

#new_to_class
#new_to_subclass

def get_new_to_cate( txn, cust_mv, week_type):

    ## get new to category customers group
    
    c_new_cate   = cust_mv.where (cust_mv.customer_micro_flag.isin('new_to_class','new_to_subclass'))\
                          .select(cust_mv.household_id.alias('cust_id'))
    
    ## get total #new to cate customers
    
    cust_new_cnt = c_new_cate.agg(sum(lit(1)).alias('cust_cnt')).collect()[0].cust_cnt
    
    ## Get customers behavior in ppp + pre period
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_fis_wk'
        week_col   = 'week_id'        
    elif week_type == 'promo_wk':
        period_col = 'period_promo_wk'
        week_col   = 'promoweek_id'
    elif week_type == 'promo_mv_wk':
        period_col = 'period_promo_mv_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## Prep Transaction
    txn_pre_cond = """ ({0} in ('pre', 'ppp') ) """.format(period_col)
    
    txn_all_pcst = txn.where(txn_pre_cond)\
                      .join (c_new_cate, [txn.household_id == c_new_cate.cust_id], 'left_semi')\
                      .persist()
    
    ## Prep product table
    
    prd = sqlContext.table('tdm.v_prod_dim_c').select( F.col('upc_id')
                                                      ,F.col('subclass_code').alias('sclass_cd')
                                                      ,F.col('division_name').alias('div_nm')
                                                      ,F.col('department_name').alias('dept_nm')
                                                      ,F.col('section_name').alias('sec_nm')
                                                      ,F.col('class_name').alias('class_nm')
                                                      ,F.col('subclass_name').alias('sclass_nm')
                                                      ,F.col('brand_name').alias('brand_name')
                                                      ,F.col('product_en_desc').alias('product_name_en')
                                                      ,F.col('product_desc').alias('product_name_th')
                                                     )
                                                     
    ## prod subclass
    prd_sc = prd.select( prd.sclass_cd
                        ,prd.div_nm
                        ,prd.dept_nm
                        ,prd.sec_nm
                        ,prd.class_nm
                        ,prd.sclass_nm
                       )\
                .dropDuplicates()

    ## get category bought
    
    cate_cust_from = txn_all_pcst.groupBy( txn_all_pcst.subclass_code )\
                                 .agg    ( countDistinct(txn_all_pcst.household_id).alias('cate_cust_cnt')
                                          ,countDistinct(txn_all_pcst.transaction_uid).alias('cate_basket_cnt')
                                          ,sum(txn_all_pcst.net_spend_amt).alias('cate_sales')
                                         )
                                 
    ## use rank, in case same number of customer will get same rank, if want more the continue count in next order use dense_rank, 
    ## rank will be 1,1,3 but dense_rank will be 1,1,2
    cate_cust_from = cate_cust_from.withColumn('cate_order_bycust',F.rank().over(Window.orderBy(cate_cust_from.cate_cust_cnt.desc())))\
                                   .withColumn('cate_order_bycust_dense',F.dense_rank().over(Window.orderBy(cate_cust_from.cate_cust_cnt.desc())))\
                                   .withColumn('pct_cate_cust', F.col('cate_cust_cnt')/cust_new_cnt )\
                                   .withColumn('all_cust_new_cate_cnt', lit(cust_new_cnt))\
                                   .persist()
    
    cate_cust_from.printSchema()
    
    ## Category information with column re order
    
    cate_info_df   = cate_cust_from.join   (prd_sc, [cate_cust_from.subclass_code == prd.sclass_cd], 'inner')\
                                   .select ( cate_cust_from.subclass_code
                                            ,prd_sc.div_nm
                                            ,prd_sc.dept_nm
                                            ,prd_sc.sec_nm
                                            ,prd_sc.class_nm
                                            ,prd_sc.sclass_nm
                                            ,cate_cust_from.cate_order_bycust
                                            ,cate_cust_from.cate_order_bycust_dense
                                            ,cate_cust_from.all_cust_new_cate_cnt
                                            ,cate_cust_from.cate_cust_cnt
                                            ,cate_cust_from.pct_cate_cust
                                            ,cate_cust_from.cate_basket_cnt
                                            ,cate_cust_from.cate_sales
                                           )\
                                   .orderBy(cate_cust_from.cate_order_bycust)
    
    ## get top cate find brand bought
    
    top_5cate      = cate_cust_from.where ( cate_cust_from.cate_order_bycust <= 5)\
                                   .select( cate_cust_from.subclass_code
                                           ,cate_cust_from.cate_cust_cnt
                                          )
    
    txn_top5cate   = txn_all_pcst.join   (top_5cate, 'subclass_code', 'inner')\
                                 .groupBy( txn_all_pcst.subclass_code
                                          ,txn_all_pcst.brand_name
                                         )\
                                 .agg    ( countDistinct(txn_all_pcst.household_id).alias('brand_cust_cnt')
                                          ,(countDistinct(txn_all_pcst.household_id)/cust_new_cnt).alias('pct_to_allnew_cate_cust')
                                          ,(countDistinct(txn_all_pcst.household_id)/max(top_5cate.cate_cust_cnt)).alias('pct_to_cate_cust')
                                          ,countDistinct(txn_all_pcst.transaction_uid).alias('brand_bsk_cnt')
                                          ,sum(txn_all_pcst.net_spend_amt).alias('brand_sales')
                                         )
    
    txn_top5cate    = txn_top5cate.withColumn('brand_order', F.rank().over(Window.partitionBy('subclass_code').orderBy(F.col('brand_cust_cnt').desc())))\
                                  .withColumn('brand_order_dense', F.dense_rank().over(Window.partitionBy('subclass_code').orderBy(F.col('brand_cust_cnt').desc())))
                
    cate_brand_info = txn_top5cate.join    ( cate_info_df, [txn_top5cate.subclass_code == cate_info_df.subclass_code], 'inner')\
                                  .select  ( cate_info_df['*']
                                            ,txn_top5cate.brand_name
                                            ,txn_top5cate.brand_order
                                            ,txn_top5cate.brand_order_dense
                                            ,txn_top5cate.brand_cust_cnt
                                            ,txn_top5cate.pct_to_cate_cust
                                            ,txn_top5cate.pct_to_allnew_cate_cust
                                           )\
                                  .orderBy( cate_info_df.cate_order_bycust
                                           ,cate_info_df.subclass_code
                                           ,txn_top5cate.brand_order
                                           ,txn_top5cate.brand_name
                                          )
                                         
    ## return 2 df but can use only "cate_brand_info" with a complete information
    
    return cate_info_df, cate_brand_info

## end def


# COMMAND ----------

# MAGIC %md ## [With Lotuss cate pen] Category of New to Category customers

# COMMAND ----------

# tdm_seg.media_camp_eval_2022_0124_m01e_cust_mv
# tdm_seg.media_campaign_eval_txn_data_2022_0124_m01e
## Need 2 input tables from prior step => cust_mv, txn_all

#new_to_class
#new_to_subclass

def get_new_to_cate_wth_allpen( txn, cust_mv, week_type, store_fmt):

    ## get new to category customers group
    
    c_new_cate   = cust_mv.where (cust_mv.customer_micro_flag.isin('new_to_class','new_to_subclass'))\
                          .select(cust_mv.household_id.alias('cust_id'))
    
    ## get total #new to cate customers
    
    cust_new_cnt = c_new_cate.agg(sum(lit(1)).alias('cust_cnt')).collect()[0].cust_cnt
    
    ## Get customers behavior in ppp + pre period
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_fis_wk'
        week_col   = 'week_id'        
    elif week_type == 'promo_wk':
        period_col = 'period_promo_wk'
        week_col   = 'promoweek_id'
    elif week_type == 'promo_mv_wk':
        period_col = 'period_promo_mv_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## Prep Transaction >> MyLo pre-period + ppp any format
    txn_pre_cond    = """ ({0} in ('pre', 'ppp') ) and ( household_id is not null ) """.format(period_col)
    
    ## Add trans during condition > only media format
    
    txn_dur_cond    = """ ({0} in ('cmp') ) and 
                          ( lower(store_format_group) == '{1}' ) and 
                          ( household_id is not null )""".format(period_col, store_fmt.lower())
                          
    ## 12 Mar 2024 -- Add total lotuss level customers during period
    
    txn_all_dur     = txn.where(txn_dur_cond)
     ## Get total lotus customers during period
     
    lts_mylo_dur    = txn_all_dur.agg(countDistinct('household_id').alias('mylo_dur')).collect()[0].mylo_dur
    ##-----------------------------------------------------
    txn_all_pre     = txn.where(txn_pre_cond)
    
    txn_all_pcst    = txn_all_pre.join (c_new_cate, [txn.household_id == c_new_cate.cust_id], 'left_semi').persist()
        
    ## Prep product table
    
    prd = sqlContext.table('tdm.v_prod_dim_c').select( F.col('upc_id')
                                                      ,F.col('subclass_code').alias('sclass_cd')
                                                      ,F.col('division_name').alias('div_nm')
                                                      ,F.col('department_name').alias('dept_nm')
                                                      ,F.col('section_name').alias('sec_nm')
                                                      ,F.col('class_name').alias('class_nm')
                                                      ,F.col('subclass_name').alias('sclass_nm')
                                                      ,F.col('brand_name').alias('brand_name')
                                                      ,F.col('product_en_desc').alias('product_name_en')
                                                      ,F.col('product_desc').alias('product_name_th')
                                                     )
                                                     
    ## prod subclass
    prd_sc = prd.select( prd.sclass_cd
                        ,prd.div_nm
                        ,prd.dept_nm
                        ,prd.sec_nm
                        ,prd.class_nm
                        ,prd.sclass_nm
                       )\
                .dropDuplicates()

    ## get category bought
    
    cate_cust_from = txn_all_pcst.groupBy( txn_all_pcst.subclass_code )\
                                 .agg    ( countDistinct(txn_all_pcst.household_id).alias('cate_cust_cnt')
                                          ,countDistinct(txn_all_pcst.transaction_uid).alias('cate_basket_cnt')
                                          ,sum(txn_all_pcst.net_spend_amt).alias('cate_sales')
                                         )
                                 
    ## use rank, in case same number of customer will get same rank, if want more the continue count in next order use dense_rank, 
    ## rank will be 1,1,3 but dense_rank will be 1,1,2
    cate_cust_from = cate_cust_from.withColumn('cate_order_bycust',F.rank().over(Window.orderBy(cate_cust_from.cate_cust_cnt.desc())))\
                                   .withColumn('cate_order_bycust_dense',F.dense_rank().over(Window.orderBy(cate_cust_from.cate_cust_cnt.desc())))\
                                   .withColumn('pct_cate_cust', F.col('cate_cust_cnt')/cust_new_cnt )\
                                   .withColumn('all_cust_new_cate_cnt', lit(cust_new_cnt))\
                                   .persist()
    
    cate_cust_from.printSchema()
    
    ## Category information with column re order
    
    cate_info_df   = cate_cust_from.join   (prd_sc, [cate_cust_from.subclass_code == prd.sclass_cd], 'inner')\
                                   .select ( cate_cust_from.subclass_code
                                            ,prd_sc.div_nm
                                            ,prd_sc.dept_nm
                                            ,prd_sc.sec_nm
                                            ,prd_sc.class_nm
                                            ,prd_sc.sclass_nm
                                            ,cate_cust_from.cate_order_bycust
                                            ,cate_cust_from.cate_order_bycust_dense
                                            ,cate_cust_from.all_cust_new_cate_cnt
                                            ,cate_cust_from.cate_cust_cnt
                                            ,cate_cust_from.pct_cate_cust
                                            ,cate_cust_from.cate_basket_cnt
                                            ,cate_cust_from.cate_sales
                                           )\
                                   .orderBy(cate_cust_from.cate_order_bycust)
    
    ## get top cate find brand bought
    ## 12 Mar 2024 : Change to Top 10 cate (from top 5) get more information
    
    top_cate       = cate_info_df.where ( cate_info_df.cate_order_bycust <= 10)\
                                 .select( cate_info_df.subclass_code
                                         ,cate_info_df.cate_cust_cnt
                                         ,cate_info_df.cate_order_bycust
                                         ,cate_info_df.pct_cate_cust
                                        )
                                          
    ## add step get #customers in top category                                      
    top_cate_pent  = txn_all_dur.join   (top_cate, 'subclass_code', 'inner')\
                                 .groupBy(txn_all_dur.subclass_code)\
                                 .agg    ( max(F.col('division_name')).alias('div_nm')
                                          ,max(F.col('department_name')).alias('dept_nm')
                                          ,max(F.col('section_name')).alias('sec_nm')
                                          ,max(F.col('class_name')).alias('class_nm')
                                          ,max(F.col('subclass_name')).alias('sclass_nm')
                                          ,max(top_cate.cate_order_bycust).alias('cate_from_newtocate_order')
                                          ,max(top_cate.cate_cust_cnt).alias('n_cust_atv_from_cate')
                                          ,max(top_cate.pct_cate_cust).alias('pct_cust_atv_from_cate')
                                          ,countDistinct('household_id').alias('dur_cate_cust')
                                          ,(countDistinct('household_id')/lts_mylo_dur ).alias('pct_cate_cst_pen_lotuss_dur')
                                          ,(max(top_cate.pct_cate_cust)/(countDistinct('household_id')/lts_mylo_dur )).alias('idx_cstpen')
                                         )
                                 
    top_cate_pent   = top_cate_pent.orderBy('cate_from_newtocate_order')
    
    txn_topcate_bnd = txn_all_pcst.join   (top_cate, 'subclass_code', 'inner')\
                                 .groupBy( txn_all_pcst.subclass_code
                                          ,txn_all_pcst.brand_name
                                         )\
                                 .agg    ( countDistinct(txn_all_pcst.household_id).alias('brand_cust_cnt')
                                          ,(countDistinct(txn_all_pcst.household_id)/cust_new_cnt).alias('pct_to_allnew_cate_cust')
                                          ,(countDistinct(txn_all_pcst.household_id)/max(top_cate.cate_cust_cnt)).alias('pct_to_cate_cust')
                                          ,countDistinct(txn_all_pcst.transaction_uid).alias('brand_bsk_cnt')
                                          ,sum(txn_all_pcst.net_spend_amt).alias('brand_sales')
                                         )
    
    txn_topcate_bnd = txn_topcate_bnd.withColumn('brand_order', F.rank().over(Window.partitionBy('subclass_code').orderBy(F.col('brand_cust_cnt').desc())))\
                                     .withColumn('brand_order_dense', F.dense_rank().over(Window.partitionBy('subclass_code').orderBy(F.col('brand_cust_cnt').desc())))
                
    cate_brand_info = txn_topcate_bnd.join    ( cate_info_df, [txn_topcate_bnd.subclass_code == cate_info_df.subclass_code], 'inner')\
                                     .select  ( cate_info_df['*']
                                               ,txn_topcate_bnd.brand_name
                                               ,txn_topcate_bnd.brand_order
                                               ,txn_topcate_bnd.brand_order_dense
                                               ,txn_topcate_bnd.brand_cust_cnt
                                               ,txn_topcate_bnd.pct_to_cate_cust
                                               ,txn_topcate_bnd.pct_to_allnew_cate_cust
                                              )\
                                     .orderBy( cate_info_df.cate_order_bycust
                                              ,cate_info_df.subclass_code
                                              ,txn_topcate_bnd.brand_order
                                              ,txn_topcate_bnd.brand_name
                                             )
                                         
    ## return 2 df but can use only "cate_brand_info" with a complete information
    
    return cate_info_df, cate_brand_info, top_cate_pent

## end def


# COMMAND ----------

# MAGIC %md #sales growth and share

# COMMAND ----------

def get_aspect_df(txn, prior_st_date, pre_en_date, c_st_date, c_en_date, 
                  sel_brand, sel_sku, sel_class, sel_subclass, store_matching_df, 
                  store_format = 'HDE', brand_level = True, class_level = True):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table

    prior_st_date (str): In format YYYY-MM-DD. Start date of the 26-week period prior to start of promotion period

    pre_en_date (str): In format YYYY-MM-DD. End date of the 26-week period prior to start of promotion period

    c_st_date (str): In format YYYY-MM-DD. Start date of the promotion period

    c_en_date (str): In format YYYY-MM-DD. End date of the promotion period 

    sel_brand (str): Selected brand for feature (used if brand_level = True)

    sel_sku (list): List of selected SKUs for feature (used if brand_level = False)

    sel_class (str): Selected class for category

    sel_subclass (str): Selected subclass for category (used if subclass_level = False)

    store_matching_df (Pandas df): Dataframe of selected test stores and their matching control stores

    store_format (str): store format used in promotion campaign (default = 'HDE')

    brand_level (bool): Flag if feature should be at brand level (filtered by brand name), otherwise SKU level (filter by SKUs) (default = True)

    class_level (bool): Flag if category should be at subclass level, otherwise class level (default = True)

    Output:
    aspect_results (Spark df): dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    #filter txn for category (class) level and brand level
    if brand_level == True:
        txn_feature_pre = txn.filter(F.col('date_id').between(prior_st_date,pre_en_date))\
                             .filter(F.col('store_format_online_subchannel_other')==store_format)\
                             .filter(F.col('brand_name').isin(sel_brand))

        txn_feature_dur = txn.filter(F.col('date_id').between(c_st_date,c_en_date))\
                             .filter(F.col('store_format_online_subchannel_other')==store_format)\
                             .filter(F.col('brand_name').isin(sel_brand))
    else:
        txn_feature_pre = txn.filter(F.col('date_id').between(prior_st_date,pre_en_date))\
                             .filter(F.col('store_format_online_subchannel_other')==store_format)\
                             .filter(F.col('upc_id').isin(sel_sku))

        txn_feature_dur = txn.filter(F.col('date_id').between(c_st_date,c_en_date))\
                             .filter(F.col('store_format_online_subchannel_other')==store_format)\
                             .filter(F.col('upc_id').isin(sel_sku))

    if class_level == True:
        txn_category_pre = txn.filter(F.col('date_id').between(prior_st_date,pre_en_date))\
                              .filter(F.col('store_format_online_subchannel_other')==store_format)\
                              .filter(F.col('class_name').isin(sel_class))

        txn_category_dur = txn.filter(F.col('date_id').between(c_st_date,c_en_date))\
                              .filter(F.col('store_format_online_subchannel_other')==store_format)\
                              .filter(F.col('class_name').isin(sel_class))
    else:
        txn_category_pre = txn.filter(F.col('date_id').between(prior_st_date,pre_en_date))\
                              .filter(F.col('store_format_online_subchannel_other')==store_format)\
                              .filter(F.col('subclass_name').isin(sel_subclass))

        txn_category_dur = txn.filter(F.col('date_id').between(c_st_date,c_en_date))\
                              .filter(F.col('store_format_online_subchannel_other')==store_format)\
                              .filter(F.col('subclass_name').isin(sel_subclass))

    #create total sales for each item
    sale_feature_pre = txn_feature_pre.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
    sale_feature_dur = txn_feature_dur.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
    sale_category_pre = txn_category_pre.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
    sale_category_dur = txn_category_dur.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0)

    # Get list of test stores and their matched control stores
    matched_stores = spark.createDataFrame(store_matching_df).select('store_id', 'ctr_store_cos')

    # For each Test and Control store, get the total sale over Pre and During campaign periods, for Category and Feature levels
    aspect_results = matched_stores.join(sale_feature_pre, on='store_id', how='left').withColumnRenamed('sales', 'test_feature_pre_sale')
    aspect_results = aspect_results.join(sale_feature_dur, on='store_id', how='left').withColumnRenamed('sales', 'test_feature_dur_sale')
    aspect_results = aspect_results.join(sale_category_pre, on='store_id', how='left').withColumnRenamed('sales', 'test_category_pre_sale')
    aspect_results = aspect_results.join(sale_category_dur, on='store_id', how='left').withColumnRenamed('sales', 'test_category_dur_sale')

    aspect_results = aspect_results.join(sale_feature_pre.withColumnRenamed('store_id', 'ctr_store_cos'), 
                                       on='ctr_store_cos', how='left').withColumnRenamed('sales', 'ctr_feature_pre_sale')
    aspect_results = aspect_results.join(sale_feature_dur.withColumnRenamed('store_id', 'ctr_store_cos'),
                                       on='ctr_store_cos', how='left').withColumnRenamed('sales', 'ctr_feature_dur_sale')
    aspect_results = aspect_results.join(sale_category_pre.withColumnRenamed('store_id', 'ctr_store_cos'),
                                       on='ctr_store_cos', how='left').withColumnRenamed('sales', 'ctr_category_pre_sale')
    aspect_results = aspect_results.join(sale_category_dur.withColumnRenamed('store_id', 'ctr_store_cos'),
                                       on='ctr_store_cos', how='left').withColumnRenamed('sales', 'ctr_category_dur_sale')

    return aspect_results


def get_aspect_output(aspect_results, camp_weeks):
    '''
    Using aspect_results dataframe, get the corresponding outputs for generating graphs in the report

    Parameters:
    aspect_results (Spark df): dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period

    camp_weeks: number of weeks in campaign period

    Output:
    aspect_output (Spark df): dataframe with data: 
      - At Category-level and Feature-level, average sale per week per store, for test stores and control stores, pre and during the campaign period
      - Shares of Feature-level sale vs Category-level sale, for test stores and control stores, pre and during the campaign period
    '''  
    # Sum all columns
    sum_table = aspect_results.groupBy().sum()
    num_of_stores = aspect_results.count()
    
    sum_table_columns = sum_table.columns
    
    # Create column and fill with 0 if column doesn't already exist (e.g. the feature does not have any sale over that period)
    if 'sum(test_feature_pre_sale)' not in sum_table_columns:
        sum_table = sum_table.withColumn('sum(test_feature_pre_sale)', F.lit(0))
    if 'sum(test_feature_dur_sale)' not in sum_table_columns:
        sum_table = sum_table.withColumn('sum(test_feature_dur_sale)', F.lit(0))   
    if 'sum(ctr_feature_pre_sale)' not in sum_table_columns:
        sum_table = sum_table.withColumn('sum(ctr_feature_pre_sale)', F.lit(0))    
    if 'sum(ctr_feature_dur_sale)' not in sum_table_columns:
        sum_table = sum_table.withColumn('sum(ctr_feature_dur_sale)', F.lit(0))    

    # For each summed column, divide by number of stores, and divide by number of weeks (campaign weeks for During, 26 for Pre)
    test_cat_pre_store_week = sum_table.select('sum(test_category_pre_sale)').collect()[0][0] / 26 / num_of_stores
    test_cat_dur_store_week = sum_table.select('sum(test_category_dur_sale)').collect()[0][0] / camp_weeks / num_of_stores
    ctr_cat_pre_store_week = sum_table.select('sum(ctr_category_pre_sale)').collect()[0][0] / 26 / num_of_stores
    ctr_cat_dur_store_week = sum_table.select('sum(ctr_category_dur_sale)').collect()[0][0] / camp_weeks / num_of_stores
    test_feature_pre_store_week = sum_table.select('sum(test_feature_pre_sale)').collect()[0][0] / 26 / num_of_stores
    test_feature_dur_store_week = sum_table.select('sum(test_feature_dur_sale)').collect()[0][0] / camp_weeks / num_of_stores
    ctr_feature_pre_store_week = sum_table.select('sum(ctr_feature_pre_sale)').collect()[0][0] / 26 / num_of_stores
    ctr_feature_dur_store_week = sum_table.select('sum(ctr_feature_dur_sale)').collect()[0][0] / camp_weeks / num_of_stores

    # Get sales shares by dividing brand sale by category sale for each relevant period
    test_share_pre = test_feature_pre_store_week / test_cat_pre_store_week
    test_share_dur = test_feature_dur_store_week / test_cat_dur_store_week
    ctr_share_pre = ctr_feature_pre_store_week / ctr_cat_pre_store_week
    ctr_share_dur = ctr_feature_dur_store_week / ctr_cat_dur_store_week

    # Create dataframe for output in desired format
    aspect_col = ['graph', 'x', 'Prior', 'During']
    aspect_data = [('Category', 'Test', test_cat_pre_store_week, test_cat_dur_store_week),
                 ('Category', 'Ctr', ctr_cat_pre_store_week, ctr_cat_dur_store_week),
                 ('Feature', 'Test', test_feature_pre_store_week, test_feature_dur_store_week),
                 ('Feature', 'Ctr', ctr_feature_pre_store_week, ctr_feature_dur_store_week), 
                 ('Share', 'Test', test_share_pre, test_share_dur), 
                 ('Share', 'Ctr', ctr_share_pre, ctr_share_dur)]

    aspect_output = spark.createDataFrame(aspect_data, aspect_col)	

    return aspect_output

# COMMAND ----------

# MAGIC %md # Get Sale Market Share Growth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gett_sales_mkt_growth with control store - New updated with duplicate control transaction per matched used.

# COMMAND ----------

def get_sales_mkt_growth( txn
                         ,prod_df
                         ,cate_df
                         ,prod_level
                         ,cate_level
                         ,week_type
                         ,store_format
                         ,store_matching_df
                        ):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table
    prod_df (spark dataframe) : product dataframe to filter transaction
    cate_df (spark dataframe) : Category product dataframe to filter transaction
    prod_level (string) : (brand/feature) Level of product to get value need to align with prod_df that pass in to (using this variable as lable only)
    cate_level (string) : (class/subclass) Level of category to get value need to align with prod_df that pass in to (using this variable as lable only)
    week_type (string)  : (fis_wk / promo_wk) use to select grouping week that need to use default = fis_wk
    
    store_matching_df (Spark df): Dataframe of selected test stores and their matching control stores

    store_format (str): store format used in promotion campaign (default = 'HDE')

    Return:
    sales_info_pd (pandas dataframe) : dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    if store_format == '':
        print('Not define store format, will use HDE as default')
        store_format = 'HDE'
    ## end if
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    elif week_type == 'promo_wk':
        period_col = 'period_promo_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## Prep store matching for #control store used
    
    ##ctrl_str_df = store_matching_df.select(store_matching_df.ctr_store_var.alias('store_id'))  ## will have duplicate control store_id here.
    
    ## change to use new control store column - Feb 2023 Pat

    ctrl_str_df = store_matching_df.select(store_matching_df.ctr_store_cos.alias('store_id'))  ## will have duplicate control store_id here.

    
    #print('Display ctrl_str_df')
    
    #ctrl_str_df.display()
    
    ## convert matching df to spark dataframe for joining
    
    #filter txn for category (class) level and brand level    
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    ## Cate Target    
    txn_cate_trg     = txn.join (cate_df          , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')
                          
    txn_cate_pre_trg = txn_cate_trg.where(txn_pre_cond)
    txn_cate_dur_trg = txn_cate_trg.where(txn_dur_cond)
        
    ## Cate control
    ## inner join to control store with duplicate line , will make transaction duplicate for control store being used by itself    
    txn_cate_ctl     = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (ctrl_str_df, [txn.store_id == ctrl_str_df.store_id], 'inner')
    
    txn_cate_pre_ctl = txn_cate_ctl.where(txn_pre_cond)
    txn_cate_dur_ctl = txn_cate_ctl.where(txn_dur_cond)


    ## Prod Target
    
    txn_prod_trg     = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')
    
    txn_prod_pre_trg = txn_prod_trg.where(txn_pre_cond)
    txn_prod_dur_trg = txn_prod_trg.where(txn_dur_cond)
    
    ## Prod control
    
    txn_prod_ctl     = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (ctrl_str_df, [txn.store_id == ctrl_str_df.store_id], 'inner')
    
    txn_prod_pre_ctl = txn_prod_ctl.where(txn_pre_cond)
    txn_prod_dur_ctl = txn_prod_ctl.where(txn_dur_cond)
    
    
    ## get Weekly sales & Average weekly sales for each group
    
    ## for fis_week
    if week_col == 'week_id':
        
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## control prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
    else:  ## use promo week
    
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## control prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    ## end if
    
    ## Get average weekly sales using average function
    
    ## target
    ### average in each period
    cate_pre_trg_agg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg'), sum('sales').alias('cate_pre_trg_ssum'))
    cate_dur_trg_agg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg'), sum('sales').alias('cate_dur_trg_ssum'))
    prod_pre_trg_agg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg'), sum('sales').alias('prod_pre_trg_ssum'))
    prod_dur_trg_agg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg'), sum('sales').alias('prod_dur_trg_ssum'))
    
    cate_pre_trg_avg  = cate_pre_trg_agg.select('cate_pre_trg_avg').collect()[0].cate_pre_trg_avg
    cate_dur_trg_avg  = cate_dur_trg_agg.select('cate_dur_trg_avg').collect()[0].cate_dur_trg_avg
    prod_pre_trg_avg  = prod_pre_trg_agg.select('prod_pre_trg_avg').collect()[0].prod_pre_trg_avg
    prod_dur_trg_avg  = prod_dur_trg_agg.select('prod_dur_trg_avg').collect()[0].prod_dur_trg_avg
    
    cate_pre_trg_ssum  = cate_pre_trg_agg.select('cate_pre_trg_ssum').collect()[0].cate_pre_trg_ssum
    cate_dur_trg_ssum  = cate_dur_trg_agg.select('cate_dur_trg_ssum').collect()[0].cate_dur_trg_ssum
    prod_pre_trg_ssum  = prod_pre_trg_agg.select('prod_pre_trg_ssum').collect()[0].prod_pre_trg_ssum
    prod_dur_trg_ssum  = prod_dur_trg_agg.select('prod_dur_trg_ssum').collect()[0].prod_dur_trg_ssum

    
    del cate_pre_trg_agg
    del cate_dur_trg_agg
    del prod_pre_trg_agg
    del prod_dur_trg_agg
    
    #cate_pre_trg_avg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg')).fillna(0).collect()[0].cate_pre_trg_avg
    #cate_dur_trg_avg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg')).fillna(0).collect()[0].cate_dur_trg_avg
    #prod_pre_trg_avg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg')).fillna(0).collect()[0].prod_pre_trg_avg
    #prod_dur_trg_avg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg')).fillna(0).collect()[0].prod_dur_trg_avg
    #### sum all in each period
    #cate_pre_trg_ssum = cate_pre_trg_sum.agg(sum('sales').alias('cate_pre_trg_ssum')).fillna(0).collect()[0].cate_pre_trg_ssum
    #cate_dur_trg_ssum = cate_dur_trg_sum.agg(sum('sales').alias('cate_dur_trg_ssum')).fillna(0).collect()[0].cate_dur_trg_ssum
    #prod_pre_trg_ssum = prod_pre_trg_sum.agg(sum('sales').alias('prod_pre_trg_ssum')).fillna(0).collect()[0].prod_pre_trg_ssum
    #prod_dur_trg_ssum = prod_dur_trg_sum.agg(sum('sales').alias('prod_dur_trg_ssum')).fillna(0).collect()[0].prod_dur_trg_ssum
    
    ## control
    ### average in each period
    cate_pre_ctl_agg  = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg'), sum('sales').alias('cate_pre_ctl_ssum'))
    cate_dur_ctl_agg  = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg'), sum('sales').alias('cate_dur_ctl_ssum'))
    prod_pre_ctl_agg  = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg'), sum('sales').alias('prod_pre_ctl_ssum'))
    prod_dur_ctl_agg  = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg'), sum('sales').alias('prod_dur_ctl_ssum'))
    
    cate_pre_ctl_avg  = cate_pre_ctl_agg.select('cate_pre_ctl_avg').collect()[0].cate_pre_ctl_avg
    cate_dur_ctl_avg  = cate_dur_ctl_agg.select('cate_dur_ctl_avg').collect()[0].cate_dur_ctl_avg
    prod_pre_ctl_avg  = prod_pre_ctl_agg.select('prod_pre_ctl_avg').collect()[0].prod_pre_ctl_avg
    prod_dur_ctl_avg  = prod_dur_ctl_agg.select('prod_dur_ctl_avg').collect()[0].prod_dur_ctl_avg
    
    cate_pre_ctl_ssum  = cate_pre_ctl_agg.select('cate_pre_ctl_ssum').collect()[0].cate_pre_ctl_ssum
    cate_dur_ctl_ssum  = cate_dur_ctl_agg.select('cate_dur_ctl_ssum').collect()[0].cate_dur_ctl_ssum
    prod_pre_ctl_ssum  = prod_pre_ctl_agg.select('prod_pre_ctl_ssum').collect()[0].prod_pre_ctl_ssum
    prod_dur_ctl_ssum  = prod_dur_ctl_agg.select('prod_dur_ctl_ssum').collect()[0].prod_dur_ctl_ssum
    
    del cate_pre_ctl_agg
    del cate_dur_ctl_agg
    del prod_pre_ctl_agg
    del prod_dur_ctl_agg
    
    ### average in each period
    #cate_pre_ctl_avg = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg')).fillna(0).collect()[0].cate_pre_ctl_avg
    #cate_dur_ctl_avg = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg')).fillna(0).collect()[0].cate_dur_ctl_avg
    #prod_pre_ctl_avg = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg')).fillna(0).collect()[0].prod_pre_ctl_avg
    #prod_dur_ctl_avg = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg')).fillna(0).collect()[0].prod_dur_ctl_avg
    #### sum all in each period
    #cate_pre_ctl_ssum = cate_pre_ctl_sum.agg(sum('sales').alias('cate_pre_ctl_ssum')).fillna(0).collect()[0].cate_pre_ctl_ssum
    #cate_dur_ctl_ssum = cate_dur_ctl_sum.agg(sum('sales').alias('cate_dur_ctl_ssum')).fillna(0).collect()[0].cate_dur_ctl_ssum
    #prod_pre_ctl_ssum = prod_pre_ctl_sum.agg(sum('sales').alias('prod_pre_ctl_ssum')).fillna(0).collect()[0].prod_pre_ctl_ssum
    #prod_dur_ctl_ssum = prod_dur_ctl_sum.agg(sum('sales').alias('prod_dur_ctl_ssum')).fillna(0).collect()[0].prod_dur_ctl_ssum
    
    ##--------------------------------------
    ## get sales growth pre vs during -> use average weekly Product level test vs control
    ##--------------------------------------
    if prod_pre_trg_avg is None :
        prod_pre_trg_avg = 0
    ## end if

    if prod_pre_ctl_avg is None:
        prod_pre_ctl_avg = 0
    ## end if   
     
    trg_wkly_growth = div_zero_chk( (prod_dur_trg_avg - prod_pre_trg_avg) , prod_pre_trg_avg)
    ctl_wkly_growth = div_zero_chk( (prod_dur_ctl_avg - prod_pre_ctl_avg) , prod_pre_ctl_avg)
       
    ##--------------------------------------
    ## get Market share growth pre vs during
    ##--------------------------------------
    ## target
    if prod_pre_trg_ssum is None:
        prod_pre_trg_ssum = 0
        trg_mkt_pre       = 0
    else :
        trg_mkt_pre    = div_zero_chk(prod_pre_trg_ssum ,cate_pre_trg_ssum)  
    ## end if      
    trg_mkr_dur    = div_zero_chk(prod_dur_trg_ssum, cate_dur_trg_ssum)
    trg_mkt_growth = div_zero_chk( (trg_mkr_dur - trg_mkt_pre) , trg_mkt_pre)
    
    ## control
    if prod_pre_ctl_ssum is None:
        prod_pre_ctl_ssum = 0
        ctl_mkt_pre       = 0
    else :
        ctl_mkt_pre    = div_zero_chk(prod_pre_ctl_ssum ,cate_pre_ctl_ssum)
    ## end if
    
    ctl_mkr_dur    = div_zero_chk(prod_dur_ctl_ssum, cate_dur_ctl_ssum)
    ctl_mkt_growth = div_zero_chk( (ctl_mkr_dur - ctl_mkt_pre) , ctl_mkt_pre)
    ##
    dummy          = 0/cate_pre_trg_ssum
    
    pd_cols = ['level','str_fmt', 'pre', 'during', 'week_type']
    pd_data = [ [cate_level+"_wk_avg",'Test'   ,cate_pre_trg_avg ,cate_dur_trg_avg , week_col]
               ,[cate_level+"_wk_avg",'Control',cate_pre_ctl_avg ,cate_dur_ctl_avg , week_col]
               ,[prod_level+"_wk_avg",'Test'   ,prod_pre_trg_avg ,prod_dur_trg_avg , week_col]
               ,[prod_level+"_wk_avg",'Control',prod_pre_ctl_avg ,prod_dur_ctl_avg , week_col]
               ,['MKT_SHARE'         ,'Test'   ,trg_mkt_pre      ,trg_mkr_dur      , week_col]
               ,['MKT_SHARE'         ,'Control',ctl_mkt_pre      ,ctl_mkr_dur      , week_col]
               ,['cate_sales'        ,'Test'   ,cate_pre_trg_ssum,cate_dur_trg_ssum, week_col]
               ,['cate_sales'        ,'Control',cate_pre_ctl_ssum,cate_dur_ctl_ssum, week_col]
               ,['prod_sales'        ,'Test'   ,prod_pre_trg_ssum,prod_dur_trg_ssum, week_col]
               ,['prod_sales'        ,'Control',prod_pre_ctl_ssum,prod_dur_ctl_ssum, week_col]
               ,['mkt_growth'        ,'Test'   ,dummy            ,trg_mkt_growth   , week_col]
               ,['mkt_growth'        ,'Control',dummy            ,ctl_mkt_growth   , week_col]
              ]
    
    sales_info_pd = pd.DataFrame(pd_data, columns = pd_cols)

    return sales_info_pd

## end def


# COMMAND ----------

# MAGIC %md ## get_sales_mkt_growth_noctrl For promo eval

# COMMAND ----------

def get_sales_mkt_growth_noctrl( txn
                                ,prod_df
                                ,cate_df
                                ,prod_level
                                ,cate_level
                                ,week_type
                                ,store_format
                                ,trg_store_df
                               ):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table
    prod_df (spark dataframe) : product dataframe to filter transaction
    cate_df (spark dataframe) : Category product dataframe to filter transaction
    prod_level (string) : (brand/feature) Level of product to get value need to align with prod_df that pass in to (using this variable as lable only)
    cate_level (string) : (class/subclass) Level of category to get value need to align with prod_df that pass in to (using this variable as lable only)
    week_type (string)  : (fis_wk / promo_wk) use to select grouping week that need to use default = fis_wk
    
    trg_store_df (spark df): Dataframe of target store

    store_format (str): store format used in promotion campaign (default = 'HDE')

    Return:
    sales_info_pd (pandas dataframe) : dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    if store_format == '':
        print('Not define store format, will use HDE as default')
        store_format = 'HDE'
    ## end if
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    elif week_type == 'promo_wk':
        period_col = 'period_promo_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## convert matching df to spark dataframe for joining
    
    #filter txn for category (class) level and brand level    
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    ## Cate Target
    txn_cate_pre_trg = txn.join (cate_df      , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df , [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_cate_dur_trg = txn.join (cate_df      , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df , [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ### Cate control
    #txn_cate_pre_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_pre_cond)
    #
    #txn_cate_dur_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_dur_cond)
                      
    ## Prod Target
    
    txn_prod_pre_trg = txn.join (prod_df     , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df, [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_prod_dur_trg = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df, [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ### Prod control
    #
    #txn_prod_pre_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_pre_cond)
    #
    #txn_prod_dur_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_dur_cond)
    
    ## get Weekly sales & Average weekly sales for each group
    
    ## for fis_week
    if week_col == 'week_id':
        
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ### control cate
        #cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.week_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.week_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        ### control prod
        #prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.week_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.week_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
    
    else:  ## use promo week
    
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ### control cate
        #cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        ### control prod
        #prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
    ## end if
    
    ## Get average weekly sales using average function -- For case of promo, pre period & during period having the same #weeks
    
    ## target
    ### average in each period
    cate_pre_trg_agg = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg'), sum('sales').alias('cate_pre_trg_ssum'))
    cate_dur_trg_agg = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg'), sum('sales').alias('cate_dur_trg_ssum'))
    prod_pre_trg_agg = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg'), sum('sales').alias('prod_pre_trg_ssum'))
    prod_dur_trg_agg = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg'), sum('sales').alias('prod_dur_trg_ssum'))
    
    #cate_pre_trg_avg  = cate_pre_trg_agg.select('cate_pre_trg_avg').collect()[0].cate_pre_trg_avg
    #cate_dur_trg_avg  = cate_dur_trg_agg.select('cate_dur_trg_avg').collect()[0].cate_dur_trg_avg
    #prod_pre_trg_avg  = prod_pre_trg_agg.select('prod_pre_trg_avg').collect()[0].prod_pre_trg_avg
    #prod_dur_trg_avg  = prod_dur_trg_agg.select('prod_dur_trg_avg').collect()[0].prod_dur_trg_avg
    #
    #cate_pre_trg_ssum  = cate_pre_trg_agg.select('cate_pre_trg_ssum').collect()[0].cate_pre_trg_ssum
    #cate_dur_trg_ssum  = cate_dur_trg_agg.select('cate_dur_trg_ssum').collect()[0].cate_dur_trg_ssum
    #prod_pre_trg_ssum  = prod_pre_trg_agg.select('prod_pre_trg_ssum').collect()[0].prod_pre_trg_ssum
    #prod_dur_trg_ssum  = prod_dur_trg_agg.select('prod_dur_trg_ssum').collect()[0].prod_dur_trg_ssum

    cate_pre_trg_ssum, cate_pre_trg_avg = cate_pre_trg_agg.select('cate_pre_trg_ssum', 'cate_pre_trg_avg').collect()[0]
    cate_dur_trg_ssum, cate_dur_trg_avg = cate_dur_trg_agg.select('cate_dur_trg_ssum', 'cate_dur_trg_avg').collect()[0]
    prod_pre_trg_ssum, prod_pre_trg_avg = prod_pre_trg_agg.select('prod_pre_trg_ssum', 'prod_pre_trg_avg').collect()[0]
    prod_dur_trg_ssum, prod_dur_trg_avg = prod_dur_trg_agg.select('prod_dur_trg_ssum', 'prod_dur_trg_avg').collect()[0]    
    
    del cate_pre_trg_agg
    del cate_dur_trg_agg
    del prod_pre_trg_agg
    del prod_dur_trg_agg
    
    ##cate_pre_trg_avg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg')).fillna(0).collect()[0].cate_pre_trg_avg
    ##cate_dur_trg_avg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg')).fillna(0).collect()[0].cate_dur_trg_avg
    ##prod_pre_trg_avg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg')).fillna(0).collect()[0].prod_pre_trg_avg
    ##prod_dur_trg_avg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg')).fillna(0).collect()[0].prod_dur_trg_avg
    ##### sum all in each period
    ##cate_pre_trg_ssum = cate_pre_trg_sum.agg(sum('sales').alias('cate_pre_trg_ssum')).fillna(0).collect()[0].cate_pre_trg_ssum
    ##cate_dur_trg_ssum = cate_dur_trg_sum.agg(sum('sales').alias('cate_dur_trg_ssum')).fillna(0).collect()[0].cate_dur_trg_ssum
    ##prod_pre_trg_ssum = prod_pre_trg_sum.agg(sum('sales').alias('prod_pre_trg_ssum')).fillna(0).collect()[0].prod_pre_trg_ssum
    ##prod_dur_trg_ssum = prod_dur_trg_sum.agg(sum('sales').alias('prod_dur_trg_ssum')).fillna(0).collect()[0].prod_dur_trg_ssum
    
    ### control
    #### average in each period
    #cate_pre_ctl_agg  = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg'), sum('sales').alias('cate_pre_ctl_ssum'))
    #cate_dur_ctl_agg  = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg'), sum('sales').alias('cate_dur_ctl_ssum'))
    #prod_pre_ctl_agg  = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg'), sum('sales').alias('prod_pre_ctl_ssum'))
    #prod_dur_ctl_agg  = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg'), sum('sales').alias('prod_dur_ctl_ssum'))
    #
    #cate_pre_ctl_avg  = cate_pre_ctl_agg.select('cate_pre_ctl_avg').collect()[0].cate_pre_ctl_avg
    #cate_dur_ctl_avg  = cate_dur_ctl_agg.select('cate_dur_ctl_avg').collect()[0].cate_dur_ctl_avg
    #prod_pre_ctl_avg  = prod_pre_ctl_agg.select('prod_pre_ctl_avg').collect()[0].prod_pre_ctl_avg
    #prod_dur_ctl_avg  = prod_dur_ctl_agg.select('prod_dur_ctl_avg').collect()[0].prod_dur_ctl_avg
    #
    #cate_pre_ctl_ssum  = cate_pre_ctl_agg.select('cate_pre_ctl_ssum').collect()[0].cate_pre_ctl_ssum
    #cate_dur_ctl_ssum  = cate_dur_ctl_agg.select('cate_dur_ctl_ssum').collect()[0].cate_dur_ctl_ssum
    #prod_pre_ctl_ssum  = prod_pre_ctl_agg.select('prod_pre_ctl_ssum').collect()[0].prod_pre_ctl_ssum
    #prod_dur_ctl_ssum  = prod_dur_ctl_agg.select('prod_dur_ctl_ssum').collect()[0].prod_dur_ctl_ssum
    #
    #del cate_pre_ctl_agg
    #del cate_dur_ctl_agg
    #del prod_pre_ctl_agg
    #del prod_dur_ctl_agg
    
    #### average in each period
    ##cate_pre_ctl_avg = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg')).fillna(0).collect()[0].cate_pre_ctl_avg
    ##cate_dur_ctl_avg = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg')).fillna(0).collect()[0].cate_dur_ctl_avg
    ##prod_pre_ctl_avg = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg')).fillna(0).collect()[0].prod_pre_ctl_avg
    ##prod_dur_ctl_avg = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg')).fillna(0).collect()[0].prod_dur_ctl_avg
    ##### sum all in each period
    ##cate_pre_ctl_ssum = cate_pre_ctl_sum.agg(sum('sales').alias('cate_pre_ctl_ssum')).fillna(0).collect()[0].cate_pre_ctl_ssum
    ##cate_dur_ctl_ssum = cate_dur_ctl_sum.agg(sum('sales').alias('cate_dur_ctl_ssum')).fillna(0).collect()[0].cate_dur_ctl_ssum
    ##prod_pre_ctl_ssum = prod_pre_ctl_sum.agg(sum('sales').alias('prod_pre_ctl_ssum')).fillna(0).collect()[0].prod_pre_ctl_ssum
    ##prod_dur_ctl_ssum = prod_dur_ctl_sum.agg(sum('sales').alias('prod_dur_ctl_ssum')).fillna(0).collect()[0].prod_dur_ctl_ssum
    
    ##--------------------------------------
    ## get sales growth pre vs during -> use average weekly Product level test vs control
    ##--------------------------------------
    
    if (prod_pre_trg_avg is None) :
        print(' "prod_pre_trg_avg" has no value, will set value to = 0 \n ')
        prod_pre_trg_avg = 0
    ## end if
    trg_wkly_growth  = div_zero_chk( (prod_dur_trg_avg - prod_pre_trg_avg) , prod_pre_trg_avg)
    
    if (cate_pre_trg_avg is None) :
        print(' "cate_pre_trg_avg" has no value, will set value to = 0 \n ')
        cate_pre_trg_avg = 0
    ## end if
    
    cate_wkly_growth = div_zero_chk( (cate_dur_trg_avg - cate_pre_trg_avg) , cate_pre_trg_avg)
        
    ## change to category 
    #ctl_wkly_growth = div_zero_chk( (prod_dur_ctl_avg - prod_pre_ctl_avg) , prod_pre_ctl_avg)
    
    ##--------------------------------------
    ## get Market share growth pre vs during -- only show target
    ##--------------------------------------
    ## target

    if (prod_pre_trg_ssum is None) :
        print(' "prod_pre_trg_ssum" has no value, will set value to = 0 \n ')
        prod_pre_trg_ssum = 0
    ## end if

    trg_mkt_pre    = div_zero_chk(prod_pre_trg_ssum ,cate_pre_trg_ssum)
    trg_mkr_dur    = div_zero_chk(prod_dur_trg_ssum, cate_dur_trg_ssum)
    trg_mkt_growth = div_zero_chk( (trg_mkr_dur - trg_mkt_pre) , trg_mkt_pre)
    
    ### control
    #ctl_mkt_pre    = div_zero_chk(prod_pre_ctl_ssum ,cate_pre_ctl_ssum)
    #ctl_mkr_dur    = div_zero_chk(prod_dur_ctl_ssum, cate_dur_ctl_ssum)
    #ctl_mkt_growth = div_zero_chk( (ctl_mkr_dur - ctl_mkt_pre) , ctl_mkt_pre)
    
    ##
    dummy          = 0/cate_pre_trg_ssum
    
    pd_cols = ['level','str_fmt', 'pre', 'during', 'week_type']
    pd_data = [ [cate_level+"_wk_avg",'Test'    ,cate_pre_trg_avg ,cate_dur_trg_avg , week_col]
               ,[prod_level+"_wk_avg",'Test'    ,prod_pre_trg_avg ,prod_dur_trg_avg , week_col]
               ,[cate_level+"_wk_grw",'Test'    ,dummy            ,cate_wkly_growth , week_col]
               ,[prod_level+"_wk_grw",'Test'    ,dummy            ,trg_wkly_growth  , week_col]
               ,['MKT_SHARE'         ,'Test'    ,trg_mkt_pre      ,trg_mkr_dur      , week_col]
               ,['cate_sales'        ,'Test'    ,cate_pre_trg_ssum,cate_dur_trg_ssum, week_col]
               ,['prod_sales'        ,'Test'    ,prod_pre_trg_ssum,prod_dur_trg_ssum, week_col]
               ,['mkt_growth'        ,'Test'    ,dummy            ,trg_mkt_growth   , week_col]
              ]
    
    sales_info_pd = pd.DataFrame(pd_data, columns = pd_cols)

    return sales_info_pd

## end def


# COMMAND ----------

# MAGIC %md ## back up Get Market Share growth - with control Not dup control transaction
# MAGIC
# MAGIC

# COMMAND ----------

def get_sales_mkt_growth_backup( txn
                         ,prod_df
                         ,cate_df
                         ,prod_level
                         ,cate_level
                         ,week_type
                         ,store_format
                         ,store_matching_df
                        ):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table
    prod_df (spark dataframe) : product dataframe to filter transaction
    cate_df (spark dataframe) : Category product dataframe to filter transaction
    prod_level (string) : (brand/feature) Level of product to get value need to align with prod_df that pass in to (using this variable as lable only)
    cate_level (string) : (class/subclass) Level of category to get value need to align with prod_df that pass in to (using this variable as lable only)
    week_type (string)  : (fis_wk / promo_wk) use to select grouping week that need to use default = fis_wk
    
    store_matching_df (Pandas df): Dataframe of selected test stores and their matching control stores

    store_format (str): store format used in promotion campaign (default = 'HDE')

    Return:
    sales_info_pd (pandas dataframe) : dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    if store_format == '':
        print('Not define store format, will use HDE as default')
        store_format = 'HDE'
    ## end if
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    elif week_type == 'promo_wk':
        period_col = 'period_promo_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## convert matching df to spark dataframe for joining
    
    #filter txn for category (class) level and brand level    
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    ## Cate Target
    txn_cate_pre_trg = txn.join (cate_df          , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_cate_dur_trg = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ## Cate control
    txn_cate_pre_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_cate_dur_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
                          .where(txn_dur_cond)
                      
    ## Prod Target
    
    txn_prod_pre_trg = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_prod_dur_trg = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ## Prod control
    
    txn_prod_pre_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_prod_dur_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
                          .where(txn_dur_cond)
    
    ## get Weekly sales & Average weekly sales for each group
    
    ## for fis_week
    if week_col == 'week_id':
        
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
    else:  ## use promo week
    
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    ## end if
    
    ## Get average weekly sales using average function
    
    ## target
    ### average in each period
    cate_pre_trg_agg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg'), sum('sales').alias('cate_pre_trg_ssum'))
    cate_dur_trg_agg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg'), sum('sales').alias('cate_dur_trg_ssum'))
    prod_pre_trg_agg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg'), sum('sales').alias('prod_pre_trg_ssum'))
    prod_dur_trg_agg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg'), sum('sales').alias('prod_dur_trg_ssum'))
    
    cate_pre_trg_avg  = cate_pre_trg_agg.select('cate_pre_trg_avg').collect()[0].cate_pre_trg_avg
    cate_dur_trg_avg  = cate_dur_trg_agg.select('cate_dur_trg_avg').collect()[0].cate_dur_trg_avg
    prod_pre_trg_avg  = prod_pre_trg_agg.select('prod_pre_trg_avg').collect()[0].prod_pre_trg_avg
    prod_dur_trg_avg  = prod_dur_trg_agg.select('prod_dur_trg_avg').collect()[0].prod_dur_trg_avg
    
    cate_pre_trg_ssum  = cate_pre_trg_agg.select('cate_pre_trg_ssum').collect()[0].cate_pre_trg_ssum
    cate_dur_trg_ssum  = cate_dur_trg_agg.select('cate_dur_trg_ssum').collect()[0].cate_dur_trg_ssum
    prod_pre_trg_ssum  = prod_pre_trg_agg.select('prod_pre_trg_ssum').collect()[0].prod_pre_trg_ssum
    prod_dur_trg_ssum  = prod_dur_trg_agg.select('prod_dur_trg_ssum').collect()[0].prod_dur_trg_ssum

    
    del cate_pre_trg_agg
    del cate_dur_trg_agg
    del prod_pre_trg_agg
    del prod_dur_trg_agg
    
    #cate_pre_trg_avg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg')).fillna(0).collect()[0].cate_pre_trg_avg
    #cate_dur_trg_avg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg')).fillna(0).collect()[0].cate_dur_trg_avg
    #prod_pre_trg_avg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg')).fillna(0).collect()[0].prod_pre_trg_avg
    #prod_dur_trg_avg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg')).fillna(0).collect()[0].prod_dur_trg_avg
    #### sum all in each period
    #cate_pre_trg_ssum = cate_pre_trg_sum.agg(sum('sales').alias('cate_pre_trg_ssum')).fillna(0).collect()[0].cate_pre_trg_ssum
    #cate_dur_trg_ssum = cate_dur_trg_sum.agg(sum('sales').alias('cate_dur_trg_ssum')).fillna(0).collect()[0].cate_dur_trg_ssum
    #prod_pre_trg_ssum = prod_pre_trg_sum.agg(sum('sales').alias('prod_pre_trg_ssum')).fillna(0).collect()[0].prod_pre_trg_ssum
    #prod_dur_trg_ssum = prod_dur_trg_sum.agg(sum('sales').alias('prod_dur_trg_ssum')).fillna(0).collect()[0].prod_dur_trg_ssum
    
    ## control
    ### average in each period
    cate_pre_ctl_agg  = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg'), sum('sales').alias('cate_pre_ctl_ssum'))
    cate_dur_ctl_agg  = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg'), sum('sales').alias('cate_dur_ctl_ssum'))
    prod_pre_ctl_agg  = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg'), sum('sales').alias('prod_pre_ctl_ssum'))
    prod_dur_ctl_agg  = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg'), sum('sales').alias('prod_dur_ctl_ssum'))
    
    cate_pre_ctl_avg  = cate_pre_ctl_agg.select('cate_pre_ctl_avg').collect()[0].cate_pre_ctl_avg
    cate_dur_ctl_avg  = cate_dur_ctl_agg.select('cate_dur_ctl_avg').collect()[0].cate_dur_ctl_avg
    prod_pre_ctl_avg  = prod_pre_ctl_agg.select('prod_pre_ctl_avg').collect()[0].prod_pre_ctl_avg
    prod_dur_ctl_avg  = prod_dur_ctl_agg.select('prod_dur_ctl_avg').collect()[0].prod_dur_ctl_avg
    
    cate_pre_ctl_ssum  = cate_pre_ctl_agg.select('cate_pre_ctl_ssum').collect()[0].cate_pre_ctl_ssum
    cate_dur_ctl_ssum  = cate_dur_ctl_agg.select('cate_dur_ctl_ssum').collect()[0].cate_dur_ctl_ssum
    prod_pre_ctl_ssum  = prod_pre_ctl_agg.select('prod_pre_ctl_ssum').collect()[0].prod_pre_ctl_ssum
    prod_dur_ctl_ssum  = prod_dur_ctl_agg.select('prod_dur_ctl_ssum').collect()[0].prod_dur_ctl_ssum
    
    del cate_pre_ctl_agg
    del cate_dur_ctl_agg
    del prod_pre_ctl_agg
    del prod_dur_ctl_agg
    
    ### average in each period
    #cate_pre_ctl_avg = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg')).fillna(0).collect()[0].cate_pre_ctl_avg
    #cate_dur_ctl_avg = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg')).fillna(0).collect()[0].cate_dur_ctl_avg
    #prod_pre_ctl_avg = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg')).fillna(0).collect()[0].prod_pre_ctl_avg
    #prod_dur_ctl_avg = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg')).fillna(0).collect()[0].prod_dur_ctl_avg
    #### sum all in each period
    #cate_pre_ctl_ssum = cate_pre_ctl_sum.agg(sum('sales').alias('cate_pre_ctl_ssum')).fillna(0).collect()[0].cate_pre_ctl_ssum
    #cate_dur_ctl_ssum = cate_dur_ctl_sum.agg(sum('sales').alias('cate_dur_ctl_ssum')).fillna(0).collect()[0].cate_dur_ctl_ssum
    #prod_pre_ctl_ssum = prod_pre_ctl_sum.agg(sum('sales').alias('prod_pre_ctl_ssum')).fillna(0).collect()[0].prod_pre_ctl_ssum
    #prod_dur_ctl_ssum = prod_dur_ctl_sum.agg(sum('sales').alias('prod_dur_ctl_ssum')).fillna(0).collect()[0].prod_dur_ctl_ssum
    
    ##--------------------------------------
    ## get sales growth pre vs during -> use average weekly Product level test vs control
    ##--------------------------------------
    if prod_pre_trg_avg is None :
        prod_pre_trg_avg = 0
        #trg_wkly_growth = 0
    elif prod_pre_ctl_avg is None :
        prod_pre_ctl_avg = 0
        #ctl_wkly_growth = 0
    ## end if
    trg_wkly_growth = div_zero_chk( (prod_dur_trg_avg - prod_pre_trg_avg) , prod_pre_trg_avg)
    ctl_wkly_growth = div_zero_chk( (prod_dur_ctl_avg - prod_pre_ctl_avg) , prod_pre_ctl_avg)
    ## end if

    ##--------------------------------------
    ## get Market share growth pre vs during
    ##--------------------------------------
    ## target
    if prod_pre_trg_ssum is None:
        prod_pre_trg_ssum = 0
    ## end if

    trg_mkt_pre    = div_zero_chk(prod_pre_trg_ssum ,cate_pre_trg_ssum)
    trg_mkr_dur    = div_zero_chk(prod_dur_trg_ssum, cate_dur_trg_ssum)
    trg_mkt_growth = div_zero_chk( (trg_mkr_dur - trg_mkt_pre) , trg_mkt_pre)
    
    ## control
    if prod_pre_ctl_ssum is None:
        prod_pre_ctl_ssum = 0
    ## end if

    ctl_mkt_pre    = div_zero_chk(prod_pre_ctl_ssum ,cate_pre_ctl_ssum)
    ctl_mkr_dur    = div_zero_chk(prod_dur_ctl_ssum, cate_dur_ctl_ssum)
    ctl_mkt_growth = div_zero_chk( (ctl_mkr_dur - ctl_mkt_pre) , ctl_mkt_pre)
    ##
    dummy          = 0/cate_pre_trg_ssum
    
    pd_cols = ['level','str_fmt', 'pre', 'during', 'week_type']
    pd_data = [ [cate_level+"_wk_avg",'Test'   ,cate_pre_trg_avg ,cate_dur_trg_avg , week_col]
               ,[cate_level+"_wk_avg",'Control',cate_pre_ctl_avg ,cate_dur_ctl_avg , week_col]
               ,[prod_level+"_wk_avg",'Test'   ,prod_pre_trg_avg ,prod_dur_trg_avg , week_col]
               ,[prod_level+"_wk_avg",'Control',prod_pre_ctl_avg ,prod_dur_ctl_avg , week_col]
               ,['MKT_SHARE'         ,'Test'   ,trg_mkt_pre      ,trg_mkr_dur      , week_col]
               ,['MKT_SHARE'         ,'Control',ctl_mkt_pre      ,ctl_mkr_dur      , week_col]
               ,['cate_sales'        ,'Test'   ,cate_pre_trg_ssum,cate_dur_trg_ssum, week_col]
               ,['cate_sales'        ,'Control',cate_pre_ctl_ssum,cate_dur_ctl_ssum, week_col]
               ,['prod_sales'        ,'Test'   ,prod_pre_trg_ssum,prod_dur_trg_ssum, week_col]
               ,['prod_sales'        ,'Control',prod_pre_ctl_ssum,prod_dur_ctl_ssum, week_col]
               ,['mkt_growth'        ,'Test'   ,dummy            ,trg_mkt_growth   , week_col]
               ,['mkt_growth'        ,'Control',dummy            ,ctl_mkt_growth   , week_col]
              ]
    
    sales_info_pd = pd.DataFrame(pd_data, columns = pd_cols)

    return sales_info_pd

## end def


# COMMAND ----------

# MAGIC %md # Target Store sales trend

# COMMAND ----------

## period_promo_mv_wk

def get_sales_trend_trg(intxn
                       ,trg_store_df
                       ,prod_df
                       ,prod_lvl
                       ,week_type
                       ,period_col
                       ):

    """ Get weekly target store sales trend at either SKU or Brand level, 
        5 input parameter
        trg_store_df : Target store dataframe
        prod_df      : dataframe contain list of product to get sales trend, either SKU or brand
        prod_lvl     : scope definition of product list (will use to show value only)
        week_type    : week type to get sales trend, either "fis_wk" or "promo_wk" 
        period_col   : column of period definition in transaction table.
    """
    
    ## check correctness of "week_type"
    week_type_chk = ['fis_wk', 'promo_wk']
    
    if week_type in week_type_chk :
        print('Week type to use for trend line = ' + str(week_type) + '\n')
    else:
        print('Warning !! : Given week column "' + str(week_type) + '" is not correct.  Process will use default value = "fis_wk" instead. \n')
        week_type = 'fis_wk'    
    ## end if
    
    ## Check correctness of period_column
    
    period_chk = ['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk']
    
    if period_col in period_chk :
        print('Period column to filter transaction = ' + str(period_col) + '\n')
    else:
        print('Warning !! : Given period column "' + str(period_col) + '" is not correct.  Process will use default value = "period_fis_wk" instead. \n')
        period_col = 'period_fis_wk'    
    ## end if
    
    txn_cond  = """ (({0} in ('pre', 'cmp')) and 
                     ( channel == 'OFFLINE')
                    ) 
                """.format(period_col)
    
    ## Prep main txn table
    if week_type == 'fis_wk' :
        
        txn = intxn.where (txn_cond)\
                   .join  (trg_store_df, intxn.store_id == trg_store_df.store_id, 'left_semi')\
                   .join  (prod_df     , intxn.upc_id == prod_df.upc_id, 'left_semi')\
                   .select( intxn.week_id.alias('data_week')
                           ,intxn.period_fis_wk.alias('period')
                           ,intxn.store_id
                           ,intxn.store_region
                           ,intxn.household_id
                           ,intxn.transaction_uid
                           ,intxn.net_spend_amt.alias('sales')
                           ,intxn.pkg_weight_unit.alias('units')
                          )
    
    elif week_type == 'promo_wk' :
    
        if period_col == 'period_promo_wk':
            txn = intxn.where (txn_cond)\
                       .join  (trg_store_df, intxn.store_id == trg_store_df.store_id, 'left_semi')\
                       .join  (prod_df     , intxn.upc_id == prod_df.upc_id, 'left_semi')\
                       .select( intxn.promoweek_id.alias('data_week')
                               ,intxn.period_promo_wk.alias('period')
                               ,intxn.store_id
                               ,intxn.store_region
                               ,intxn.household_id
                               ,intxn.transaction_uid
                               ,intxn.net_spend_amt.alias('sales')
                               ,intxn.pkg_weight_unit.alias('units')
                              )
        elif period_col == 'period_promo_mv_wk':
            txn = intxn.where (txn_cond)\
                       .join  (trg_store_df, intxn.store_id == trg_store_df.store_id, 'left_semi')\
                       .join  (prod_df     , intxn.upc_id == prod_df.upc_id, 'left_semi')\
                       .select( intxn.promoweek_id.alias('data_week')
                               ,intxn.period_promo_mv_wk.alias('period')
                               ,intxn.store_id
                               ,intxn.store_region
                               ,intxn.household_id
                               ,intxn.transaction_uid
                               ,intxn.net_spend_amt.alias('sales')
                               ,intxn.pkg_weight_unit.alias('units')
                              )
        else :
            print('Warning!! Incorrect period column code will return None.')
            return None
        ## end if
    ## end if
    
    ## Group by data for sales trend
    
    wk_txn = txn.groupBy( txn.data_week)\
                .agg    ( lit(week_type).alias('week_type')
                         ,max(txn.period).alias('period')
                         ,lit(prod_lvl).alias('product_level')
                         ,sum(txn.sales).alias('wk_sales')
                         ,sum(txn.units).alias('wk_units')
                         ,countDistinct(txn.household_id).alias('wk_cust')
                         ,countDistinct(txn.transaction_uid).alias('wk_visits')
                        )\
                .orderBy(txn.data_week)
    
    return wk_txn

## end def    


# COMMAND ----------

# MAGIC %md # Get Sales Market Growth Equal Week

# COMMAND ----------

def get_sales_mkt_growth_noctrl_eq_week( txn
                                ,prod_df
                                ,cate_df
                                ,prod_level
                                ,cate_level
                                ,week_type
                                ,store_format
                                ,trg_store_df
                               ):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table
    prod_df (spark dataframe) : product dataframe to filter transaction
    cate_df (spark dataframe) : Category product dataframe to filter transaction
    prod_level (string) : (brand/feature) Level of product to get value need to align with prod_df that pass in to (using this variable as lable only)
    cate_level (string) : (class/subclass) Level of category to get value need to align with prod_df that pass in to (using this variable as lable only)
    week_type (string)  : (fis_wk / promo_wk) use to select grouping week that need to use default = fis_wk
    
    trg_store_df (spark df): Dataframe of target store

    store_format (str): store format used in promotion campaign (default = 'HDE')

    Return:
    sales_info_pd (pandas dataframe) : dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    if store_format == '':
        print('Not define store format, will use HDE as default')
        store_format = 'HDE'
    ## end if
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_eq_fis_wk'
        week_col   = 'week_id'
    elif week_type == 'promo_wk':
        period_col = 'period_eq_promo_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_eq_fis_wk'
        week_col   = 'week_id'
    ## end if
    ## convert matching df to spark dataframe for joining
    
    #filter txn for category (class) level and brand level    
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    ## Cate Target
    txn_cate_pre_trg = txn.join (cate_df      , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df , [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_cate_dur_trg = txn.join (cate_df      , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df , [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ### Cate control
    #txn_cate_pre_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_pre_cond)
    #
    #txn_cate_dur_ctl = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_dur_cond)
                      
    ## Prod Target
    
    txn_prod_pre_trg = txn.join (prod_df     , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df, [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_pre_cond)
    
    txn_prod_dur_trg = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (trg_store_df, [txn.store_id == trg_store_df.store_id], 'left_semi')\
                          .where(txn_dur_cond)
    
    ### Prod control
    #
    #txn_prod_pre_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_pre_cond)
    #
    #txn_prod_dur_ctl = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
    #                      .join (store_matching_df, [txn.store_id == store_matching_df.ctr_store_var], 'left_semi')\
    #                      .where(txn_dur_cond)
    
    ## get Weekly sales & Average weekly sales for each group
    
    ## for fis_week
    if week_col == 'week_id':
        
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    

    
    else:  ## use promo week
    
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ### control cate
        #cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        ### control prod
        #prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
        #
        #prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.promoweek_id.alias('period_wk'))\
        #                                   .agg    ( sum('net_spend_amt').alias('sales'))
    ## end if
    
    ## Get average weekly sales using average function -- For case of promo, pre period & during period having the same #weeks
    
    ## target
    ### average in each period
    cate_pre_trg_agg = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg'), sum('sales').alias('cate_pre_trg_ssum'))
    cate_dur_trg_agg = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg'), sum('sales').alias('cate_dur_trg_ssum'))
    prod_pre_trg_agg = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg'), sum('sales').alias('prod_pre_trg_ssum'))
    prod_dur_trg_agg = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg'), sum('sales').alias('prod_dur_trg_ssum'))
    
    #cate_pre_trg_avg  = cate_pre_trg_agg.select('cate_pre_trg_avg').collect()[0].cate_pre_trg_avg
    #cate_dur_trg_avg  = cate_dur_trg_agg.select('cate_dur_trg_avg').collect()[0].cate_dur_trg_avg
    #prod_pre_trg_avg  = prod_pre_trg_agg.select('prod_pre_trg_avg').collect()[0].prod_pre_trg_avg
    #prod_dur_trg_avg  = prod_dur_trg_agg.select('prod_dur_trg_avg').collect()[0].prod_dur_trg_avg
    #
    #cate_pre_trg_ssum  = cate_pre_trg_agg.select('cate_pre_trg_ssum').collect()[0].cate_pre_trg_ssum
    #cate_dur_trg_ssum  = cate_dur_trg_agg.select('cate_dur_trg_ssum').collect()[0].cate_dur_trg_ssum
    #prod_pre_trg_ssum  = prod_pre_trg_agg.select('prod_pre_trg_ssum').collect()[0].prod_pre_trg_ssum
    #prod_dur_trg_ssum  = prod_dur_trg_agg.select('prod_dur_trg_ssum').collect()[0].prod_dur_trg_ssum

    cate_pre_trg_ssum, cate_pre_trg_avg = cate_pre_trg_agg.select('cate_pre_trg_ssum', 'cate_pre_trg_avg').collect()[0]
    cate_dur_trg_ssum, cate_dur_trg_avg = cate_dur_trg_agg.select('cate_dur_trg_ssum', 'cate_dur_trg_avg').collect()[0]
    prod_pre_trg_ssum, prod_pre_trg_avg = prod_pre_trg_agg.select('prod_pre_trg_ssum', 'prod_pre_trg_avg').collect()[0]
    prod_dur_trg_ssum, prod_dur_trg_avg = prod_dur_trg_agg.select('prod_dur_trg_ssum', 'prod_dur_trg_avg').collect()[0]    
    
    del cate_pre_trg_agg
    del cate_dur_trg_agg
    del prod_pre_trg_agg
    del prod_dur_trg_agg
    
    ##cate_pre_trg_avg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg')).fillna(0).collect()[0].cate_pre_trg_avg
    ##cate_dur_trg_avg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg')).fillna(0).collect()[0].cate_dur_trg_avg
    ##prod_pre_trg_avg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg')).fillna(0).collect()[0].prod_pre_trg_avg
    ##prod_dur_trg_avg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg')).fillna(0).collect()[0].prod_dur_trg_avg
    ##### sum all in each period
    ##cate_pre_trg_ssum = cate_pre_trg_sum.agg(sum('sales').alias('cate_pre_trg_ssum')).fillna(0).collect()[0].cate_pre_trg_ssum
    ##cate_dur_trg_ssum = cate_dur_trg_sum.agg(sum('sales').alias('cate_dur_trg_ssum')).fillna(0).collect()[0].cate_dur_trg_ssum
    ##prod_pre_trg_ssum = prod_pre_trg_sum.agg(sum('sales').alias('prod_pre_trg_ssum')).fillna(0).collect()[0].prod_pre_trg_ssum
    ##prod_dur_trg_ssum = prod_dur_trg_sum.agg(sum('sales').alias('prod_dur_trg_ssum')).fillna(0).collect()[0].prod_dur_trg_ssum
    
    ### control
    #### average in each period
    #cate_pre_ctl_agg  = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg'), sum('sales').alias('cate_pre_ctl_ssum'))
    #cate_dur_ctl_agg  = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg'), sum('sales').alias('cate_dur_ctl_ssum'))
    #prod_pre_ctl_agg  = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg'), sum('sales').alias('prod_pre_ctl_ssum'))
    #prod_dur_ctl_agg  = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg'), sum('sales').alias('prod_dur_ctl_ssum'))
    #
    #cate_pre_ctl_avg  = cate_pre_ctl_agg.select('cate_pre_ctl_avg').collect()[0].cate_pre_ctl_avg
    #cate_dur_ctl_avg  = cate_dur_ctl_agg.select('cate_dur_ctl_avg').collect()[0].cate_dur_ctl_avg
    #prod_pre_ctl_avg  = prod_pre_ctl_agg.select('prod_pre_ctl_avg').collect()[0].prod_pre_ctl_avg
    #prod_dur_ctl_avg  = prod_dur_ctl_agg.select('prod_dur_ctl_avg').collect()[0].prod_dur_ctl_avg
    #
    #cate_pre_ctl_ssum  = cate_pre_ctl_agg.select('cate_pre_ctl_ssum').collect()[0].cate_pre_ctl_ssum
    #cate_dur_ctl_ssum  = cate_dur_ctl_agg.select('cate_dur_ctl_ssum').collect()[0].cate_dur_ctl_ssum
    #prod_pre_ctl_ssum  = prod_pre_ctl_agg.select('prod_pre_ctl_ssum').collect()[0].prod_pre_ctl_ssum
    #prod_dur_ctl_ssum  = prod_dur_ctl_agg.select('prod_dur_ctl_ssum').collect()[0].prod_dur_ctl_ssum
    #
    #del cate_pre_ctl_agg
    #del cate_dur_ctl_agg
    #del prod_pre_ctl_agg
    #del prod_dur_ctl_agg
    
    #### average in each period
    ##cate_pre_ctl_avg = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg')).fillna(0).collect()[0].cate_pre_ctl_avg
    ##cate_dur_ctl_avg = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg')).fillna(0).collect()[0].cate_dur_ctl_avg
    ##prod_pre_ctl_avg = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg')).fillna(0).collect()[0].prod_pre_ctl_avg
    ##prod_dur_ctl_avg = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg')).fillna(0).collect()[0].prod_dur_ctl_avg
    ##### sum all in each period
    ##cate_pre_ctl_ssum = cate_pre_ctl_sum.agg(sum('sales').alias('cate_pre_ctl_ssum')).fillna(0).collect()[0].cate_pre_ctl_ssum
    ##cate_dur_ctl_ssum = cate_dur_ctl_sum.agg(sum('sales').alias('cate_dur_ctl_ssum')).fillna(0).collect()[0].cate_dur_ctl_ssum
    ##prod_pre_ctl_ssum = prod_pre_ctl_sum.agg(sum('sales').alias('prod_pre_ctl_ssum')).fillna(0).collect()[0].prod_pre_ctl_ssum
    ##prod_dur_ctl_ssum = prod_dur_ctl_sum.agg(sum('sales').alias('prod_dur_ctl_ssum')).fillna(0).collect()[0].prod_dur_ctl_ssum
    
    ##--------------------------------------
    ## get sales growth pre vs during -> use average weekly Product level test vs control
    ##--------------------------------------
    
    if (prod_pre_trg_avg is None) :
        print(' "prod_pre_trg_avg" has no value, will set value to = 0 \n ')
        prod_pre_trg_avg = 0
    ## end if
    trg_wkly_growth  = div_zero_chk( (prod_dur_trg_avg - prod_pre_trg_avg) , prod_pre_trg_avg)
    
    if (cate_pre_trg_avg is None) :
        print(' "cate_pre_trg_avg" has no value, will set value to = 0 \n ')
        cate_pre_trg_avg = 0
    ## end if
    
    cate_wkly_growth = div_zero_chk( (cate_dur_trg_avg - cate_pre_trg_avg) , cate_pre_trg_avg)
        
    ## change to category 
    #ctl_wkly_growth = div_zero_chk( (prod_dur_ctl_avg - prod_pre_ctl_avg) , prod_pre_ctl_avg)
    
    ##--------------------------------------
    ## get Market share growth pre vs during -- only show target
    ##--------------------------------------
    ## target

    if (prod_pre_trg_ssum is None) :
        print(' "prod_pre_trg_ssum" has no value, will set value to = 0 \n ')
        prod_pre_trg_ssum = 0
    ## end if

    trg_mkt_pre    = div_zero_chk(prod_pre_trg_ssum ,cate_pre_trg_ssum)
    trg_mkr_dur    = div_zero_chk(prod_dur_trg_ssum, cate_dur_trg_ssum)
    trg_mkt_growth = div_zero_chk( (trg_mkr_dur - trg_mkt_pre) , trg_mkt_pre)
    
    ### control
    #ctl_mkt_pre    = div_zero_chk(prod_pre_ctl_ssum ,cate_pre_ctl_ssum)
    #ctl_mkr_dur    = div_zero_chk(prod_dur_ctl_ssum, cate_dur_ctl_ssum)
    #ctl_mkt_growth = div_zero_chk( (ctl_mkr_dur - ctl_mkt_pre) , ctl_mkt_pre)
    
    ##
    dummy          = 0/cate_pre_trg_ssum
    
    pd_cols = ['level','str_fmt', 'pre', 'during', 'week_type']
    pd_data = [ [cate_level+"_wk_avg",'Test'    ,cate_pre_trg_avg ,cate_dur_trg_avg , week_col]
               ,[prod_level+"_wk_avg",'Test'    ,prod_pre_trg_avg ,prod_dur_trg_avg , week_col]
               ,[cate_level+"_wk_grw",'Test'    ,dummy            ,cate_wkly_growth , week_col]
               ,[prod_level+"_wk_grw",'Test'    ,dummy            ,trg_wkly_growth  , week_col]
               ,['MKT_SHARE'         ,'Test'    ,trg_mkt_pre      ,trg_mkr_dur      , week_col]
               ,['cate_sales'        ,'Test'    ,cate_pre_trg_ssum,cate_dur_trg_ssum, week_col]
               ,['prod_sales'        ,'Test'    ,prod_pre_trg_ssum,prod_dur_trg_ssum, week_col]
               ,['mkt_growth'        ,'Test'    ,dummy            ,trg_mkt_growth   , week_col]
              ]
    
    sales_info_pd = pd.DataFrame(pd_data, columns = pd_cols)

    return sales_info_pd

## end def

# COMMAND ----------

def get_sales_mkt_growth_eq_week( txn
                         ,prod_df
                         ,cate_df
                         ,prod_level
                         ,cate_level
                         ,week_type
                         ,store_format
                         ,store_matching_df
                        ):
    '''
    Get dataframe with columns for total sales at feature level and category level, for period prior to promotion and during promotion,
    for each test store and each matching control store.

    Parameters:
    txn (Spark dataframe): Item-level transaction table
    prod_df (spark dataframe) : product dataframe to filter transaction
    cate_df (spark dataframe) : Category product dataframe to filter transaction
    prod_level (string) : (brand/feature) Level of product to get value need to align with prod_df that pass in to (using this variable as lable only)
    cate_level (string) : (class/subclass) Level of category to get value need to align with prod_df that pass in to (using this variable as lable only)
    week_type (string)  : (fis_wk / promo_wk) use to select grouping week that need to use default = fis_wk
    
    store_matching_df (Spark df): Dataframe of selected test stores and their matching control stores

    store_format (str): store format used in promotion campaign (default = 'HDE')

    Return:
    sales_info_pd (pandas dataframe) : dataframe with columns:
      - ID of test store and ID of its matching control store
      - For test and control store, sales and feature and category level, pre and during promotion period
    '''
    if store_format == '':
        print('Not define store format, will use HDE as default')
        store_format = 'HDE'
    ## end if
    
    ## Select week period column name 
    if week_type == 'fis_wk':
        period_col = 'period_eq_fis_wk'
        week_col   = 'week_id'
    elif week_type == 'promo_wk':
        period_col = 'period_eq_promo_wk'
        week_col   = 'promoweek_id'
    else:
        print('In correct week_type, Will use fis_week as default value \n')
        period_col = 'period_eq_fis_wk'
        week_col   = 'week_id'
    ## end if
    
    ## Prep store matching for #control store used
    
    ##ctrl_str_df = store_matching_df.select(store_matching_df.ctr_store_var.alias('store_id'))  ## will have duplicate control store_id here.
    
    ## change to use new control store column - Feb 2023 Pat

    ctrl_str_df = store_matching_df.select(store_matching_df.ctr_store_cos.alias('store_id'))  ## will have duplicate control store_id here.

    
    #print('Display ctrl_str_df')
    
    #ctrl_str_df.display()
    
    ## convert matching df to spark dataframe for joining
    
    #filter txn for category (class) level and brand level    
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(period_col, store_format)
    
    ## Cate Target    
    txn_cate_trg     = txn.join (cate_df          , [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')
                          
    txn_cate_pre_trg = txn_cate_trg.where(txn_pre_cond)
    txn_cate_dur_trg = txn_cate_trg.where(txn_dur_cond)
        
    ## Cate control
    ## inner join to control store with duplicate line , will make transaction duplicate for control store being used by itself    
    txn_cate_ctl     = txn.join (cate_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (ctrl_str_df, [txn.store_id == ctrl_str_df.store_id], 'inner')
    
    txn_cate_pre_ctl = txn_cate_ctl.where(txn_pre_cond)
    txn_cate_dur_ctl = txn_cate_ctl.where(txn_dur_cond)


    ## Prod Target
    
    txn_prod_trg     = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (store_matching_df, [txn.store_id == store_matching_df.store_id], 'left_semi')
    
    txn_prod_pre_trg = txn_prod_trg.where(txn_pre_cond)
    txn_prod_dur_trg = txn_prod_trg.where(txn_dur_cond)
    
    ## Prod control
    
    txn_prod_ctl     = txn.join (prod_df, [txn.upc_id == cate_df.upc_id], 'left_semi')\
                          .join (ctrl_str_df, [txn.store_id == ctrl_str_df.store_id], 'inner')
    
    txn_prod_pre_ctl = txn_prod_ctl.where(txn_pre_cond)
    txn_prod_dur_ctl = txn_prod_ctl.where(txn_dur_cond)
    
    
    ## get Weekly sales & Average weekly sales for each group
    
    ## for fis_week
    if week_col == 'week_id':
        
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## control prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.week_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
    else:  ## use promo week
    
        ## target cate
        cate_pre_trg_sum = txn_cate_pre_trg.groupBy(txn_cate_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_trg_sum = txn_cate_dur_trg.groupBy(txn_cate_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## target prod
        prod_pre_trg_sum = txn_prod_pre_trg.groupBy(txn_prod_pre_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_trg_sum = txn_prod_dur_trg.groupBy(txn_prod_dur_trg.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    
        ## control cate
        cate_pre_ctl_sum = txn_cate_pre_ctl.groupBy(txn_cate_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        cate_dur_ctl_sum = txn_cate_dur_ctl.groupBy(txn_cate_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        ## control prod
        prod_pre_ctl_sum = txn_prod_pre_ctl.groupBy(txn_prod_pre_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
        
        prod_dur_ctl_sum = txn_prod_dur_ctl.groupBy(txn_prod_dur_ctl.promoweek_id.alias('period_wk'))\
                                           .agg    ( sum('net_spend_amt').alias('sales'))
    ## end if
    
    ## Get average weekly sales using average function
    
    ## target
    ### average in each period
    cate_pre_trg_agg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg'), sum('sales').alias('cate_pre_trg_ssum'))
    cate_dur_trg_agg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg'), sum('sales').alias('cate_dur_trg_ssum'))
    prod_pre_trg_agg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg'), sum('sales').alias('prod_pre_trg_ssum'))
    prod_dur_trg_agg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg'), sum('sales').alias('prod_dur_trg_ssum'))
    
    cate_pre_trg_avg  = cate_pre_trg_agg.select('cate_pre_trg_avg').collect()[0].cate_pre_trg_avg
    cate_dur_trg_avg  = cate_dur_trg_agg.select('cate_dur_trg_avg').collect()[0].cate_dur_trg_avg
    prod_pre_trg_avg  = prod_pre_trg_agg.select('prod_pre_trg_avg').collect()[0].prod_pre_trg_avg
    prod_dur_trg_avg  = prod_dur_trg_agg.select('prod_dur_trg_avg').collect()[0].prod_dur_trg_avg
    
    cate_pre_trg_ssum  = cate_pre_trg_agg.select('cate_pre_trg_ssum').collect()[0].cate_pre_trg_ssum
    cate_dur_trg_ssum  = cate_dur_trg_agg.select('cate_dur_trg_ssum').collect()[0].cate_dur_trg_ssum
    prod_pre_trg_ssum  = prod_pre_trg_agg.select('prod_pre_trg_ssum').collect()[0].prod_pre_trg_ssum
    prod_dur_trg_ssum  = prod_dur_trg_agg.select('prod_dur_trg_ssum').collect()[0].prod_dur_trg_ssum

    
    del cate_pre_trg_agg
    del cate_dur_trg_agg
    del prod_pre_trg_agg
    del prod_dur_trg_agg
    
    #cate_pre_trg_avg  = cate_pre_trg_sum.agg(avg('sales').alias('cate_pre_trg_avg')).fillna(0).collect()[0].cate_pre_trg_avg
    #cate_dur_trg_avg  = cate_dur_trg_sum.agg(avg('sales').alias('cate_dur_trg_avg')).fillna(0).collect()[0].cate_dur_trg_avg
    #prod_pre_trg_avg  = prod_pre_trg_sum.agg(avg('sales').alias('prod_pre_trg_avg')).fillna(0).collect()[0].prod_pre_trg_avg
    #prod_dur_trg_avg  = prod_dur_trg_sum.agg(avg('sales').alias('prod_dur_trg_avg')).fillna(0).collect()[0].prod_dur_trg_avg
    #### sum all in each period
    #cate_pre_trg_ssum = cate_pre_trg_sum.agg(sum('sales').alias('cate_pre_trg_ssum')).fillna(0).collect()[0].cate_pre_trg_ssum
    #cate_dur_trg_ssum = cate_dur_trg_sum.agg(sum('sales').alias('cate_dur_trg_ssum')).fillna(0).collect()[0].cate_dur_trg_ssum
    #prod_pre_trg_ssum = prod_pre_trg_sum.agg(sum('sales').alias('prod_pre_trg_ssum')).fillna(0).collect()[0].prod_pre_trg_ssum
    #prod_dur_trg_ssum = prod_dur_trg_sum.agg(sum('sales').alias('prod_dur_trg_ssum')).fillna(0).collect()[0].prod_dur_trg_ssum
    
    ## control
    ### average in each period
    cate_pre_ctl_agg  = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg'), sum('sales').alias('cate_pre_ctl_ssum'))
    cate_dur_ctl_agg  = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg'), sum('sales').alias('cate_dur_ctl_ssum'))
    prod_pre_ctl_agg  = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg'), sum('sales').alias('prod_pre_ctl_ssum'))
    prod_dur_ctl_agg  = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg'), sum('sales').alias('prod_dur_ctl_ssum'))
    
    cate_pre_ctl_avg  = cate_pre_ctl_agg.select('cate_pre_ctl_avg').collect()[0].cate_pre_ctl_avg
    cate_dur_ctl_avg  = cate_dur_ctl_agg.select('cate_dur_ctl_avg').collect()[0].cate_dur_ctl_avg
    prod_pre_ctl_avg  = prod_pre_ctl_agg.select('prod_pre_ctl_avg').collect()[0].prod_pre_ctl_avg
    prod_dur_ctl_avg  = prod_dur_ctl_agg.select('prod_dur_ctl_avg').collect()[0].prod_dur_ctl_avg
    
    cate_pre_ctl_ssum  = cate_pre_ctl_agg.select('cate_pre_ctl_ssum').collect()[0].cate_pre_ctl_ssum
    cate_dur_ctl_ssum  = cate_dur_ctl_agg.select('cate_dur_ctl_ssum').collect()[0].cate_dur_ctl_ssum
    prod_pre_ctl_ssum  = prod_pre_ctl_agg.select('prod_pre_ctl_ssum').collect()[0].prod_pre_ctl_ssum
    prod_dur_ctl_ssum  = prod_dur_ctl_agg.select('prod_dur_ctl_ssum').collect()[0].prod_dur_ctl_ssum
    
    del cate_pre_ctl_agg
    del cate_dur_ctl_agg
    del prod_pre_ctl_agg
    del prod_dur_ctl_agg
    
    ### average in each period
    #cate_pre_ctl_avg = cate_pre_ctl_sum.agg(avg('sales').alias('cate_pre_ctl_avg')).fillna(0).collect()[0].cate_pre_ctl_avg
    #cate_dur_ctl_avg = cate_dur_ctl_sum.agg(avg('sales').alias('cate_dur_ctl_avg')).fillna(0).collect()[0].cate_dur_ctl_avg
    #prod_pre_ctl_avg = prod_pre_ctl_sum.agg(avg('sales').alias('prod_pre_ctl_avg')).fillna(0).collect()[0].prod_pre_ctl_avg
    #prod_dur_ctl_avg = prod_dur_ctl_sum.agg(avg('sales').alias('prod_dur_ctl_avg')).fillna(0).collect()[0].prod_dur_ctl_avg
    #### sum all in each period
    #cate_pre_ctl_ssum = cate_pre_ctl_sum.agg(sum('sales').alias('cate_pre_ctl_ssum')).fillna(0).collect()[0].cate_pre_ctl_ssum
    #cate_dur_ctl_ssum = cate_dur_ctl_sum.agg(sum('sales').alias('cate_dur_ctl_ssum')).fillna(0).collect()[0].cate_dur_ctl_ssum
    #prod_pre_ctl_ssum = prod_pre_ctl_sum.agg(sum('sales').alias('prod_pre_ctl_ssum')).fillna(0).collect()[0].prod_pre_ctl_ssum
    #prod_dur_ctl_ssum = prod_dur_ctl_sum.agg(sum('sales').alias('prod_dur_ctl_ssum')).fillna(0).collect()[0].prod_dur_ctl_ssum
    
    ##--------------------------------------
    ## get sales growth pre vs during -> use average weekly Product level test vs control
    ##--------------------------------------
    if prod_pre_trg_avg is None :
        prod_pre_trg_avg = 0
    ## end if

    if prod_pre_ctl_avg is None:
        prod_pre_ctl_avg = 0
    ## end if   
     
    trg_wkly_growth = div_zero_chk( (prod_dur_trg_avg - prod_pre_trg_avg) , prod_pre_trg_avg)
    ctl_wkly_growth = div_zero_chk( (prod_dur_ctl_avg - prod_pre_ctl_avg) , prod_pre_ctl_avg)
       
    ##--------------------------------------
    ## get Market share growth pre vs during
    ##--------------------------------------
    ## target
    if prod_pre_trg_ssum is None:
        prod_pre_trg_ssum = 0
        trg_mkt_pre       = 0
    else :
        trg_mkt_pre    = div_zero_chk(prod_pre_trg_ssum ,cate_pre_trg_ssum)  
    ## end if      
    trg_mkr_dur    = div_zero_chk(prod_dur_trg_ssum, cate_dur_trg_ssum)
    trg_mkt_growth = div_zero_chk( (trg_mkr_dur - trg_mkt_pre) , trg_mkt_pre)
    
    ## control
    if prod_pre_ctl_ssum is None:
        prod_pre_ctl_ssum = 0
        ctl_mkt_pre       = 0
    else :
        ctl_mkt_pre    = div_zero_chk(prod_pre_ctl_ssum ,cate_pre_ctl_ssum)
    ## end if
    
    ctl_mkr_dur    = div_zero_chk(prod_dur_ctl_ssum, cate_dur_ctl_ssum)
    ctl_mkt_growth = div_zero_chk( (ctl_mkr_dur - ctl_mkt_pre) , ctl_mkt_pre)
    ##
    dummy          = 0/cate_pre_trg_ssum
    
    pd_cols = ['level','str_fmt', 'pre', 'during', 'week_type']
    pd_data = [ [cate_level+"_wk_avg",'Test'   ,cate_pre_trg_avg ,cate_dur_trg_avg , week_col]
               ,[cate_level+"_wk_avg",'Control',cate_pre_ctl_avg ,cate_dur_ctl_avg , week_col]
               ,[prod_level+"_wk_avg",'Test'   ,prod_pre_trg_avg ,prod_dur_trg_avg , week_col]
               ,[prod_level+"_wk_avg",'Control',prod_pre_ctl_avg ,prod_dur_ctl_avg , week_col]
               ,['MKT_SHARE'         ,'Test'   ,trg_mkt_pre      ,trg_mkr_dur      , week_col]
               ,['MKT_SHARE'         ,'Control',ctl_mkt_pre      ,ctl_mkr_dur      , week_col]
               ,['cate_sales'        ,'Test'   ,cate_pre_trg_ssum,cate_dur_trg_ssum, week_col]
               ,['cate_sales'        ,'Control',cate_pre_ctl_ssum,cate_dur_ctl_ssum, week_col]
               ,['prod_sales'        ,'Test'   ,prod_pre_trg_ssum,prod_dur_trg_ssum, week_col]
               ,['prod_sales'        ,'Control',prod_pre_ctl_ssum,prod_dur_ctl_ssum, week_col]
               ,['mkt_growth'        ,'Test'   ,dummy            ,trg_mkt_growth   , week_col]
               ,['mkt_growth'        ,'Control',dummy            ,ctl_mkt_growth   , week_col]
              ]
    
    sales_info_pd = pd.DataFrame(pd_data, columns = pd_cols)

    return sales_info_pd

## end def

