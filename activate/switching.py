import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval
from utils import period_cal
from activate import activated
from exposure.exposed import create_txn_offline_x_aisle_target_store

def get_cust_brand_switching_and_penetration(cmp: CampaignEval):
    """Media evaluation solution, customer switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    brand_df = cmp.feat_brand_sku
    switching_lv = cmp.params["cate_lvl"]
    cust_movement_sf = cmp.sku_activated_cust_movement
    
    #---- Helper fn
    ## Customer Switching by Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: SparkDataFrame,
                   prod_trans: SparkDataFrame, grp: List,
                   prod_lev: str, full_prod_lev: str ,
                   col_rename: str, period: str
                   ):
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )

        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]

        ## Add check None -- to prevent error Float (NoneType) --- Pat 25 Nov 2022
        if total_oth is None:
            total_oth = 0
        ## end if

        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))

        print("\t\t\t\t\t\t**Running micro df2")
        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name',
                      'class_name',
                      # 'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name',
                      'class_name', # TO BE DONE support for multi-class
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        #---- To be done : if switching at multi class
        # elif prod_lev == 'class':
        #     micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        else:
            micro_df_summ = cmp.spark.createDataFrame([],[])

        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name',
                 'class_name', # TO BE DONE support for multi-class
                 'original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
        #  .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(),
                #   F.col('pct_spend_oth_'+full_prod_lev).desc()
        )

        switching_result = switching_result.checkpoint()

        return switching_result

    def _get_swtchng_pntrtn(switching_lv: str):
        """Get Switching and penetration based on defined switching at class / subclass
        Support multi subclass
        """
        if switching_lv == "subclass":
            prd_scope_df = sclass_df
            gr_col = ['division_name','department_name','section_name','class_name',
                      'brand_name','household_id']
        else:
            prd_scope_df = class_df
            gr_col = ['division_name','department_name','section_name',
                      "class_name",  # TO BE DONE support for multi subclass
                      'brand_name','household_id']

        prior_pre_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col_nm).isin(['pre', 'ppp']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        prior_pre_cc_txn_prd_scope_sel_brand = prior_pre_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        prior_pre_prd_scope_sel_brand_kpi = \
        (prior_pre_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )

        dur_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col_nm).isin(['dur']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        dur_cc_txn_prd_scope_sel_brand = dur_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        dur_prd_scope_sel_brand_kpi = \
        (dur_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )

        pre_dur_band_spend = \
        (prior_pre_prd_scope_sel_brand_kpi
         .join(dur_prd_scope_sel_brand_kpi, gr_col, 'outer')
        )

        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand',
                                                 cust_movement_pre_dur_spend,
                                                 prior_pre_cc_txn_prd_scope,
                                               # ['subclass_name', 'brand_name'],
                                                 ["class_name", 'brand_name'], # TO BE DONE : support for multi-subclass
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        dur_prd_scope_cust = dur_cc_txn_prd_scope.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_prd_scope
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(dur_prd_scope_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen

    #---- Main
    print("-"*80)
    print("Customer brand switching")
    print(f"Brand switching within : {switching_lv.upper()}")
    print("-"*80)
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col_nm}")
    print("-"*80)

    new_to_brand_switching, cust_mv_pre_dur_spend, brand_cust_pen = _get_swtchng_pntrtn(switching_lv=switching_lv)
    cust_brand_switching_and_pen = \
        (new_to_brand_switching.alias("a")
         .join(brand_cust_pen.alias("b"),
               F.col("a.oth_brand_in_category")==F.col("b.brand_name"), "left")
                  .orderBy(F.col("pct_cust_oth_brand_in_category").desc())
        )

    return new_to_brand_switching, brand_cust_pen, cust_brand_switching_and_pen

def get_cust_brand_switching_and_penetration_multi(cmp: CampaignEval):
    """Media evaluation solution, customer switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    cate_df = cmp.feat_cate_sku
    switching_lv = cmp.params["cate_lvl"]
    cust_movement_sf = cmp.sku_activated_cust_movement
    
    #---- Main
    print("-"*80)
    print("Customer brand switching")
    print(f"Brand switching within : {switching_lv.upper()}")
    print("-"*80)
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col_nm}")
    print("-"*80)

    new_to_brand_cust = cust_movement_sf.where(F.col('customer_micro_flag') == "new_to_brand")
    n_new_to_brand_cust = cust_movement_sf.where(F.col('customer_micro_flag') == "new_to_brand").agg(F.count_distinct("household_id")).collect()[0][0]

    prior_pre_new_to_brand_txn_in_cate = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col_nm).isin(['pre', 'ppp']))

     .join(new_to_brand_cust, "household_id", "inner")
     .join(cate_df, "upc_id", "inner")
    )
    prior_pre_new_to_brand_txn_in_cate.agg(F.count_distinct("household_id")).display()
    combine_hier = \
    (prior_pre_new_to_brand_txn_in_cate
     .select("brand_name", F.concat_ws("_", "division_name", "department_name", "section_name", "class_name", "subclass_name").alias("comb_hier"))
     .groupBy("brand_name")
     .agg(F.collect_set("comb_hier").alias("category"))
     .select("brand_name", "category")
    )

    pre_new_to_brand_cate_cust = prior_pre_new_to_brand_txn_in_cate.agg(F.count_distinct("household_id")).collect()[0][0]
    pre_brand_in_cate = \
    (prior_pre_new_to_brand_txn_in_cate
     .groupBy("brand_name")
     .agg(F.count_distinct("household_id").alias("pre_brand_switch_cust"))
     .withColumn("new_to_brand_cust", F.lit(n_new_to_brand_cust))
     .withColumn("prop_cust_switch", F.col("pre_brand_switch_cust")/F.col("new_to_brand_cust"))
    )
    #pre_brand_in_cate.display()

    prior_pre_txn_in_cate = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col_nm).isin(['pre', 'ppp']))
     .join(cate_df, "upc_id", "inner")
    )

    pre_cate_cust = prior_pre_txn_in_cate.agg(F.count_distinct("household_id")).collect()[0][0]
    pre_brand_cust_pen = \
    (prior_pre_txn_in_cate
     .groupBy("brand_name")
     .agg(F.count_distinct("household_id").alias("pre_brand_cust"))
     .withColumn("pre_total_cate_cust", F.lit(pre_cate_cust))
     .withColumn("cust_pen", F.col("pre_brand_cust")/F.col("pre_total_cate_cust"))
    )

    switch_pen = \
    (pre_brand_in_cate.join(pre_brand_cust_pen, "brand_name", "inner")
     .withColumn("switching_idx", F.col("prop_cust_switch")/F.col("cust_pen"))
     .join(combine_hier,  "brand_name", "inner")
     .orderBy(F.col("prop_cust_switch").desc_nulls_last())
     .withColumnRenamed("brand_name", "pre_brand_name")
    )
    # switch_pen.display()

    return switch_pen

def get_cust_sku_switching(cmp: CampaignEval):
    """Media evaluation solution, customer sku switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    switching_lv = cmp.params["cate_lvl"]
    cust_purchased_exposure_count = activated.get_cust_by_mech_exposed_purchased(cmp,
                                                                                 prd_scope_df = cmp.feat_sku,
                                                                                 prd_scope_nm = "sku")
    
    sku_activated = cust_purchased_exposure_count.select("household_id").drop_duplicates()

    feat_list = cmp.feat_sku.toPandas()["upc_id"].to_numpy().tolist()

    #---- Helper fn
    #---- Main
    print("-"*80)
    print("Customer switching SKU for 'OFFLINE' + 'ONLINE'")
    print(f"Switching within : {switching_lv.upper()}")
    print("Customer Movement consider only Feature SKU activated")
    print("-"*80)

    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col_nm}")
    print("-"*80)

    if switching_lv == "subclass":
        cat_df = sclass_df
    else:
        cat_df = class_df

    prod_desc = cmp.spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()

    # (PPP+Pre) vs Dur Category Sales of each household_id
    ## Windows aggregate style
    txn_per_dur_cat_sale = \
        (txn
            .where(F.col('household_id').isNotNull())
            .where(F.col(period_wk_col_nm).isin(['pre', 'ppp', 'dur']))
            .join(cat_df, "upc_id", "inner")
            .withColumn('pre_cat_sales', F.when( F.col(period_wk_col_nm).isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('dur_cat_sales', F.when( F.col(period_wk_col_nm).isin(['dur']), F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('cust_tt_pre_cat_sales', F.sum(F.col('pre_cat_sales')).over(Window.partitionBy('household_id') ))
            .withColumn('cust_tt_dur_cat_sales', F.sum(F.col('dur_cat_sales')).over(Window.partitionBy('household_id') ))
            )

    txn_cat_both = txn_per_dur_cat_sale.where( (F.col('cust_tt_pre_cat_sales')>0) & (F.col('cust_tt_dur_cat_sales')>0) )

    txn_cat_both_sku_only_dur = \
    (txn_cat_both
        .withColumn('pre_sku_sales',
                    F.when( (F.col(period_wk_col_nm).isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('dur_sku_sales',
                    F.when( (F.col(period_wk_col_nm).isin(['dur'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
        .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
        .where( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
    )

    n_cust_switch_sku = \
    (txn_cat_both_sku_only_dur
        .where(F.col('pre_cat_sales')>0) # only other products
        .join(sku_activated, 'household_id', 'inner')
        .groupBy('upc_id')
        .agg(F.countDistinct('household_id').alias('custs'))
        .join(prod_desc, 'upc_id', 'left')
        .orderBy('custs', ascending=False)
    )

    return n_cust_switch_sku
