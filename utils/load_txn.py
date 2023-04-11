import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np
from pathlib import Path

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))

def load_txn(cmp: CampaignEval,
             txn_mode: str = ""):
    """
    """
    
    if txn_mode == "create_new":
        snap = edm_class.txnItem(end_wk_id=cmp.cmp_en_wk, 
                                str_wk_id=cmp.ppp_st_wk, 
                                manuf_name=False, 
                                head_col_select=["transaction_uid", "date_id", "store_id", "channel", "pos_type", "pos_id"],
                                item_col_select=['transaction_uid', 'store_id', 'date_id', 'upc_id', 'week_id', 
                                                'net_spend_amt', 'unit', 'customer_id', 'discount_amt', 'cc_flag'],
                                prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
                                                'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                                'department_code', 'section_code', 'class_code', 'subclass_code'])
        
        txn = snap.txn
        
        brand_list = brand_df.select("brand_nm").drop_duplicates().toPandas()["brand_nm"].tolist()
        brand_list.sort()
        if len(brand_list) > 1:
            txn_all = txn_all.withColumn("brand_name", F.when(F.col("brand_name").isin(brand_list), F.lit(brand_list[0])).otherwise(F.col("brand_name")))
    
        try:
            
            txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
            print(f'Load data table for period : Ppp - Pre - Gap - Cmp, All store All format \n from : tdm_seg.media_campaign_eval_txn_data_{cmp_id}')

        except:
            print(f'Create intermediate transaction table for period Prior - Pre - Dur , all store format : tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
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

            txn_all.write.saveAsTable(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
            ## Pat add, delete dataframe before re-read
            del txn_all
            ## Re-read from table
            txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
