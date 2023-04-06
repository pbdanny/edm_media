import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np

from utils import period_cal
from utils.DBPath import DBPath
from utils import logger

class CampaignConfigFile:
    
    def __init__(self, source_file):
        self.source_config_file = source_file
        self.cmp_config_file = DBPath(source_file)
        self.cmp_config_file_name = self.cmp_config_file.name
        self.cmp_config_df = pd.read_csv(self.cmp_config_file.file_api())
        self.cmp_config_df.insert(loc=0, column='row_num', value=self.cmp_config_df.index + 1)
        self.total_rows = self.cmp_config_df.shape[0]
        self.cmp_inputs_files = self.cmp_config_file.parent/"inputs_files"
        self.cmp_output = self.cmp_config_file.parents[1]
        pass
        
    def __repr__(self):
        return f"CampaignConfigFile class, source file = '{self.source_config_file}'"
    
    def display_details(self):
        display(self.cmp_config_df)
        pass
      
    def search_details(self, 
                       column: str,
                       search_key: str):
        return self.cmp_config_df[self.cmp_config_df[column].str.contains(search_key)]

class CampaignParams:
    
    def __init__(self, config_file, cmp_row_no):
        self.cmp_config_file = config_file.cmp_config_file
        self.all_cmp_df = config_file.cmp_config_df
        self.all_cmp_max_row = config_file.total_rows
        self.cmp_inputs_files = config_file.cmp_inputs_files
        
        if cmp_row_no > self.all_cmp_max_row:
            raise ValueError(f"Campaign input have only '{self.all_cmp_max_row}' rows, request {cmp_row_no} is not available")
        elif cmp_row_no < 0:
            raise ValueError(f"Campaign input have only '{self.all_cmp_max_row}' rows, request {cmp_row_no} is not available")
        else:
            self.row_no = cmp_row_no
            self.params = (self.all_cmp_df
                           .applymap(lambda x : x.strip() if type(x)==str else x)
                           .iloc[self.row_no - 1]
                           .replace(np.nan, None)
                           .replace('', None)
                           ).to_dict()
            self.output_path = config_file.cmp_output/self.params["cmp_month"]/self.params["cmp_nm"]
            self.std_input_path = config_file.cmp_output.parent/"00_std_inputs"
        pass
            
    def __repr__(self):
        return f"CampaignParams class, config file : '{self.cmp_input_file}'\nRow number : {self.row_no}"
    
    def display_details(self):
        pprint.pp(self.params)
        pass
    
class CampaignEval(CampaignParams):
    
    def __init__(self, config_file, cmp_row_no):
        
        super().__init__(config_file, cmp_row_no)
        self.spark = SparkSession.builder.appName("campaingEval").getOrCreate()

        self.store_fmt = self.params["store_fmt"]
        self.wk_type = self.params["wk_type"]
        
        self.cmp_nm = self.params["cmp_nm"]
        self.cmp_start = self.params["cmp_start"]
        self.cmp_end = self.params["cmp_end"]
        self.media_fee = self.params["media_fee"]
        
        self.sku_file = self.cmp_inputs_files/f"upc_list_{self.params['cmp_id']}.csv"
        self.target_store_file = self.cmp_inputs_files/f"target_store_{self.params['cmp_id']}.csv"
         
        self.resrv_store_file = self.std_input_path/f"{self.params['resrv_store_file']}"
        self.use_reserved_store = bool(self.params["use_reserved_store"])
        
        self.custom_ctrl_store_file = self.cmp_inputs_files/f"control_store_{self.params['cmp_id']}.csv"
        
        self.adjacency_file = self.std_input_path/f"{self.params['adjacency_file']}"
        self.svv_table = self.params["svv_table"]
        self.purchase_cyc_table = self.params["purchase_cyc_table"]
        
        self.load_period()
        pass
        
    def __repr__(self):
        return f"CampaignEval class \nConfig file : '{self.cmp_config_file}'\nRow number : {self.row_no}"
    
    def load_period(self):         
        self.cmp_st_wk = period_cal.wk_of_year_ls(self.cmp_start)
        self.cmp_en_wk = period_cal.wk_of_year_ls(self.cmp_end)
        self.cmp_st_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_start)
        self.cmp_en_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_end)
        self.gap_start_date = self.params["gap_start_date"]
        self.gap_end_date = self.params["gap_end_date"]
        
        if ( self.gap_start_date is None ) & ( self.gap_end_date is None ):
            print(f'No Gap Week for campaign : {self.cmp_nm}')
            self.gap_flag    = False
            self.chk_pre_wk  = self.cmp_st_wk
            self.chk_pre_dt  = self.cmp_start
            
        elif ( self.gap_start_date is not None ) & ( self.gap_end_date is not None ):    
            print(f'\n Campaign {self.cmp_nm} has gap period between : {self.gap_start_date} and {self.gap_end_date} \n')
            
            ## fis_week
            self.gap_st_wk   = period_cal.wk_of_year_ls(self.gap_start_date)
            self.gap_en_wk   = period_cal.wk_of_year_ls(self.gap_end_date)

            ## promo
            self.gap_st_promo_wk  = period_cal.wk_of_year_promo_ls(self.gap_start_date)
            self.gap_en_promo_wk  = period_cal.wk_of_year_promo_ls(self.gap_end_date)

            self.gap_flag         = True    

            self.chk_pre_dt       = self.gap_start_date
            self.chk_pre_wk       = self.gap_st_wk
            self.chk_pre_promo_wk = self.gap_st_promo_wk

        else:
            print('Incorrect gap period. Please recheck - Code will skip !! \n')
            print(f'Received Gap = {self.gap_start_date} + " and " + {self.gap_end_date}')
            raise Exception("Incorrect Gap period value please recheck !!")   

        self.pre_en_date = (datetime.strptime(self.chk_pre_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
        self.pre_en_wk   = period_cal.wk_of_year_ls(self.pre_en_date)
        self.pre_st_wk   = period_cal.week_cal(self.pre_en_wk, -12)                       ## get 12 week away from end week -> inclusive pre_en_wk = 13 weeks
        self.pre_st_date = period_cal.f_date_of_wk(self.pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data
        
        ## promo week
        self.pre_en_promo_wk = period_cal.wk_of_year_promo_ls(self.pre_en_date)
        self.pre_st_promo_wk = period_cal.promo_week_cal(self.pre_en_promo_wk, -12)   

        self.ppp_en_wk       = period_cal.week_cal(self.pre_st_wk, -1)
        self.ppp_st_wk       = period_cal.week_cal(self.ppp_en_wk, -12)
        
        ## promo week
        self.ppp_en_promo_wk = period_cal.promo_week_cal(self.pre_st_promo_wk, -1)
        self.ppp_st_promo_wk = period_cal.promo_week_cal(self.ppp_en_promo_wk, -12)

        self.ppp_st_date = period_cal.f_date_of_wk(self.ppp_en_wk).strftime('%Y-%m-%d')
        self.ppp_en_date = period_cal.f_date_of_wk(self.ppp_st_wk).strftime('%Y-%m-%d')

        ## Add setup week type parameter

        if self.params["wk_type"] == 'fis_wk':
            self.wk_tp     = 'fiswk'
            self.week_type = 'fis_week'    
        elif self.params["wk_type"] == 'promo_wk':
            self.wk_tp     = 'promowk'
            self.week_type = 'promo_week'    
    
    def load_store(self):
        
        def _get_rest_store(str_fmt: str, target_store: SparkDataFrame):
            store_dim_c = self.spark.table("tdm.v_store_dim_c")
            if str_fmt in ["hde", "hyper"]:
                target_format = store_dim_c.where(F.col("format_id").isin([1, 2, 3]))
            elif str_fmt in ["talad", "super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([4]))
            elif str_fmt in ["gofresh", "mini_super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([5]))
            else:
                target_format = store_dim_c.where(F.col("format_id").isin([1,2,3,4,5]))
            return target_format.join(target_store, "store_id", "leftanti").select("store_id").drop_duplicates()
        
        self.target_store = self.spark.read.csv( (self.target_store_file).spark_api(), header=True, inferSchema=True)
        
        try:
            _ctrl_store_from_custom_file = self.spark.read.csv( (self.custom_ctrl_store_file).spark_api(), header=True, inferSchema=True)
            self.params["reserved_store_type"] = "Custom control store file"
            self.ctrl_store = _ctrl_store_from_custom_file
            
        except Exception as e:
            if self.params["resrv_store_class"] is not None:
                _ctrl_store_from_class = (self.spark.read.csv( (self.resrv_store_file).spark_api(), header=True, inferSchema=True)
                                          .where(F.col("class_code")==self.params["resrv_store_class"])
                                          .select("store_id")
                                         )
                self.params["reserved_store_type"] = "Reserved store class"
                self.ctrl_store = _ctrl_store_from_class

            else:
                _ctrl_store_rest = _get_rest_store(self.params["store_fmt"], self.target_store)
                self.params["reserved_store_type"] = "Rest"
                self.ctrl_store = _ctrl_store_rest
        pass
                                                                                   
    def load_prod(self):
        self.feat_sku = self.spark.read.csv( (self.sku_file).spark_api(), header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c")
        feat_subclass = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("subclass_code").drop_duplicates()
        feat_class = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("class_code").drop_duplicates()
        self.feat_subclass_sku = prd_dim_c.join(feat_subclass, "subclass_code").select("upc_id").drop_duplicates()
        self.feat_class_sku = prd_dim_c.join(feat_class, "class_code").select("upc_id").drop_duplicates()
        if self.params["cate_lvl"].lower() in ["class"]:
            self.feat_cate_sku = self.feat_class_sku
        elif self.params["cate_lvl"].lower() in ["subclass"]:
            self.feat_cate_sku = self.feat_class_sku
        else:
            self.feat_cate_sku = None
        pass
    
    def load_aisle(self):
        
        def _convert_param_to_list(param_name: str) -> List:
            if self.params[param_name] is not None:
                param = self.params["cross_cate_cd"]
                if param.find("[") != -1:
                    return literal_eval(param)
                elif param.find(",") != -1:
                    return str.split(param)
                else:
                    return [param]
            else:
                return []
                    
        self.load_prod()
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c")
        self.cross_cate_cd_list = _convert_param_to_list("cross_cate_cd")
        aisle_master = self.spark.read.csv( self.adjacency_file.spark_api(), header=True, inferSchema=True)
        if not self.cross_cate_cd_list:
            feat_subclass = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("subclass_code").drop_duplicates()
            aisle_group = aisle_master.join(feat_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
        else:
            x_subclass = self.spark.createDataFrame(pd.DataFrame(data=self.cross_cate_cd_list, columns=["subclass_code"])).drop_duplicates()
            aisle_group = aisle_master.join(x_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
        pass
    
    def load_txn(self):
        try:
            self.txn = self.spark.table(f"tdm_seg.media_campaign_eval_txn_data_{self.params['cmp_id'].lower()}")
        except Exception as e:
            logger.logger("No snapped transaction")
        pass