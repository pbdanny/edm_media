import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np
from pathlib import Path

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
        return self.cmp_config_df.display()

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

    def convert_param_to_list(self,
                              param_name: str) -> List:
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

    def __init__(self, config_file, cmp_row_no):

        super().__init__(config_file, cmp_row_no)
        self.spark = SparkSession.builder.appName("campaingEval").getOrCreate()

        self.store_fmt = self.params["store_fmt"].lower()
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
        self.load_target_store()
        self.load_control_store()
        self.load_prod()
        self.load_aisle()
        # self.load_txn()

        pass

    def __repr__(self):
        return f"CampaignEval class \nConfig file : '{self.cmp_config_file}'\nRow number : {self.row_no}"

    def load_period(self):
        self.cmp_st_wk = period_cal.wk_of_year_ls(self.cmp_start)
        self.params["cmp_st_wk"] = self.cmp_st_wk
        self.cmp_en_wk = period_cal.wk_of_year_ls(self.cmp_end)
        self.params["cmp_en_wk"] = self.cmp_en_wk
        self.cmp_st_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_start)
        self.params["cmp_st_promo_wk"] = self.cmp_st_promo_wk
        self.cmp_en_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_end)
        self.params["cmp_en_promo_wk"] = self.cmp_en_promo_wk
        self.gap_start_date = self.params["gap_start_date"]
        self.gap_end_date = self.params["gap_end_date"]

        if ( self.gap_start_date is None ) & ( self.gap_end_date is None ):
            print(f'No Gap Week for campaign : {self.cmp_nm}')
            self.gap_flag    = False
            chk_pre_dt  = self.cmp_start
            chk_pre_wk  = self.cmp_st_wk

        elif ( self.gap_start_date is not None ) & ( self.gap_end_date is not None ):
            print(f'Campaign {self.cmp_nm} has gap period between : {self.gap_start_date} and {self.gap_end_date}')

            ## fis_week
            self.gap_st_wk   = period_cal.wk_of_year_ls(self.gap_start_date)
            self.gap_en_wk   = period_cal.wk_of_year_ls(self.gap_end_date)

            ## promo
            self.gap_st_promo_wk  = period_cal.wk_of_year_promo_ls(self.gap_start_date)
            self.gap_en_promo_wk  = period_cal.wk_of_year_promo_ls(self.gap_end_date)

            self.gap_flag         = True

            chk_pre_dt       = self.gap_start_date
            chk_pre_wk       = self.gap_st_wk
            chk_pre_promo_wk = self.gap_st_promo_wk

        else:
            print('Incorrect gap period. Please recheck - Code will skip !! \n')
            print(f'Received Gap = {self.gap_start_date} + " and " + {self.gap_end_date}')
            raise Exception("Incorrect Gap period value please recheck !!")

        self.pre_en_date = (datetime.strptime(chk_pre_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
        # pre end week migh overlap with cmp st wk
        self.pre_en_wk   = period_cal.wk_of_year_ls(self.pre_en_date)
        self.params["pre_en_wk"] = self.pre_en_wk
        self.pre_st_wk   = period_cal.week_cal(self.pre_en_wk, -12)                       ## get 12 week away from end week -> inclusive pre_en_wk = 13 weeks
        self.params["pre_st_wk"] = self.pre_st_wk
        self.pre_st_date = period_cal.f_date_of_wk(self.pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data

        ## promo week
        self.pre_en_promo_wk = period_cal.wk_of_year_promo_ls(self.pre_en_date)
        self.params["pre_en_promo_wk"] = self.pre_en_promo_wk
        self.pre_st_promo_wk = period_cal.promo_week_cal(self.pre_en_promo_wk, -12)
        self.params["pre_st_promo_wk"] = self.pre_st_promo_wk

        self.ppp_en_wk       = period_cal.week_cal(self.pre_st_wk, -1)
        self.params["ppp_en_wk"] = self.ppp_en_wk
        self.ppp_st_wk       = period_cal.week_cal(self.ppp_en_wk, -12)
        self.params["ppp_st_wk"] = self.ppp_st_wk

        ## promo week
        self.ppp_en_promo_wk = period_cal.promo_week_cal(self.pre_st_promo_wk, -1)
        self.params["ppp_en_promo_wk"] = self.ppp_en_promo_wk
        self.ppp_st_promo_wk = period_cal.promo_week_cal(self.ppp_en_promo_wk, -12)
        self.params["ppp_st_promo_wk"] = self.ppp_st_promo_wk
        
        self.ppp_st_date = period_cal.f_date_of_wk(self.ppp_en_wk).strftime('%Y-%m-%d')
        self.ppp_en_date = period_cal.f_date_of_wk(self.ppp_st_wk).strftime('%Y-%m-%d')

        ## Add setup week type parameter

        if self.params["wk_type"] == 'fis_wk':
            self.wk_tp     = 'fiswk'
            self.week_type = 'fis_week'
        elif self.params["wk_type"] == 'promo_wk':
            self.wk_tp     = 'promowk'
            self.week_type = 'promo_week'
        pass

    def load_target_store(self):
        self.target_store = self.spark.read.csv( self.target_store_file.spark_api(), header=True, inferSchema=True)
        pass

    def check_target_store(self):
        """Base on store group name,
        - if HDE / Talad -> count check test vs total store
        - if GoFresh -> adjust 'store_region' in txn, count check
        """
        from typing import List
        from pyspark.sql import DataFrame as SparkDataFrame

        print('-'*80)
        print('Count check store region & Combine store region for GoFresh')
        print(f'Store format defined : {self.store_fmt}')

        def _get_all_and_test_store(format_id_list: List):
            """Get universe store count, based on format definded
            If store region Null -> Not show in count
            """
            all_store_count_region = \
            (self.spark.table('tdm.v_store_dim')
             .where(F.col('format_id').isin(format_id_list))
             .select('store_id', 'store_name', F.col('region').alias('store_region'))
             .drop_duplicates()
             .fillna('Unidentified', subset='store_region')
             .groupBy('store_region')
             .agg(F.count('store_id').alias(f'total_{self.store_fmt}'))
            )

            test_store_count_region = \
            (self.spark.table('tdm.v_store_dim')
             .select('store_id','store_name', 'format_id', F.col('region').alias('store_region'))
             .drop_duplicates()
             .join(self.target_store, 'store_id', 'left_semi')
             .withColumn("store_format_name", F.when(F.col("format_id").isin([1,2,3]), "target_hyper")
                                               .when(F.col("format_id").isin([4]), "target_super")
                                               .when(F.col("format_id").isin([5]), "target_mini_super")
                                               .otherwise("target_other_fmt"))
             .groupBy('store_region')
             .pivot("store_format_name")
             .agg(F.count('store_id').alias(f'test_store_count'))
            )

            return all_store_count_region, test_store_count_region

        self.load_target_store()

        if self.store_fmt in ["hde", "hyper"]:
            all_store_count_region, test_store_count_region = _get_all_and_test_store([1,2,3])

        elif self.store_fmt in ["talad", "super"]:
            all_store_count_region, test_store_count_region = _get_all_and_test_store([4])

        elif self.store_fmt in ["gofresh", "mini_super"]:
            #---- Adjust Transaction
            print('GoFresh : Combine store_region West + Central in variable "txn_all"')
            print("GoFresh : Auto-remove 'Null' region")

            adjusted_store_region =  \
            (self.spark.table('tdm.v_store_dim')
            .withColumn('store_region', F.when(F.col('region').isin(['West','Central']), F.lit('West+Central'))
                                         .when(F.col('region').isNull(), F.lit('Unidentified'))
                                         .otherwise(F.col('region')))
            .drop("region")
            .drop_duplicates()
            )

            #---- Count Region
            all_store_count_region = \
            (adjusted_store_region
            .where(F.col('format_id').isin([5]))
            .select('store_id', 'store_name', 'store_region')
            .drop_duplicates()
            .fillna('Unidentified', subset='store_region')
            .groupBy('store_region')
            .agg(F.count('store_id').alias(f'total_{self.store_fmt}'))
            )

            test_store_count_region = \
            (adjusted_store_region
             .select('store_id','store_name', 'format_id')
             .drop_duplicates()
             .join(self.target_store, 'store_id', 'left_semi')
             .withColumn("store_format_name", F.when(F.col("format_id").isin([1,2,3]), "target_hyper")
                                               .when(F.col("format_id").isin([4]), "target_super")
                                               .when(F.col("format_id").isin([5]), "target_mini_super")
                                               .otherwise("target_other_fmt"))
             .groupBy('store_region')
             .pivot("store_format_name")
             .agg(F.count('store_id').alias(f'test_store_count'))
            )

        else:
            print(f'Unknown store format group name : {self.store_fmt}')
            return None

        test_vs_all_store_count = all_store_count_region.join(test_store_count_region, 'store_region', 'left').orderBy('store_region')

        return test_vs_all_store_count

    def load_control_store(self,
                           control_store_mode: str = ''):
        def _resrv():
            self.params["control_store_source"] = "Reserved store class"
            self.control_store = (self.spark.read.csv( (self.resrv_store_file).spark_api(), header=True, inferSchema=True)
                                            .where(F.col("class_code")==self.params["resrv_store_class"])
                                            .select("store_id"))
            pass

        def _custom():
            self.params["control_store_source"] = "Custom control store file"
            self.control_store = self.spark.read.csv( (self.custom_ctrl_store_file).spark_api(), header=True, inferSchema=True)
            pass

        def _rest():
            self.params["control_store_source"] = "rest"
            store_dim_c = self.spark.table("tdm.v_store_dim_c")

            if self.store_fmt in ["hde", "hyper"]:
                target_format = store_dim_c.where(F.col("format_id").isin([1, 2, 3]))
            elif self.store_fmt in ["talad", "super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([4]))
            elif self.store_fmt in ["gofresh", "mini_super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([5]))
            else:
                target_format = store_dim_c.where(F.col("format_id").isin([1,2,3,4,5]))
                
            self.control_store = target_format.join(self.target_store, "store_id", "leftanti").select("store_id").drop_duplicates()
            pass

        self.load_target_store()

        if control_store_mode == '':
            if self.params["resrv_store_class"] is not None:
                _resrv()
            elif Path(self.custom_ctrl_store_file.file_api()).is_file():
                _custom()
            else:
                _rest()
        else:
            if control_store_mode == "reserved_store":
                _resrv()
            elif control_store_mode == "custom_control_file":
                _custom()
            elif control_store_mode == "rest":
                _rest()
            else:
                _rest()
        pass

    def load_prod(self):
        self.feat_sku = self.spark.read.csv( (self.sku_file).spark_api(), header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c")
        self.feat_subclass_code = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("subclass_code").drop_duplicates()
        self.feat_class_code = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("class_code").drop_duplicates()
        self.feat_subclass_sku = prd_dim_c.join(self.feat_subclass_code, "subclass_code").select("upc_id").drop_duplicates()
        self.feat_class_sku = prd_dim_c.join(self.feat_class_code, "class_code").select("upc_id").drop_duplicates()
        
        if self.params["cate_lvl"].lower() in ["class"]:
            self.feat_cate_sku = self.feat_class_sku
            feat_brand = prd_dim_c.join(self.feat_class_code, "class_code").join(self.feat_sku, "upc_id")
            self.feat_brand_nm = feat_brand.select("brand_name").drop_duplicates()
            self.feat_brand_sku = feat_brand.select("upc_id").drop_duplicates()
        elif self.params["cate_lvl"].lower() in ["subclass"]:
            self.feat_cate_sku = self.feat_class_sku
            feat_brand = prd_dim_c.join(self.feat_subclass_code, "subclass_code").join(self.feat_sku, "upc_id")
            self.feat_brand_nm = feat_brand.select("brand_name").drop_duplicates()
            self.feat_brand_sku = feat_brand.select("upc_id").drop_duplicates()
        else:
            self.feat_cate_sku = None
            self.feat_brand_nm = None
            self.feat_brand_sku = None
        pass

    def load_aisle(self,
                   aisle_mode: str = ''):
        def _homeshelf():
            self.params["aisle_mode"] = "homeshelf"
            feat_subclass = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("subclass_code").drop_duplicates()
            aisle_group = aisle_master.join(feat_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
            pass
        def _x_cat():
            self.params["aisle_mode"] = "cross_cate"
            x_subclass = self.spark.createDataFrame(pd.DataFrame(data=self.cross_cate_cd_list, columns=["subclass_code"])).drop_duplicates()
            aisle_group = aisle_master.join(x_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
            pass
        def _store():
            self.params["aisle_mode"] = "total_store"
            self.aisle_sku = prd_dim_c.select("upc_id").drop_duplicates()
            pass
    
        self.load_prod()
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c")
        self.cross_cate_cd_list = self.convert_param_to_list("cross_cate_cd")
        aisle_master = self.spark.read.csv( self.adjacency_file.spark_api(), header=True, inferSchema=True)
        
        if aisle_mode == '':
            
            if not self.cross_cate_cd_list:
                _homeshelf()
            else:
                _x_cat()
        else:
            if aisle_mode == "homeshelf":
                _homeshelf()
            elif aisle_mode == "cross_cat":
                _x_cat()
            else:
                _store()
        pass