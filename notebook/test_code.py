# Databricks notebook source
import os
import sys
sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))

# COMMAND ----------

from utils import period_cal
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_makro.csv")
conf.display_details()

# COMMAND ----------

conf.cmp_config_df.applymap(lambda x : x.strip() if type(x)==str else x)

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=1)

# COMMAND ----------

timport os
import sys
from pathlib import Path
import pandas as pd
from ast import literal_eval

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))

# COMMAND ----------

from utils import load_config
from utils import logger
from utils import period_cal

# COMMAND ----------

import pprint

from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np

class DBPath(_Path_):
    
    _flavour = _windows_flavour if os.name == 'nt' else _posix_flavour
    
    def __init__(self, input_file):
        super().__init__()
        pass
        
    def __repr__(self):
        return f"DBPath class : {self.as_posix()}"
    
    def file_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("/dbfs"+rm_first_5_str)
    
    def spark_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("dbfs:"+rm_first_5_str)
    
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
            self.params = self.all_cmp_df.iloc[self.row_no - 1].replace(np.nan, None).to_dict()
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

        self.store_fmt = self.params["store_fmt"]
        self.wk_type = self.params["wk_type"]
        
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
    
    def load_store(self):
        
        def _get_rest_store(str_fmt: str, target_store: SparkDataFrame):
            store_dim_c = spark.table("tdm.v_store_dim_c")
            if str_fmt in ["hde", "hyper"]:
                target_format = store_dim_c.where(F.col("format_id").isin([1, 2, 3]))
            elif str_fmt in ["talad", "super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([4]))
            elif str_fmt in ["gofresh", "mini_super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([5]))
            else:
                target_format = store_dim_c.where(F.col("format_id").isin([1,2,3,4,5]))
            return target_format.join(target_store, "store_id", "leftanti").select("store_id").drop_duplicates()
        
        self.target_store = spark.read.csv( (self.target_store_file).spark_api(), header=True, inferSchema=True)
        
        try:
            _ctrl_store_from_custom_file = spark.read.csv( (self.custom_ctrl_store_file).spark_api(), header=True, inferSchema=True)
            self.params["reserved_store_type"] = "Custom control store file"
            self.ctrl_store = _ctrl_store_from_custom_file
            
        except Exception as e:
            if self.params["resrv_store_class"] is not None:
                _ctrl_store_from_class = (spark.read.csv( (self.resrv_store_file).spark_api(), header=True, inferSchema=True)
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
        self.feat_sku = spark.read.csv( (self.sku_file).spark_api(), header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
        prd_dim_c = spark.table("tdm.v_prod_dim_c")
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
        
        def _convert_cross_cate_cd_to_list():
            if self.params["cross_cate_cd"] is not None:
                self.cross_cate_cd = self.params["cross_cate_cd"]
                if self.cross_cate_cd.find("[") != -1:
                    return literal_eval(self.cross_cate_cd)
                elif self.cross_cate_cd.find(",") != -1:
                    return str.split(self.cross_cate_cd)
                else:
                    return [self.cross_cate_cd]
            else:
                return []
                    
        self.load_prod()
        prd_dim_c = spark.table("tdm.v_prod_dim_c")
        self.cross_cate_cd_list = _convert_cross_cate_cd_to_list()
        aisle_master = spark.read.csv( self.adjacency_file.spark_api(), header=True, inferSchema=True)
        if not self.cross_cate_cd_list:
            feat_subclass = prd_dim_c.join(self.feat_sku, "upc_id", "inner").select("subclass_code").drop_duplicates()
            aisle_group = aisle_master.join(feat_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
        else:
            x_subclass = spark.createDataFrame(pd.DataFrame(data=self.cross_cate_cd_list, columns=["subclass_code"])).drop_duplicates()
            aisle_group = aisle_master.join(x_subclass, "subclass_code", "inner").select("group").drop_duplicates()
            aisle_subclass = aisle_master.join(aisle_group, "group", "inner").select("subclass_code").drop_duplicates()
            self.aisle_sku = prd_dim_c.join(aisle_subclass, "subclass_code", "inner").select("upc_id").drop_duplicates()
        pass
    
    def load_txn(self):
        try:
            self.txn = spark.table(f"tdm_seg.media_campaign_eval_txn_data_{self.params['cmp_id'].lower()}")
        except Exception as e:
            logger.logger("No snapped transaction")
        pass

# COMMAND ----------

conf = CampaignConfigFile("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_makro.csv")
conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=1)

# COMMAND ----------

cmp.display_details()

# COMMAND ----------

cmp

# COMMAND ----------

cmp.cmp_st_promo_wk

# COMMAND ----------

(txn_offline
 .join(cmp.feat_class_sku, "upc_id", "inner")
 .join(test_ctrl_str, "store_id", "inner")
 .groupBy("store_type","store_id", "week_id")
 .agg(F.sum("net_spend_amt").alias("sales"),
      F.sum("unit").alias("units"),
      F.count_distinct("transaction_uid").alias("visits"),
      ( F.sum("net_spend_amt")/F.sum("unit") ).alias("ppu")
     )
).display()

# COMMAND ----------

resrv_store_class = "13_77_166_1"
wk_id_col_nm = "week_id"
cate_lvl = "subclass"

pre_en_wk = 202229
wk_type = "fis_week"
txn_all = spark.table("tdm_seg.media_campaign_eval_txn_data_2022_0592_m01m")
feat_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/upc_list_2022_0592_M01M.csv", header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
trg_str_df = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0592_M01M.csv", header=True, inferSchema=True)

brand_df = (spark.table(TBL_PROD)
            .join(feat_sf, "upc_id")
            .select("brand_name", "subclass_code").drop_duplicates()
            .join(spark.table(TBL_PROD), ["brand_name", "subclass_code"])
            .select("upc_id")
            .drop_duplicates()
           )

sclass_df = (spark.table(TBL_PROD)
             .join(feat_sf, "upc_id")
             .select("subclass_code").drop_duplicates()
             .join(spark.table(TBL_PROD), ["subclass_code"])
             .select("upc_id")
             .drop_duplicates()
           )
# resrv_store_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/00_std_inputs/reserved_store_HDE_20211031_master_withcode.csv", header=True, inferSchema=True).where(F.col("class_code")==resrv_store_class).select("store_id")

u_ctl_str_df = spark.read.csv("dbfs:/FileStore/media/reserved_store/reserved_store_HDE_20221030.csv", header=True, inferSchema=True).where(F.col("class_code")==resrv_store_class).select("store_id")

db

# COMMAND ----------

def get_store_matching_across_region(
        txn: SparkDataFrame,
        pre_en_wk: int,
        wk_type: str,
        feat_sf: SparkDataFrame,
        brand_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        test_store_sf: SparkDataFrame,
        reserved_store_sf: SparkDataFrame,
        matching_methodology: str = 'varience',
        dbfs_project_path: str = "") -> List:
    """
    Parameters
    ----------
    txn: SparkDataFrame

    pre_en_wk: End pre_period week --> yyyymm
        Pre period end week

    wk_type: "fis_week" or "promo_week"

    feat_sf: SparkDataFrame
        Features upc_id

    brand_df: SparkDataFrame
        Feature brand upc_id (brand in switching level)

    sclass_df: SparkDataFrame
        Featurs subclass upc_id (subclass in switching level)

    test_store_sf: SparkDataFrame
        Media features store list

    reserved_store_sf: SparkDataFrame
        Customer picked reserved store list, for finding store matching -> control store

    matching_methodology: str, default 'varience'
        'varience', 'euclidean', 'cosine_distance'

    dbfs_project_path: str, default = ''
        project path = os.path.join(eval_path_fl, cmp_month, cmp_nm)

    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity

    #---- Helper fn
    def _get_wk_id_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo_week"]:
            wk_id_col_nm = "promoweek_id"
        elif wk_type in ["promozone"]:
            wk_id_col_nm = "promoweek_id"
        else:
            wk_id_col_nm = "week_id"

        return wk_id_col_nm

    def _get_min_wk_sales(prod_scope_df: SparkDataFrame):
        """Count number of week sales by store, return the smallest number of target store, control store
        """
        # Min sales week test store
        txn_match_trg  = (txn
                          .join(test_store_sf.drop("store_region_orig", "store_region"), "store_id", "inner")
                          .join(prod_scope_df, "upc_id", "leftsemi")
                          .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                          .where(F.col("offline_online_other_channel") == 'OFFLINE')
                          .select(txn['*'],
                                F.col("store_region").alias('store_region_new'),
                                F.lit('test').alias('store_type'),
                                F.col("mech_name").alias('store_mech_set'))
                          )
        trg_wk_cnt_df = txn_match_trg.groupBy(F.col("store_id")).agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        trg_min_wk = trg_wk_cnt_df.agg(F.min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]

        # Min sale week ctrl store
        txn_match_ctl = (txn
                         .join(reserved_store_sf.drop("store_region_orig", "store_region"), "store_id", 'inner')
                         .join(prod_scope_df, "upc_id", "leftsemi")
                         .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                         .where(F.col("offline_online_other_channel") == 'OFFLINE')
                         .select(txn['*'],
                                 F.col("store_region").alias('store_region_new'),
                                 F.lit('ctrl').alias('store_type'),
                                 F.lit('No Media').alias('store_mech_set'))
                         )

        ctl_wk_cnt_df = txn_match_ctl.groupBy("store_id").agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        ctl_min_wk = ctl_wk_cnt_df.agg(F.min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]

        return int(trg_min_wk), txn_match_trg, int(ctl_min_wk), txn_match_ctl

    def _get_comp_score(txn: SparkDataFrame,
                        wk_id_col_nm: str):
        """Calculate weekly avg composite score by store_id
        Composite score = (normalized sales + normalize customer) / 2
        Normalize = MinMaxScalar, normalized each week separately

        Parameter
        ----
        txn : SparkDataFrame
            transaction of product level (feature sku, feature brand, subclass) for score calculation.

        wk_id_col_nm : str
            Name of week column (week_id / promo_week_id) to calculate weekly kpi.

        """
        def __get_std(df: PandasDataFrame) -> PandasDataFrame:
            from sklearn.preprocessing import StandardScaler, MinMaxScaler

            scalar = MinMaxScaler() # StandardScaler()

            scaled = scalar.fit_transform(df)
            scaled_df = pd.DataFrame(data=scaled, index=df.index, columns=df.columns)

            return scaled_df

        txn = txn.withColumn("store_id", F.col("store_id").cast(StringType()))

        sales = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
        custs = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.count_distinct('household_id').alias('custs')).fillna(0)

        sales_df = to_pandas(sales).astype({'store_id':str}).set_index("store_id")
        custs_df = to_pandas(custs).astype({'store_id':str}).set_index("store_id")

        sales_scaled_df = __get_std(sales_df)
        custs_scaled_df = __get_std(custs_df)

        sales_unpv_df = sales_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_sales", var_name="week_id")
        custs_unpv_df = custs_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_custs", var_name="week_id")
        comb_df = pd.merge(sales_unpv_df, custs_unpv_df, how="outer", on=["store_id", "week_id"])
        comb_df["comp_score"] = (comb_df["std_sales"] + comb_df["std_custs"])/2

        return comb_df

    def __var_abs_dist(x: pd.Series, y: pd.Series):
        """Custom distance function "Varience"
        """
        import numpy as np
        var_abs = np.var(np.abs(x - y))
        return var_abs

    def __get_min_pair(data: np.ndarray,
                       test: pd.DataFrame,
                       ctrl: pd.DataFrame,
                       dist_nm: str):
        """Converted paired distance ndarray, row = test store x column = control store
        into PandasDataFrame and find min paired

        Parameter
        ----
        data : np.ndarray
            Paired distance matrix test x ctrl
        test : PandasDataFrame
            PandasDataFrame of test store id
        ctrl : PandasDataFrame
            PandasDataFrame of control store id
        dist_nm : str
            Name of distance measure
        """

        paired = pd.DataFrame(data=data, index=test.index, columns=ctrl.index)
        paired.index = paired.index.set_names("test_store_id")
        paired.columns = paired.columns.set_names("ctrl_store_id")
        # Turn wide format into long format with paired of ctrl - test store id, rename column 0 -> value
        paired_nm = paired.unstack().reset_index().rename(columns={0:"value"})

        # Add distance measure into column, sort column
        paired_nm["dist_measure"] = dist_nm
        paired_nm = paired_nm.loc[:, ["test_store_id", "ctrl_store_id", "dist_measure", "value"]]
        # Find lowest score of each test paired with ctrl
        min_paired = paired_nm.sort_values(["test_store_id", "value"], ascending=True).groupby(["test_store_id"]).head(1)

        return min_paired

    def _get_pair_min_dist(test: PandasDataFrame,
                           ctrl: PandasDataFrame,
                           dist_nm: str):
        """From test , ctrl calculate paired distance, keep lowest pair
        The test , ctrl DataFrame must have 'store_id' as index

        Parameter
        ----
        dist_nm : 'euclidean' or 'cosine'
        """
        from sklearn.metrics import pairwise_distances

        data = pairwise_distances(test, ctrl, metric=dist_nm)

        return __get_min_pair(data, test, ctrl, dist_nm)

    def _get_pair_min_dist_func(test: PandasDataFrame,
                                ctrl: PandasDataFrame,
                                dist_nm: str,
                                dist_func):
        """From test , ctrl calculate paired distance of custom distance function, keep lowest pair
        The test , ctrl DataFrame must have 'store_id' as index

        Parameter
        ----
        dist_nm : str

        dist_func : object
            custom distance function
        """

        import numpy as np

        data = np.zeros( (test.shape[0], ctrl.shape[0]) )

        for i, r in test.reset_index(drop=True).iterrows():
            for j, l in ctrl.reset_index(drop=True).iterrows():
                data[i, j] = dist_func(r, l)

        return __get_min_pair(data, test, ctrl, dist_nm)

    def _flag_outlier_mad(df: PandasDataFrame,
                          outlier_score_threshold: float = 3.0):
        """Flag outlier with MAD method
        Parameter
        ----
        df:
            PandasDataframe with column "value"

        outlier_score_threshold:
            threshold = 3.0

        Return
        ----
        : PandasDataFrame
            with column "flag_outlier"

        """
        NORMAL_DISTRIBUTION_CONSTANT = 1.4826

        flag = \
        (df
         .assign(median = lambda x : x["value"].median())
         .assign(abs_deviation = lambda x : np.abs(x["value"] - x["median"]) )
         .assign(mad = lambda x : NORMAL_DISTRIBUTION_CONSTANT * np.median( x["abs_deviation"] ) )
         .assign(outlier_score = lambda x : x["abs_deviation"]/x["mad"])
         .assign(flag_outlier = lambda x : np.where(x["outlier_score"]>=outlier_score_threshold, True, False))
        )

        return flag

    def __plt_pair(pair: PandasDataFrame,
                   store_comp_score: PandasDataFrame):
        """Comparison plot each pair of test-ctrl score
        """
        from matplotlib import pyplot as plt

        for i, row in pair.iterrows():
            test_score = store_comp_score[store_comp_score["store_id"]==row["test_store_id"]].loc[:,["week_id", "comp_score"]]
            ctrl_score = store_comp_score[store_comp_score["store_id"]==row["ctrl_store_id"]].loc[:,["week_id", "comp_score"]]
            fig, ax = plt.subplots()
            test_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Test Store : {row["test_store_id"]}')
            ctrl_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Ctrl Store : {row["ctrl_store_id"]}')
            plt.show()

        return None

    #--------------
    #---- Main ----
    #--------------

    print("-"*80)
    wk_id_col_nm = _get_wk_id_col_nm(wk_type=wk_type)
    print(f"Week_id based on column '{wk_id_col_nm}'")
    print('Matching performance only "OFFLINE" channel')

    pre_st_wk  = get_lag_wk_id(wk_id=pre_en_wk, lag_num=13, inclusive=True)

    # Find level for matching : feature sku / (feature) brand / (feature) subclass
    trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(feat_sf)

    if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
        match_lvl = 'feature sku'
        txn_matching = txn_match_trg.union(txn_match_ctl)
    else:
        trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(brand_df)
        if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
            match_lvl = 'brand'
            txn_matching = txn_match_trg.union(txn_match_ctl)
        else:
            trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(sclass_df)
            match_lvl = 'subclass'
            txn_matching = txn_match_trg.union(txn_match_ctl)

    print(f'This campaign will do matching at "{match_lvl.upper()}"')
    print(f'Matching method "{matching_methodology.upper()}"')
    print("-"*80)

    # Get composite score by store
    store_comp_score = _get_comp_score(txn_matching, wk_id_col_nm)
    store_comp_score_pv = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score").reset_index()

    # get store_id, store_region_new, store_type, store_mech_set
    store_type = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new", "store_type", "store_mech_set").drop_duplicates().toPandas()
    region_list = store_type["store_region_new"].dropna().unique()

    #---- New part : store_id as index
    store_comp_score_pv_id = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score")

    euc_list = []
    cos_list = []
    var_list = []

    # List of store_id in those region for test, ctrl
    test_store_id = store_type[(store_type["store_type"]=="test")]
    ctrl_store_id = store_type[(store_type["store_type"]=="ctrl")]

    # Store_id and score for test, ctrl
    test_store_score = store_comp_score_pv_id[store_comp_score_pv_id.index.isin(test_store_id["store_id"])]
    ctrl_store_score = store_comp_score_pv_id[store_comp_score_pv_id.index.isin(ctrl_store_id["store_id"])]

    pair_min_euc = _get_pair_min_dist(test=test_store_score, ctrl=ctrl_store_score, dist_nm="euclidean")
    pair_min_cos = _get_pair_min_dist(test=test_store_score, ctrl=ctrl_store_score, dist_nm="cosine")
    pair_min_var = _get_pair_min_dist_func(test=test_store_score, ctrl=ctrl_store_score, dist_func=__var_abs_dist, dist_nm="var_abs")

    euc_list.append(pair_min_euc)
    cos_list.append(pair_min_cos)
    var_list.append(pair_min_var)

    all_euc = pd.concat(euc_list)
    all_cos = pd.concat(cos_list)
    all_var = pd.concat(var_list)

    all_dist_no_region = pd.concat([all_euc, all_cos, all_var])

    # Map store region
    str_region = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new").drop_duplicates().toPandas()
    test_str_region = str_region.rename(columns={"store_id":"test_store_id", "store_region_new":"test_store_region"})
    ctrl_str_region = str_region.rename(columns={"store_id":"ctrl_store_id", "store_region_new":"ctrl_store_region"})
    all_dist = all_dist_no_region.merge(test_str_region, on="test_store_id", how="left").merge(ctrl_str_region, on="ctrl_store_id", how="left")

    print("All pairs matching - all distance method")
    all_dist.display()
    print("Summary all distance method value")
    all_dist.groupby(["dist_measure"])["value"].agg(["count", np.mean, np.std]).reset_index().display()
    all_dist.groupby(["dist_measure", "test_store_region"])["value"].agg(["count", np.mean, np.std]).reset_index().display()

    # Set outlier score threshold
    OUTLIER_SCORE_THRESHOLD = 3.0
    #----select control store using var method
    if matching_methodology == 'varience':
        flag_outlier = _flag_outlier_mad(all_var, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    elif matching_methodology == 'euclidean':
        flag_outlier = _flag_outlier_mad(all_euc, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    elif matching_methodology == 'cosine_distance':
        flag_outlier = _flag_outlier_mad(all_cos, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    else:
        print('Matching methodology not in scope list : varience, euclidean, cosine_distance')
        return None

    print("-"*80)
    no_outlier = flag_outlier[~flag_outlier["flag_outlier"]]
    print(f"Number of paired test-ctrl after remove bad match : {no_outlier.shape[0]}")

    print("Pair plot matched store")
    __plt_pair(no_outlier, store_comp_score=store_comp_score)

    print("-"*80)
    print(f"Outlier score threshold : {OUTLIER_SCORE_THRESHOLD}")
    print("Details of bad match pair(s)")
    outlier = flag_outlier[flag_outlier["flag_outlier"]]
    (outlier
     .merge(test_str_region, on="test_store_id", how="left")
     .merge(ctrl_str_region, on="ctrl_store_id", how="left")
    ).display()

    print("Pair plot bad match store")
    __plt_pair(outlier, store_comp_score=store_comp_score)

    ctr_store_list = list(set([s for s in no_outlier.ctrl_store_id]))

    # Backward compatibility : rename column to ["store_id", "ctr_store_var"]
    matching_df = (no_outlier
                   .merge(test_str_region, on="test_store_id", how="left")
                   .merge(ctrl_str_region, on="ctrl_store_id", how="left")
                   .rename(columns={"test_store_id":"store_id", "ctrl_store_id":"ctrl_store_var"})
                  )

    # If specific projoect path, save composite score, outlier score to 'output'
    if dbfs_project_path != "":
        pandas_to_csv_filestore(store_comp_score, "store_matching_composite_score.csv", prefix=os.path.join(dbfs_project_path, 'output'))
        pandas_to_csv_filestore(flag_outlier, "store_matching_flag_outlier.csv", prefix=os.path.join(dbfs_project_path, 'output'))

    return ctr_store_list, matching_df


# COMMAND ----------

ctr_store_list, store_matching_df = get_store_matching_across_region(txn=txn_all,
                                                       pre_en_wk=pre_en_wk,
                                                       wk_type="fis_week",
                                                       feat_sf=feat_sf,
                                                       brand_df=brand_df,
                                                       sclass_df=sclass_df,
                                                       test_store_sf=trg_str_df,
                                                       reserved_store_sf=u_ctl_str_df,
                                                       matching_methodology="varience",
                                                       dbfs_project_path="")

# COMMAND ----------

test_pv = store_comp_store_pv_id[store_comp_store_pv_id.index.isin(["1101", "1102", "1104", "1105"])]
ctrl_pv = store_comp_store_pv_id[store_comp_store_pv_id.index.isin(["1110", "1111"])]

# COMMAND ----------

pair_min_euc = _get_pair_min_dist(test=test_pv, ctrl=ctrl_pv, dist_nm="euclidean")
pair_min_cos = _get_pair_min_dist(test=test_pv, ctrl=ctrl_pv, dist_nm="cosine")

# COMMAND ----------

all_dist = pd.concat([pair_min_euc, pair_min_cos])

# COMMAND ----------

all_dist.display()

# COMMAND ----------

def mad(df: PandasDataFrame, 
        outlier_score_threshold: float = 3.0):
    """Flag outlier with MAD, outlier score
    """
    flag = \
    (df
     .assign(median = lambda x : x["value"].median())
     .assign(abs_deviation = lambda x : np.abs(x["value"] - x["median"]) )
     .assign(mad = lambda x : 1.4826 * np.median( x["abs_deviation"] ) ) 
     .assign(outlier_score = lambda x : x["abs_deviation"]/x["mad"])
     .assign(flag_outlier = lambda x : np.where(x["outlier_score"]>=outlier_score_threshold, True, False))
    )
    return flag

# COMMAND ----------

flag_out = mad(pair_min_euc, outlier_score_threshold=0.7)

# COMMAND ----------

no_outlier = flag_out[~flag_out["flag_outlier"]]

# COMMAND ----------

outlier = flag_out[flag_out["flag_outlier"]]

# COMMAND ----------

outlier

# COMMAND ----------

def __plt_pair(pair: PandasDataFrame,
               store_comp_score: PandasDataFrame):
    """Comparison plot each pair of test-ctrl score
    """
    from matplotlib import pyplot as plt

    for i, row in pair.iterrows():
        test_score = store_comp_score[store_comp_score["store_id"]==row["test_store_id"]].loc[:,["week_id", "comp_score"]]
        ctrl_score = store_comp_score[store_comp_score["store_id"]==row["ctrl_store_id"]].loc[:,["week_id", "comp_score"]]
        fig, ax = plt.subplots()
        test_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Test Store : {row["test_store_id"]}')
        ctrl_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Ctrl Store : {row["ctrl_store_id"]}')
        plt.show()
    return None

# COMMAND ----------

__plt_pair(outlier, store_comp_score)

# COMMAND ----------

__plt_pair(no_outlier, store_comp_score)

# COMMAND ----------

def find_mid_rank(num_max: int, fraction: float):
    """Calculate mid rank base on fraction x num_max in each group
    Return ArrayType(IntegerType()) of list in mid rank"""

    import math
    
    if (num_max <= 19):
        num_mid = 1
    else:
        num_mid = math.floor(num_max*fraction)
        if num_mid == 0:
            num_mid = 1
        
    pos_mid = math.floor(num_max/2.0)
    
    # create list of [1, -1, 2, -2, 3, -3]
    idx_alternate = [y for x in range(1, num_mid) for y in (x,-x)]
    # add 0 at start list
    idx_alternate.insert(0, 0)
    idx_alternate_num_mid = idx_alternate[:num_mid]
    
    list_mid = [pos_mid+i for i in idx_alternate_num_mid]
    list_mid.sort()
    
#     return num_mid, pos_mid, list_mid
    return list_mid

# COMMAND ----------

find_mid_rank(2, 0.1)

# COMMAND ----------

import math

math.floor(2*0.1)
range(1,0)

# COMMAND ----------



# COMMAND ----------

def find_mid_rank(num_max: int, fraction: float):
    """Calculate mid rank base on fraction x num_max in each group
    Return ArrayType(IntegerType()) of list in mid rank"""

    import math
    
    if (num_max <= 19):
        num_mid = 1
    else:
        num_mid = math.floor(num_max*fraction)
        
    pos_mid = math.floor(num_max/2.0)
    
    # create list of [1, -1, 2, -2, 3, -3]
    idx_alternate = [y for x in range(1, num_mid) for y in (x,-x)]
    # add 0 at start list
    idx_alternate.insert(0, 0)
    idx_alternate_num_mid = idx_alternate[:num_mid]
    
    list_mid = [pos_mid+i for i in idx_alternate_num_mid]
    list_mid.sort()
    
#     return num_mid, pos_mid, list_mid
    return list_mid

# COMMAND ----------

for i in range(1, 30):
    print(i,",", find_mid_rank(i, 0.1))

# COMMAND ----------

num_max = 3
pos_mid = math.floor(num_max/2.0)+1

# COMMAND ----------

pos_mid

# COMMAND ----------

list_mid = [pos_mid+i for i in idx_alternate_num_mid]
list_mid

# COMMAND ----------


