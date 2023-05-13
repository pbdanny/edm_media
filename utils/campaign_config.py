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
        self.cmp_config_file = DBPath(str("/dbfs" + source_file[5:]))
        self.cmp_config_file_name = self.cmp_config_file.name
        self.cmp_config_df = pd.read_csv(self.cmp_config_file.file_api())
        self.cmp_config_df.insert(
            loc=0, column="row_num", value=self.cmp_config_df.index + 1
        )
        self.total_rows = self.cmp_config_df.shape[0]
        # self.cmp_inputs_files = self.cmp_config_file.parent / "inputs_files"
        self.cmp_inputs_files = next(
            self.cmp_config_file.parent.glob("**/input*"))
        self.cmp_output = self.cmp_config_file.parents[1]
        pass

    def __repr__(self):
        return f"CampaignConfigFile class, source file = '{self.source_config_file}'"

    def display_details(self):
        return self.cmp_config_df.display()

    def search_details(self, column: str, search_key: str):
        return self.cmp_config_df[self.cmp_config_df[column].str.contains(search_key)]


class CampaignParams:
    def __init__(self, config_file, cmp_row_no):
        self.cmp_config_file = config_file.cmp_config_file
        self.all_cmp_df = config_file.cmp_config_df
        self.all_cmp_max_row = config_file.total_rows
        self.cmp_inputs_files = config_file.cmp_inputs_files

        if cmp_row_no > self.all_cmp_max_row:
            raise ValueError(
                f"Campaign input have only '{self.all_cmp_max_row}' rows, request {cmp_row_no} is not available"
            )
        elif cmp_row_no < 0:
            raise ValueError(
                f"Campaign input have only '{self.all_cmp_max_row}' rows, request {cmp_row_no} is not available"
            )
        else:
            self.row_no = cmp_row_no
            self.params = (
                self.all_cmp_df.applymap(
                    lambda x: x.strip() if type(x) == str else x)
                .iloc[self.row_no - 1]
                .replace(np.nan, None)
                .replace("", None)
            ).to_dict()
            self.output_path = (
                config_file.cmp_output
                / self.params["cmp_month"]
                / self.params["cmp_nm"]
            )
            self.std_input_path = config_file.cmp_output.parent / "00_std_inputs"
        pass

    def __repr__(self):
        return f"CampaignParams class, config file : '{self.cmp_input_file}'\nRow number : {self.row_no}"

    def display_details(self):
        pprint.pp(self.params)
        pass


class CampaignEval(CampaignParams):

    def convert_param_to_list(self, param_name: str) -> List:
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
        self.spark = SparkSession.builder.appName("campaignEval").getOrCreate()

        self.store_fmt = self.params["store_fmt"].lower()
        self.wk_type = self.params["wk_type"]

        self.cmp_nm = self.params["cmp_nm"]
        self.cmp_start = self.params["cmp_start"]
        self.cmp_end = self.params["cmp_end"]
        self.media_fee = self.params["media_fee"]

        self.sku_file = self.cmp_inputs_files / \
            f"upc_list_{self.params['cmp_id']}.csv"
        self.target_store_file = (
            self.cmp_inputs_files / f"target_store_{self.params['cmp_id']}.csv"
        )

        self.resrv_store_file = (
            self.std_input_path / f"{self.params['resrv_store_file']}"
        )
        self.use_reserved_store = bool(self.params["use_reserved_store"])

        self.custom_ctrl_store_file = (
            self.cmp_inputs_files /
            f"control_store_{self.params['cmp_id']}.csv"
        )

        self.adjacency_file = self.std_input_path / \
            f"{self.params['adjacency_file']}"
        self.svv_table = self.params["svv_table"]
        self.purchase_cyc_table = self.params["purchase_cyc_table"]

        self.load_period()
        self.load_target_store()
        self.load_control_store()
        self.load_store_dim_adjusted()
        self.load_prod()
        self.load_product_dim_adjusted()
        self.load_aisle()
        # self.load_txn()

        pass

    def __repr__(self):
        return f"CampaignEval class \nConfig file : '{self.cmp_config_file}'\nRow number : {self.row_no}"

    def load_period(self, eval_mode: str = "homeshelf"):
        """Load campaign period : cmp, pre, ppp & gap
        For evaluation type "promotion_zone" the pre period number of week = same week as cmp period
        For evaluation type "homeshelf" the pre period number of week 13 week before cmp period

        Parameters
        ----------
        eval_mode: str, default "homeshelf"
            Evaluation type : "promotion_zone", "homeshelf"
        """
        self.cmp_st_wk = period_cal.wk_of_year_ls(self.cmp_start)
        self.params["cmp_st_wk"] = self.cmp_st_wk
        self.cmp_en_wk = period_cal.wk_of_year_ls(self.cmp_end)
        self.params["cmp_en_wk"] = self.cmp_en_wk
        self.cmp_st_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_start)
        self.params["cmp_st_promo_wk"] = self.cmp_st_promo_wk
        self.cmp_en_promo_wk = period_cal.wk_of_year_promo_ls(self.cmp_end)
        self.params["cmp_en_promo_wk"] = self.cmp_en_promo_wk

        dt_diff = (
            datetime.strptime(self.cmp_end, "%Y-%m-%d")
            - datetime.strptime(self.cmp_start, "%Y-%m-%d")
        ) + timedelta(days=1)
        # convert from time delta to int (number of days diff)
        diff_days = dt_diff.days
        wk_cmp = int(np.round(diff_days / 7, 0))

        self.gap_start_date = self.params["gap_start_date"]
        self.gap_end_date = self.params["gap_end_date"]

        if (self.gap_start_date is None) & (self.gap_end_date is None):
            print(f"No Gap Week for campaign : {self.cmp_nm}")
            self.gap_flag = False
            chk_pre_dt = self.cmp_start

        elif (self.gap_start_date is not None) & (self.gap_end_date is not None):
            print(
                f"Campaign {self.cmp_nm} has gap period between : {self.gap_start_date} and {self.gap_end_date}"
            )

            # fis_week
            self.gap_st_wk = period_cal.wk_of_year_ls(self.gap_start_date)
            self.gap_en_wk = period_cal.wk_of_year_ls(self.gap_end_date)

            # promo
            self.gap_st_promo_wk = period_cal.wk_of_year_promo_ls(
                self.gap_start_date)
            self.gap_en_promo_wk = period_cal.wk_of_year_promo_ls(
                self.gap_end_date)

            self.gap_flag = True

            chk_pre_dt = self.gap_start_date

        else:
            print("Incorrect gap period. Please recheck - Code will skip\n")
            print(
                f'Received Gap = {self.gap_start_date} + " and " + {self.gap_end_date}'
            )
            raise Exception("Incorrect Gap period value please recheck")

        self.pre_en_date = (
            datetime.strptime(chk_pre_dt, "%Y-%m-%d") + timedelta(days=-1)
        ).strftime("%Y-%m-%d")
        self.pre_en_wk = period_cal.wk_of_year_ls(self.pre_en_date)
        self.pre_en_promo_wk = period_cal.wk_of_year_promo_ls(self.pre_en_date)
        self.pre_en_promo_mv_wk = self.pre_en_promo_wk

        self.pre_st_wk = period_cal.week_cal(self.pre_en_wk, -12)
        self.pre_st_mv_wk = self.pre_st_wk
        self.pre_st_promo_wk = period_cal.promo_week_cal(
            self.pre_en_promo_wk, -12)
        self.pre_st_promo_mv_wk = self.pre_st_promo_wk

        self.ppp_en_wk = period_cal.week_cal(self.pre_st_wk, -1)
        self.ppp_en_mv_wk = self.ppp_en_wk
        self.ppp_en_promo_wk = period_cal.promo_week_cal(
            self.pre_st_promo_wk, -1)
        self.ppp_en_promo_mv_wk = self.ppp_en_promo_wk

        self.ppp_st_wk = period_cal.week_cal(self.ppp_en_wk, -12)
        self.ppp_st_mv_wk = self.ppp_st_wk
        self.ppp_st_promo_wk = period_cal.promo_week_cal(
            self.ppp_en_promo_wk, -12)
        self.ppp_st_promo_mv_wk = self.ppp_st_promo_wk

        if eval_mode == "promozone":
            self.pre_st_wk = period_cal.week_cal(
                self.pre_en_wk, (wk_cmp - 1) * -1)
            self.pre_st_promo_wk = period_cal.promo_week_cal(
                self.pre_en_promo_wk, (wk_cmp - 1) * -1
            )
            self.ppp_en_wk = period_cal.week_cal(self.pre_st_wk, -1)
            self.ppp_en_promo_wk = period_cal.promo_week_cal(
                self.pre_st_promo_wk, -1)
            self.ppp_st_wk = period_cal.week_cal(
                self.ppp_en_wk, (wk_cmp - 1) * -1)
            self.ppp_st_promo_wk = period_cal.promo_week_cal(
                self.ppp_en_promo_wk, (wk_cmp - 1) * -1)

        if self.params["wk_type"] == "fis_wk":
            self.wk_tp = "fiswk"
            self.week_type = "fis_week"
        elif self.params["wk_type"] == "promo_wk":
            self.wk_tp = "promowk"
            self.week_type = "promo_week"
        pass

    def load_target_store(self):
        """Load target store"""
        self.target_store = \
            (self.spark.read.csv(self.target_store_file.spark_api(), header=True, inferSchema=True)
             .fillna(str(self.cmp_start), subset='c_start')
             .fillna(str(self.cmp_end), subset='c_end')
             )
        pass

    def load_control_store(self, control_store_mode: str = ""):
        """Load control store

        Parameters
        ----------
        control_store_mode: str, default=""
            "" : (leave blank) = Auto upto input in config file
            "reserved_store" : use reserved store
            "custom_control_file" : load from custom control store
            "rest" : rest
        """

        def _resrv():
            self.params["control_store_source"] = "Reserved store class"
            self.control_store = (
                self.spark.read.csv(
                    (self.resrv_store_file).spark_api(), header=True, inferSchema=True
                )
                .where(F.col("class_code") == self.params["resrv_store_class"])
                .select("store_id")
            )
            pass

        def _custom():
            self.params["control_store_source"] = "Custom control store file"
            self.control_store = self.spark.read.csv(
                (self.custom_ctrl_store_file).spark_api(), header=True, inferSchema=True
            )
            pass

        def _rest():
            self.params["control_store_source"] = "rest"
            store_dim_c = self.spark.table("tdm.v_store_dim_c")

            if self.store_fmt in ["hde", "hyper"]:
                target_format = store_dim_c.where(
                    F.col("format_id").isin([1, 2, 3]))
            elif self.store_fmt in ["talad", "super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([4]))
            elif self.store_fmt in ["gofresh", "mini_super"]:
                target_format = store_dim_c.where(F.col("format_id").isin([5]))
            else:
                target_format = store_dim_c.where(
                    F.col("format_id").isin([1, 2, 3, 4, 5])
                )

            self.control_store = (
                target_format.join(self.target_store, "store_id", "leftanti")
                .select("store_id")
                .drop_duplicates()
            )
            pass

        self.load_target_store()

        if control_store_mode == "":
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

    def load_store_dim_adjusted(self):
        """Create internal store dim with adjusted store region & combine "West" & "Central" -> West+Central"""
        store_dim = (
            self.spark.table("tdm.v_store_dim")
            .select(
                "store_id",
                "format_id",
                "store_name",
                "date_opened",
                "date_closed",
                "status",
                F.lower(F.col("region")).alias("store_region_orig"),
            )
            .drop_duplicates()
        )
        self.store_dim = store_dim.withColumn(
            "store_region",
            F.when(
                (F.col("format_id").isin(5))
                & (F.col("store_region_orig").isin(["West", "Central"])),
                F.lit("West+Central"),
            )
            .when(F.col("store_region_orig").isNull(), F.lit("Unidentified"))
            .otherwise(F.col("store_region_orig")),
        ).withColumn(
            "store_format_name",
            F.when(F.col("format_id").isin([1, 2, 3]), "hde")
            .when(F.col("format_id").isin([4]), "talad")
            .when(F.col("format_id").isin([5]), "gofresh")
            .otherwise("other_fmt"),
        )

        pass

    def load_prod(self):
        """Load feature product, feature brand name, feature subclass, feature subclass"""
        self.feat_sku = self.spark.read.csv(
            (self.sku_file).spark_api(), header=True, inferSchema=True
        ).withColumnRenamed("feature", "upc_id")
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c").fillna("Unidentified", subset="brand_name")
        self.feat_subclass_code = (
            prd_dim_c.join(self.feat_sku, "upc_id", "inner")
            .select("subclass_code")
            .drop_duplicates()
        )
        self.feat_class_code = (
            prd_dim_c.join(self.feat_sku, "upc_id", "inner")
            .select("class_code")
            .drop_duplicates()
        )
        self.feat_subclass_sku = (
            prd_dim_c.join(self.feat_subclass_code, "subclass_code")
            .select("upc_id")
            .drop_duplicates()
        )
        self.feat_class_sku = (
            prd_dim_c.join(self.feat_class_code, "class_code")
            .select("upc_id")
            .drop_duplicates()
        )

        if self.params["cate_lvl"].lower() in ["class"]:
            self.feat_cate_sku = self.feat_class_sku
            self.feat_cate_cd_brand_nm = \
                (prd_dim_c
                 .join(self.feat_class_code, "class_code")
                 .join(self.feat_sku, "upc_id")
                 .select("class_code", "class_name", "brand_name")
                 .drop_duplicates()
                 )
            self.feat_brand_nm = self.feat_cate_cd_brand_nm.select(
                "brand_name").drop_duplicates()
            self.feat_brand_sku = prd_dim_c.join(self.feat_cate_cd_brand_nm, [
                                                 "class_code", "brand_name"]).select("upc_id").drop_duplicates()
            
        elif self.params["cate_lvl"].lower() in ["subclass"]:
            self.feat_cate_sku = self.feat_subclass_sku
            self.feat_cate_cd_brand_nm = \
                (prd_dim_c
                 .join(self.feat_subclass_code, "subclass_code")
                 .join(self.feat_sku, "upc_id")
                 .select("subclass_code", "subclass_name", "brand_name")
                 .drop_duplicates()
                 )
            self.feat_brand_nm = self.feat_cate_cd_brand_nm.select(
                "brand_name").drop_duplicates()
            self.feat_brand_sku = prd_dim_c.join(self.feat_cate_cd_brand_nm, [
                                                 "subclass_code", "brand_name"]).select("upc_id").drop_duplicates()            
        else:
            self.feat_cate_sku = None
            self.feat_brand_nm = None
            self.feat_brand_sku = None
        pass

    def load_product_dim_adjusted(self):
        """Create product_dim with adjustment
            1) Mulitiple feature brand name -> Single brand name 
        """
        prd_dim_c = self.spark.table("tdm.v_prod_dim_c").fillna("Unidentified", subset="brand_name")
        brand_list = self.feat_brand_nm.toPandas()["brand_name"].tolist()
        brand_list.sort()
        main_brand = brand_list[0]

        if self.params["cate_lvl"].lower() in ["class"]:
            
            feature_class_cd = list(set(self.feat_brand_nm.toPandas()["class_code"].tolist()))
            self.product_dim = \
                (prd_dim_c
                 .withColumn("brand_name", 
                             F.when(
                                 (F.col("brand_name").isin(brand_list)) 
                                 & (F.col("class_code").isin(feature_class_cd))
                                 , F.lit(main_brand)
                                 ).otherwise(F.col("brand_name"))
                 )
                )
        elif self.params["cate_lvl"].lower() in ["subclass"]:
            feature_subclass_cd = list(set(self.feat_brand_nm.toPandas()["subclass_code"].tolist()))
            self.product_dim = \
                (prd_dim_c
                 .withColumn("brand_name", 
                             F.when(
                                 (F.col("brand_name").isin(brand_list)) 
                                 & (F.col("subclass_code").isin(feature_subclass_cd))
                                 , F.lit(main_brand)
                                 ).otherwise(F.col("brand_name"))
                 )
                )        
        pass
        
    def load_aisle(self, aisle_mode: str = ""):
        """Load aisle for exposure calculation

        Parameters
        ----------
        aisle_mode: str
            "" : (leave blank) = Auto upto input in config file
            "homeshelf" : use feature sku & aisle definition
            "cross_cate" : use defined cross catgory & aisle definition
            "total_store" : total store product
            "target_store_config" : Aisle defined at target store file
        """

        def _homeshelf():
            self.params["aisle_mode"] = "homeshelf"
            aisle_master = self.spark.read.csv(
                self.adjacency_file.spark_api(), header=True, inferSchema=True
            )
            feat_subclass = (
                self.product_dim.join(self.feat_sku, "upc_id", "inner")
                .select("subclass_code")
                .drop_duplicates()
            )
            aisle_group = (
                aisle_master.join(feat_subclass, "subclass_code", "inner")
                .select("group")
                .drop_duplicates()
            )
            aisle_subclass = (
                aisle_master.join(aisle_group, "group", "inner")
                .select("subclass_code")
                .drop_duplicates()
            )
            self.aisle_sku = (
                self.product_dim.join(aisle_subclass, "subclass_code", "inner")
                .select("upc_id")
                .drop_duplicates()
            )
            date_dim = self.spark.table("tdm.date_dim").select(
                "date_id", "week_id").drop_duplicates()
            avg_media_fee = self.media_fee / self.target_store.count()
            self.aisle_target_store_conf = \
                (self.target_store
                 .join(date_dim)
                 .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                 .join(self.aisle_sku)
                 .withColumn("media_fee", F.lit(avg_media_fee))
                 )
            pass

        def _x_cat():
            self.params["aisle_mode"] = "cross_cate"
            aisle_master = self.spark.read.csv(
                self.adjacency_file.spark_api(), header=True, inferSchema=True
            )
            x_subclass = self.spark.createDataFrame(
                pd.DataFrame(data=self.cross_cate_cd_list,
                             columns=["subclass_code"])
            ).drop_duplicates()
            aisle_group = (
                aisle_master.join(x_subclass, "subclass_code", "inner")
                .select("group")
                .drop_duplicates()
            )
            aisle_subclass = (
                aisle_master.join(aisle_group, "group", "inner")
                .select("subclass_code")
                .drop_duplicates()
            )
            self.aisle_sku = (
                self.product_dim.join(aisle_subclass, "subclass_code", "inner")
                .select("upc_id")
                .drop_duplicates()
            )
            date_dim = self.spark.table("tdm.date_dim").select(
                "date_id", "week_id").drop_duplicates()
            avg_media_fee = self.media_fee / self.target_store.count()

            self.aisle_target_store_conf = \
                (self.target_store
                 .join(date_dim)
                 .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                 .join(self.aisle_sku)
                 .withColumn("media_fee", F.lit(avg_media_fee))
                 )
            pass

        def _store():
            self.params["aisle_mode"] = "total_store"
            self.aisle_sku = self.product_dim.select("upc_id").drop_duplicates()
            date_dim = self.spark.table("tdm.date_dim").select(
                "date_id", "week_id").drop_duplicates()
            avg_media_fee = self.media_fee / self.target_store.count()

            self.aisle_target_store_conf = \
                (self.target_store
                 .join(date_dim)
                 .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                 .join(self.aisle_sku)
                 .withColumn("media_fee", F.lit(avg_media_fee))
                 )
            pass

        def _target_store_config():
            """Aisle defined by target store config file
            """
            self.params["aisle_mode"] = "target_store_config"
            self.aisle_sku = None

            adj_tbl = self.spark.read.csv(self.adjacency_file.spark_api(
            ), header=True, inferSchema=True).select("subclass_code", "group").drop_duplicates()
            prd_dim = self.product_dim.select(
                "upc_id", "subclass_code").drop_duplicates()
            date_dim = self.spark.table("tdm.date_dim").select(
                "date_id", "week_id").drop_duplicates()

            self.aisle_target_store_conf = \
                (self.target_store
                 .join(adj_tbl, self.target_store.aisle_subclass_cd == adj_tbl.subclass_code)
                 .drop("subclass_code")
                 .join(adj_tbl, "group")
                 .join(prd_dim, "subclass_code")
                 .join(date_dim)
                 .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                 )

            pass

        self.load_prod()
        self.cross_cate_cd_list = self.convert_param_to_list("cross_cate_cd")

        if aisle_mode == "":
            if not self.cross_cate_cd_list:
                _homeshelf()
            else:
                _x_cat()
        else:
            if aisle_mode == "homeshelf":
                _homeshelf()
            elif aisle_mode == "cross_cat":
                _x_cat()
            elif aisle_mode == "total_store":
                _store()
            else:
                _target_store_config()
        pass

    def _get_prod_df(self):
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

        sku_list = self.feat_sku.toPandas()["upc_id"].to_numpy().tolist()
        cate_lvl = self.params["cate_lvl"]
        std_ai_df = self.spark.read.csv(
            self.adjacency_file.spark_api(), header=True, inferSchema=True
        )
        x_cate_flag = 0.0 if self.params["cross_cate_flag"] is None else 1.0
        x_cate_cd = 0.0 if self.params["cross_cate_cd"] is None else 1.0

        prod_all = self.spark.table("tdm.v_prod_dim_c")
        mfr = self.spark.table("tdm.v_mfr_dim_c")

        prod_mfr = (
            prod_all.join(mfr, "mfr_id", "left")
            .select(
                prod_all.upc_id,
                prod_all.product_en_desc.alias("prod_en_desc"),
                prod_all.brand_name.alias("brand_nm"),
                prod_all.mfr_id,
                mfr.mfr_name,
                prod_all.division_id.alias("div_id"),
                prod_all.division_name.alias("div_nm"),
                prod_all.department_id.alias("dept_id"),
                prod_all.department_code.alias("dept_cd"),
                prod_all.department_name.alias("dept_nm"),
                prod_all.section_id.alias("sec_id"),
                prod_all.section_code.alias("sec_cd"),
                prod_all.section_name.alias("sec_nm"),
                prod_all.class_id.alias("class_id"),
                prod_all.class_code.alias("class_cd"),
                prod_all.class_name.alias("class_nm"),
                prod_all.subclass_id.alias("sclass_id"),
                prod_all.subclass_code.alias("sclass_cd"),
                prod_all.subclass_name.alias("sclass_nm"),
            )
            .persist()
        )

        feat_df = prod_mfr.where(prod_mfr.upc_id.isin(sku_list))

        del mfr
        del prod_all

        # get brand name list
        brand_pd = feat_df.select(feat_df.brand_nm).dropDuplicates().toPandas()

        brand_list = brand_pd["brand_nm"].to_list()

        print("-" * 80 + "\n List of Brand Name show below : \n " + "-" * 80)
        print(brand_list)

        del brand_pd

        # get subclass list
        sclass_cd_list = (
            feat_df.select(feat_df.sclass_cd)
            .dropDuplicates()
            .toPandas()["sclass_cd"]
            .to_list()
        )
        sclass_nm_list = (
            feat_df.select(feat_df.sclass_nm)
            .dropDuplicates()
            .toPandas()["sclass_nm"]
            .to_list()
        )

        print(
            "-" * 80
            + "\n List of Subclass Id and Subclass Name show below : \n "
            + "-" * 80
        )
        print(sclass_cd_list)
        print(sclass_nm_list)

        # get class list
        class_cd_list = (
            feat_df.select(feat_df.class_cd)
            .dropDuplicates()
            .toPandas()["class_cd"]
            .to_list()
        )
        class_nm_list = (
            feat_df.select(feat_df.class_nm)
            .dropDuplicates()
            .toPandas()["class_nm"]
            .to_list()
        )

        print("-" * 80 + "\n List of Class Name show below : \n " + "-" * 80)
        print(class_cd_list)
        print(class_nm_list)

        # get section list
        sec_cd_list = (
            feat_df.select(feat_df.sec_cd)
            .dropDuplicates()
            .toPandas()["sec_cd"]
            .to_list()
        )
        sec_nm_list = (
            feat_df.select(feat_df.sec_nm)
            .dropDuplicates()
            .toPandas()["sec_nm"]
            .to_list()
        )

        print("-" * 80 + "\n List of Section Name show below : \n " + "-" * 80)
        print(sec_nm_list)

        # get mfr name
        mfr_nm_list = (
            feat_df.select(feat_df.mfr_name)
            .dropDuplicates()
            .toPandas()["mfr_name"]
            .to_list()
        )

        print("-" * 80 + "\n List of Manufactor Name show below : \n " + "-" * 80)
        print(mfr_nm_list)

        # get use aisle dataframe
        print("Cross cate flag = " + str(x_cate_flag))

        if str(x_cate_flag) == "":
            x_cate_flag = 0
        # end if

        # check cross category
        if (float(x_cate_flag) == 1) | (str(x_cate_flag) == "true"):
            x_cate_list = x_cate_cd.split(",")
            get_ai_sclass = [cd.strip() for cd in x_cate_list]
            print(
                "-" * 80
                + "\n Using Cross Category code to define Aisle, input list of subclass code show below \n "
                + "-" * 80
            )
        else:
            get_ai_sclass = sclass_cd_list  # use feature subclass list
            print(
                "-" * 80
                + "\n Using Subclass of feature product to define Aisle, input list of subclass code show below \n "
                + "-" * 80
            )
        # end if

        print("get_ai_sclass = " + str(get_ai_sclass))

        use_ai_grp_list = list(
            std_ai_df.where(std_ai_df.subclass_code.isin(get_ai_sclass))
            .select(std_ai_df.group)
            .dropDuplicates()
            .toPandas()["group"]
        )

        print("-" * 80 + "\n List of Aisle group to be use show below : \n " + "-" * 80)
        print(use_ai_grp_list)

        use_ai_sclass = list(
            std_ai_df.where(std_ai_df.group.isin(use_ai_grp_list))
            .select(std_ai_df.subclass_code)
            .dropDuplicates()
            .toPandas()["subclass_code"]
        )

        use_ai_df = prod_mfr.where(
            prod_mfr.sclass_cd.isin(use_ai_sclass)
        )  # all product in aisles group

        use_ai_sec_list = list(
            use_ai_df.select(use_ai_df.sec_nm).dropDuplicates().toPandas()[
                "sec_nm"]
        )

        # get class & Subclass DataFrame
        class_df = prod_mfr.where(prod_mfr.class_cd.isin(class_cd_list))
        sclass_df = prod_mfr.where(prod_mfr.sclass_cd.isin(sclass_cd_list))

        # check category_level for brand definition
        if cate_lvl == "subclass":
            brand_df = prod_mfr.where(
                prod_mfr.brand_nm.isin(brand_list)
                & prod_mfr.sclass_cd.isin(sclass_cd_list)
            )
            cate_df = prod_mfr.where(prod_mfr.sclass_cd.isin(sclass_cd_list))
            cate_cd_list = sclass_cd_list
        elif cate_lvl == "class":
            brand_df = prod_mfr.where(
                prod_mfr.brand_nm.isin(brand_list)
                & prod_mfr.class_cd.isin(class_cd_list)
            )
            cate_df = prod_mfr.where(prod_mfr.class_cd.isin(class_cd_list))
            cate_cd_list = class_cd_list
        else:
            raise Exception("Incorrect category Level")
        # end if

        return (
            feat_df,
            brand_df,
            class_df,
            sclass_df,
            cate_df,
            use_ai_df,
            brand_list,
            sec_cd_list,
            sec_nm_list,
            class_cd_list,
            class_nm_list,
            sclass_cd_list,
            sclass_nm_list,
            mfr_nm_list,
            cate_cd_list,
            use_ai_grp_list,
            use_ai_sec_list,
        )
