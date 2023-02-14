# Databricks notebook source
import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev"))

# COMMAND ----------

m = 

# COMMAND ----------

class DBPath(Path):
    def __init__(self,
                 in_path: str) -> None:
        super.__init__(in_path)
        self._base_path = in_path
    
    def show(self):
        print(self._base_path)

# COMMAND ----------

DBPath("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv")

# COMMAND ----------

m = Path("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv")

# COMMAND ----------

type(m)

# COMMAND ----------

m = DBPath("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv")

# COMMAND ----------

mp = Path("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv")

# COMMAND ----------

mp.parts

# COMMAND ----------

np = Path("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv")

# COMMAND ----------

np.parts

# COMMAND ----------

from instore_eval import load_config

# COMMAND ----------

load_config.change_path_type("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_01.csv", "file")

# COMMAND ----------

for k, v in v.items():
    if isinstance(v, PandasDataFrame):
        print(k)
        display(v)

# COMMAND ----------

df = pd.DataFrame({"a":[2,3]})

# COMMAND ----------

from typing import List
import pandas as pd
from pandas import DataFrame as PandasDataFrame

# COMMAND ----------

type(x[0])

# COMMAND ----------

for i in x:
    if isinstance(i, PandasDataFrame):
        print(i)

# COMMAND ----------

# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_helper import get_lag_wk_id, to_pandas

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


