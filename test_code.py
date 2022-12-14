# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_helper import get_lag_wk_id, to_pandas

# COMMAND ----------

resrv_store_class = "13_77_166_1"
wk_id_col_nm = "week_id"

txn = spark.table("tdm_seg.media_campaign_eval_txn_data_2022_0592_m01m")
feat_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/upc_list_2022_0592_M01M.csv", header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
test_store_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0592_M01M.csv", header=True, inferSchema=True)

resrv_store_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/00_std_inputs/reserved_store_HDE_20211031_master_withcode.csv", header=True, inferSchema=True).where(F.col("class_code")==resrv_store_class).select("store_id")

# COMMAND ----------

from pyspark.sql.types import StringType

def _get_comp_score(txn: SparkDataFrame,
                    wk_id_col_nm: str):
    """Calculate weekly kpi by store_id
    """
    def __get_std(df: PandasDataFrame) -> PandasDataFrame:
        """
        """
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
    import numpy as np
    var_abs = np.var(np.abs(x - y))
    return var_abs

def __get_min_pair(data: np.ndarray, 
                   test: pd.DataFrame, 
                   ctrl: pd.DataFrame, 
                   dist_nm: str):

    paired = pd.DataFrame(data=data, index=test.index, columns=ctrl.index)    
    paired.index = paired.index.set_names("test_store_id")
    paired.columns = paired.columns.set_names("ctrl_store_id")
    # Turn wide format into long format with paired of ctrl - test store id, rename column 0 - > value
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
    """From test , ctrl calculate paired distance, keep lowest
    """
    import numpy as np

    data = np.zeros( (test.shape[0], ctrl.shape[0]) )

    for i, r in test_pv.reset_index(drop=True).iterrows():
        for j, l in ctrl_pv.reset_index(drop=True).iterrows():
            data[i, j] = dist_func(r, l)

    return __get_min_pair(data, test, ctrl, dist_nm)

# COMMAND ----------

store_comp_score = _get_comp_score(txn.join(feat_sf, "upc_id", "leftsemi"), wk_id_col_nm)
store_comp_score_pv_id = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score")

# COMMAND ----------

store_comp_score

# COMMAND ----------

store_comp_score_pv_id

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


