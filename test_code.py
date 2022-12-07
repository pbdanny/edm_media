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

def _get_std_score(txn: SparkDataFrame,
                   wk_id_col_nm: str):
        """Calculate weekly kpi by store_id
        """
        from pyspark.sql.types import StringType
        
        def __get_std(df):
            
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            
            scalar = MinMaxScaler() # StandardScaler()
            
            scaled = scalar.fit_transform(df)
            scaled_df = pd.DataFrame(data=scaled, index=df.index, columns=df.columns)
            
            return scaled_df
        
        txn = txn.withColumn("store_id", F.col("store_id").cast(StringType()))
        
        sales = txn.groupBy("store_id").pivot(period_wk_col).agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
        custs = txn.groupBy("store_id").pivot(period_wk_col).agg(F.count_distinct('household_id').alias('custs')).fillna(0)
        
        sales_df = to_pandas(sales).astype({'store_id':str}).set_index("store_id")
        custs_df = to_pandas(custs).astype({'store_id':str}).set_index("store_id")
        
        sales_scaled_df = __get_std(sales_df)
        custs_scaled_df = __get_std(custs_df)
        
        sales_unpv_df = sales_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_sales", var_name="week_id")
        custs_unpv_df = custs_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_custs", var_name="week_id")        
        comb_df = pd.merge(sales_unpv_df, custs_unpv_df, how="outer", on=["store_id", "week_id"])
        comb_df["comp_score"] = (comb_df["std_sales"] + comb_df["std_custs"])/2
        
        return comb_df

# COMMAND ----------

cr = _get_std_score(txn.join(feat_sf, "upc_id", "leftsemi"), wk_id_col_nm)

# COMMAND ----------

import seaborn as sns
sns.kdeplot(data=cr, x="comp_score", hue="week_id")
# cr[["comp_score"]].plot(kind="kde")

# COMMAND ----------

cr

# COMMAND ----------

cr.pivot(columns="week_id", values="comp_score").reset_index()

# COMMAND ----------

cr_pv = cr.pivot(index="store_id", columns="week_id", values="comp_score").reset_index()

# COMMAND ----------

cr_pv

# COMMAND ----------

import numpy as np
from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import paired_euclidean_distances
X = pd.DataFrame(np.array([[2, 3]]), columns=["x", "y"])
Y = pd.DataFrame(np.array([[1, 0]]), columns=["x", "y"])

# COMMAND ----------

paired_euclidean_distances(X, Y)

# COMMAND ----------

X = pd.DataFrame({"store_id":[1, 2, 3],
                 "wk1":[1.2, 3.3, 4.3],
                 "wk2":[2.2, 1.3, 2.3]})

# COMMAND ----------

pd.merge(cr_pv, cr_pv, how="cross")

# COMMAND ----------


