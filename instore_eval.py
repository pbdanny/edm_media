from typing import List
from copy import deepcopy
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

spark = SparkSession.builder.appName("media_eval").getOrCreate()

def get_cust_activated(txn: SparkDataFrame,
                       cp_start_date: str,
                       cp_end_date: str, 
                       wk_type: str,
                       test_store_sf: SparkDataFrame, 
                       adj_prod_sf: SparkDataFrame, 
                       brand_sf: SparkDataFrame,
                       feat_sf: SparkDataFrame):
    """Get customer exposed & unexposed / shopped, not shop
    
    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
        "fis_week" or "promo_week"
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    
    #--- Helper fn
    def _get_period_wk_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk" 
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm
            
    def _create_test_store_sf(test_store_sf: SparkDataFrame,
                             cp_start_date: str, 
                             cp_end_date: str
                             ) -> SparkDataFrame:
        """From target store definition, fill c_start, c_end 
        based on cp_start_date, cp_end_date
        """
        filled_test_store_sf = \
            (test_store_sf
            .fillna(str(cp_start_date), subset='c_start')
            .fillna(str(cp_end_date), subset='c_end')
            )
        return filled_test_store_sf
    
    def _get_exposed_cust(txn: SparkDataFrame,
                          test_store_sf: SparkDataFrame,
                          adj_prod_sf: SparkDataFrame,
                          channel: str = "OFFLINE"
                          ) -> SparkDataFrame:
        """Get exposed customer & first exposed date
        """
        out = \
            (txn
             .where(F.col("channel")==channel)
             .where(F.col("household_id").isNotNull())
             .join(test_store_sf, "store_id","inner") # Mapping cmp_start, cmp_end, mech_count by store
             .join(adj_prod_sf, "upc_id", "inner")
             .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
             .groupBy("household_id")
             .agg(F.min("date_id").alias("first_exposed_date"))
            )
        return out
    
    def _get_shppr(txn: SparkDataFrame,
                   period_wk_col_nm: str,
                   prd_scope_df: SparkDataFrame
                   ) -> SparkDataFrame:
        """Get first brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        out = \
            (txn
             .where(F.col('household_id').isNotNull())    
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.min('date_id').alias('first_shp_date'))
             .drop_duplicates()
            )
        return out
    
    def _get_activated(exposed_cust: SparkDataFrame,
                       shppr_cust: SparkDataFrame
                       ) -> SparkDataFrame:
        """Get activated customer : First exposed date <= First (brand/sku) shopped date
        """
        out = \
            (exposed_cust.join(shppr_cust, "household_id", "inner")
             .where(F.col('first_exposed_date').isNotNull())
             .where(F.col('first_shp_date').isNotNull())
             .where(F.col('first_exposed_date') <= F.col('first_shp_date'))
             .select("household_id")
             .drop_duplicates()
             )
        return out

    #---- Main
    print("-"*80)
    print("Customer Exposure -> Activated")
    print("Exposed = shopped media aisle in media period (base on target input file) at target store , channel OFFLINE ")
    print("Activate = Exposed & Shop (Feature SKU/Feature Brand) in campaign period at any store format and any channel")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)
    
    # Brand activate
    target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)
    cmp_brand_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf)
    cmp_brand_activated = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_brand_shppr)

    nmbr_brand_activated = cmp_brand_activated.count()
    print(f'Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) : {nmbr_brand_activated:,d}')
    
    # Sku Activated
    cmp_sku_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=feat_sf)
    cmp_sku_activated = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_sku_shppr)

    nmbr_sku_activated = cmp_sku_activated.count()
    print(f'Total exposed and Features SKU shopper (Features SKU Activated) : {nmbr_sku_activated:,d}')
    
    return cmp_brand_activated, cmp_sku_activated

def get_cust_movement(txn: SparkDataFrame, 
                      wk_type: str,
                      feat_sf: SparkDataFrame,
                      sku_activated: SparkDataFrame,
                      brand_activated: SparkDataFrame,
                      class_df: SparkDataFrame,
                      sclass_df: SparkDataFrame,
                      brand_df: SparkDataFrame,
                      switching_lv: str, cp_start_date: str, 
                      cp_end_date: str, 
                      test_store_sf: SparkDataFrame, 
                      adj_prod_sf: SparkDataFrame, 
                      feat_list: List
                      ):
    """Customer movement based on tagged feature activated & brand activated
    
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper function
    def _get_period_wk_col_nm(wk_type: str) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk" 
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    #---- Main
    # Movement 
    # Existing and New SKU buyer (movement at micro level)
    print("-"*80)
    print("Customer movement")
    print("Movement consider only Feature SKU activated / Feature Brand activated customer")
    print("-"*80)    
    
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)
    
    # Features SKU movement
    prior_pre_sku_shopper = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
     .join(feat_sf, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )
    
    existing_exposed_cust_and_sku_shopper = \
    (sku_activated
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
     .checkpoint()
    )
    
    new_exposed_cust_and_sku_shopper = \
    (sku_activated
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
     .checkpoint()
    )
        
    # Customer movement for Feature SKU
    ## Macro level (New/Existing/Lapse)
    prior_pre_cc_txn = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(class_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )
    
    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sclass_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )
        
    ## Micro level
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )
    
    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )
    
    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )
        
        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sclass_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
         .drop_duplicates()
        )
        
        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )
        
        return result_movement, new_exposed_cust_and_sku_shopper

    elif switching_lv == 'class':
        
        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sclass_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )
        
        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )
        
        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )
        
        return result_movement, new_exposed_cust_and_sku_shopper

    else:
        print('Not recognized Movement and Switching level param')
        return None