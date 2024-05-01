# Databricks notebook source
def cust_kpi_noctrl_dev(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 
    #---- Features products, brand, classs, subclass
    #features_brand = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('brand_name').drop_duplicates()
    #features_class = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id').drop_duplicates()
    #features_subclass = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    
    features_brand    = brand_df
    features_category = cate_df
    

    #test_store_sf:pyspark.sql.dataframe.DataFrame
    #store_id:integer
    #c_start:date
    #c_end:date
    #mech_count:integer
    #mech_name:string
    #media_fee_psto:double
    #aisle_subclass:string
    #aisle_scope:string
    
    trg_str_df   = test_store_sf.select('store_id',"mech_name").groupBy("store_id").agg(F.concat_ws("_", F.collect_list("mech_name")).alias("combined_mech_names")).dropDuplicates().persist()
    
    txn_test_pre = txn_all.filter(txn_all.period_promo_wk.isin(['pre']))\
                          .join  (trg_str_df , 'store_id', 'left_semi')
                          
    txn_test_cmp = txn_all.filter(txn_all.period_promo_wk.isin(['cmp']))\
                          .join(trg_str_df, 'store_id', 'left_semi')
    
    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )

    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_promo_wk',"combined_mech_names")\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))

    
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                            .groupBy('period_promo_wk',"combined_mech_names")\
                                            .pivot('carded_nonCarded')\
                                            .agg(*kpis)\
                                            .withColumn('kpi_level', F.lit('feature_brand'))
    
    
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_promo_wk',"combined_mech_names")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))
                                       
   ##------------------------------------------------------------------------------
   ## Pat comment out, will do only 1 level at category (either class or subclass)
   ##------------------------------------------------------------------------------
   
   # brand_in_class_kpi = \
   # (txn_test_combine
   #  .join(features_brand, 'brand_name')
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_brand_in_class'))
   # )
   # 
   # all_class_kpi = \
   # (txn_test_combine
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_class'))
   # )
    
    combined_kpi = sku_kpi.unionByName(brand_in_cate_kpi).unionByName(all_category_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['period_promo_wk', 'kpi_level', 'MyLo_customer']].pivot(index='period_promo_wk', columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi, kpi_df, cust_share_pd

# COMMAND ----------

def cust_kpi_noctrl_fiswk(txn
                         ,store_fmt
                         ,test_store_sf
                         ,feat_list
                         ,brand_df
                         ,cate_df
                         ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 
    #---- Features products, brand, classs, subclass
    #features_brand = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('brand_name').drop_duplicates()
    #features_class = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id').drop_duplicates()
    #features_subclass = spark.table('tdm.v_prod_dim_c').filter(F.col('upc_id').isin(feat_list)).select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    
    features_brand    = brand_df
    features_category = cate_df
    
    trg_str_df   = test_store_sf.select('store_id').dropDuplicates().persist()
    
    txn_test_pre = txn_all.filter(txn_all.period_fis_wk.isin(['pre']))\
                          .join  (trg_str_df , 'store_id', 'left_semi')
                          
    txn_test_cmp = txn_all.filter(txn_all.period_fis_wk.isin(['cmp']))\
                          .join(trg_str_df, 'store_id', 'left_semi')
    
    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )

    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_fis_wk')\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))

    
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                            .groupBy('period_fis_wk')\
                                            .pivot('carded_nonCarded')\
                                            .agg(*kpis)\
                                            .withColumn('kpi_level', F.lit('feature_brand'))
    
    
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_fis_wk')\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))
                                       
   ##------------------------------------------------------------------------------
   ## Pat comment out, will do only 1 level at category (either class or subclass)
   ##------------------------------------------------------------------------------
   
   # brand_in_class_kpi = \
   # (txn_test_combine
   #  .join(features_brand, 'brand_name')
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_brand_in_class'))
   # )
   # 
   # all_class_kpi = \
   # (txn_test_combine
   #  .join(features_class, ['section_id', 'class_id'])
   #  .groupBy('period')
   #  .pivot('carded_nonCarded')
   #  .agg(*kpis)
   #  .withColumn('kpi_level', F.lit('feature_class'))
   # )
    
    combined_kpi = sku_kpi.unionByName(brand_in_cate_kpi).unionByName(all_category_kpi)
    
    kpi_df = to_pandas(combined_kpi)
    
    df_pv = kpi_df[['period_fis_wk', 'kpi_level', 'MyLo_customer']].pivot(index='period_fis_wk', columns='kpi_level', values='MyLo_customer')
    df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi, kpi_df, cust_share_pd
