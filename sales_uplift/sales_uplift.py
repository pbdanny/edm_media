import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.dbutils import DBUtils

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval
from matching import store_matching
from utils import helper

#---- Original Code
def sales_uplift_reg_mech(txn, 
                          sales_uplift_lv, 
                          brand_df,
                          feat_list,
                          matching_df, 
                          matching_methodology: str = 'varience'):
    """Support multi class, subclass
    change input paramenter to use brand_df and feat_list
    """
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    ## add pre period #weeks  -- Pat comment out
    
    #pre_days = pre_end_date - pre_start_date  ## result as timedelta
    #pre_wk   = (pre_days.days + 1)/7  ## get number of weeks
    #
    #print('----- Number of ppp+pre period = ' + str(pre_wk) + ' weeks. ----- \n')    
    
    
    ##---- Filter Hierarchy with features products
    #print('-'*20)
    #print('Store matching v.2')
    #features_sku_df = pd.DataFrame({'upc_id':sel_sku})
    #features_sku_sf = spark.createDataFrame(features_sku_df)
    #features_sec_class_subcl = spark.table(TBL_PROD).join(features_sku_sf, 'upc_id').select('section_id', 'class_id', 'subclass_id').drop_duplicates()
    #print('Multi-Section / Class / Subclass')
    #(spark
    # .table(TBL_PROD)
    # .join(features_sec_class_subcl, ['section_id', 'class_id', 'subclass_id'])
    # .select('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    # .drop_duplicates()
    # .orderBy('section_id', 'section_name', 'class_id', 'class_name', 'subclass_id', 'subclass_name')
    #).show(truncate=False)
    #print('-'*20)
    
    #----- Load matching store 
    mapping_dict_method_col_name = dict({'varience':'ctr_store_var', 'euclidean':'ctr_store_dist', 'cosine_sim':'ctr_store_cos'})
    
        
    try:
        #print('In Try --- naja ')
        
        #col_for_store_id = mapping_dict_method_col_name[matching_methodology]
        
        #matching_df_2 = matching_df[['store_id','store_region', 'store_mech_set',  col_for_store_id]].copy()
        #matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_var':'store_id_ctr'},inplace=True)
        
        ## change to use new column name from store matching FEb 2023 -- Pat
        matching_df_2 = matching_df[['store_id','test_store_region', 'store_mech_set_x', 'ctr_store_cos']].copy()  ## Pat Add store_region - May 2022
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_cos':'store_id_ctr', 'test_store_region':'store_region', 'store_mech_set_x':'store_mech_set'},inplace=True)
        
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

    ## end Try 

     # ---------------------------------------------------------------
     ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
     ## need to get txn of target/control store separately to distinct #customers    
     # ---------------------------------------------------------------
     #---- Uplift
        
     # brand buyer, during campaign
    # Change filter to column offline_online_other_channel - Dec 2022 - Ta
    if sales_uplift_lv.lower() == 'brand':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        brand_txn = txn.join(brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                       .join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                       .where((txn.offline_online_other_channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp'))
                             )

        ## get transcation at brand level for pre period
        pre_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'pre')
        
        ## get transcation at brand level for dur period
        dur_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'cmp')
        
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        feat_txn = txn.join(broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                      .where( (txn.offline_online_other_channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp')) &
                              (txn.upc_id.isin(feat_list))
                            )
        
        ## pre
        pre_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'pre')
        #print(' Check Display(10) pre txn below \n')
        #pre_txn_selected_prod_test_ctr_store.display(10)
        ## during
        dur_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'cmp')
        #print(' Check Display(10) during txn below \n')
        #dur_txn_selected_prod_test_ctr_store.display(10)
    ## end if

    #---- sales by store by period    EPOS
    sales_pre    = pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    ## region is in matching table already 
    sales_dur    = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    
    sales_pre_df = to_pandas(sales_pre)
    sales_dur_df = to_pandas(sales_dur)
    
    #print('sales_pre_df')
    
    #sales_pre_df.display()
    
    #print('sales_dur_df')
    #sales_dur_df.display()
    
    #---- sales by store by period clubcard
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                     .groupBy(F.col('store_id'))\
                                                     .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                          ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                          ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                         )
#    cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = to_pandas(cc_kpi_dur)
    
    #print('cc_kpi_dur_df')
    #cc_kpi_dur_df.display()
    
    ## create separate test/control txn table for #customers -- Pat May 2022
    
    #pre_txn_test_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    #---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ## will be count in both stores (as need to check market share & behaviour)
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = to_pandas(cust_dur_ctrl)
    
    ## Danny - convert all store_id to float type
    cust_dur_ctrl_pd["store_id"]   = cust_dur_ctrl_pd["store_id"].astype("float")
    ctrl_store_use_cnt["store_id"] = ctrl_store_use_cnt["store_id"].astype("float")
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
    
    ## Pat add force datatype - Feb 2023
    
                                                                                    
    #create weekly sales for each item
    wk_sales_pre = to_pandas(pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    #print('wk_sales_pre')
    #wk_sales_pre.display()
    
    #print('wk_sales_dur')
    #wk_sales_dur.display()
    
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    #### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
    
    ## Replace matching_df Pat add region in matching_df -- May 2022
    
    ## Pat add force datatype -- Feb 2023
    
    matching_df_2["store_id_test"] = matching_df_2["store_id_test"].astype("float")
    matching_df_2["store_id_ctr"]  = matching_df_2["store_id_ctr"].astype("float")
    
    matching_df = (matching_df_2.merge(sales_pre_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={'sales':'test_pre_sum'})
                                .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={'sales':'ctr_pre_sum'})
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                #-- get control factor for all sales, unit,visits
                                .assign(ctr_factor      = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                
                                #-- during multiply control factor
                                #-- sales
                                .merge(sales_dur_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={'sales':'test_dur_sum'})
                                .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={'sales':'ctr_dur_sum'})
                                .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
                                
                                #-- club card KPI
                                .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                                                     ,'cc_bask':'cc_test_dur_bask'
                                                                                                                     ,'cc_qty':'cc_test_dur_qty'
                                                                                                                   })
                                .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                                       ,'cc_bask':'cc_ctr_dur_bask'
                                                                                                       ,'cc_qty':'cc_ctr_dur_qty'
                                                                                                        })
                 ).loc[:, [ 'store_id_test'
                           ,'store_id_ctr'
                           , 'store_region'
                           , 'store_mech_set'
                           , 'test_pre_sum'
                           , 'ctr_pre_sum'
                           , 'ctr_factor'
                           , 'test_dur_sum'
                           , 'ctr_dur_sum'
                           , 'ctr_dur_sum_adjust' 
                           , 'cc_test_dur_sum'
                           , 'cc_ctr_dur_sum'
                           , 'cc_test_dur_bask'
                           , 'cc_ctr_dur_bask'
                           , 'cc_test_dur_qty'
                           , 'cc_ctr_dur_qty' ]]
    ##-----------------------------------------------------------
    ## -- Sales uplift at overall level  
    ##-----------------------------------------------------------
    
    print('matching_df')
    
    matching_df_all = matching_df.fillna(0)
    
    matching_df     = matching_df_all.loc[(matching_df_all['ctr_factor'] > 0 ) & (matching_df_all['ctr_pre_sum'] > 0 )]
    
    matching_df.display()
    
    # -- sales uplift overall
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()

    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    ##-----------------------------------------------------------
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Region -- 24 May 2022
    ##-----------------------------------------------------------
    # -- sales uplift by region
    #df4 = df.groupby('Courses', sort=False).agg({'Fee': ['sum'], 'Courses':['count']}).reset_index()
    #df4.columns = ['course', 'c_fee', 'c_count']
    
    ## add sort = false to optimize performance only
    
    sum_sales_test_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'test_dur_sum':['sum'], 'store_region':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_region         = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_region.columns = ['store_region', 'trg_sales_reg', 'trg_store_cnt', 'ctr_sales_reg', 'ctr_sales_adj_reg']
    
    ## get uplift region
    uplift_region['s_uplift_reg'] = uplift_region['trg_sales_reg'] - uplift_region['ctr_sales_adj_reg']
    uplift_region['pct_uplift']   = uplift_region['s_uplift_reg'] / uplift_region['ctr_sales_adj_reg']

    print('\n' + '-'*80 + '\n Uplift by Region at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_region.display()
    ## ------------------------------------------------------------
    
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Mechanic -- 7 Sep 2022 (within campaign mech compare)
    ##-----------------------------------------------------------
    
    sum_sales_test_pd         = matching_df.groupby('store_mech_set', as_index = True, sort=True).agg({'test_dur_sum':['sum'], 'store_mech_set':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_mech_set', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_mech_set', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_by_mech            = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_by_mech.columns    = ['store_mech_set', 'trg_sales_mech', 'trg_store_cnt', 'ctr_sales_mech', 'ctr_sales_adj_mech']
    
    ## get uplift by mech
    uplift_by_mech['s_uplift_mech']   = uplift_by_mech['trg_sales_mech'] - uplift_by_mech['ctr_sales_adj_mech']
    uplift_by_mech['pct_uplift_mech'] = uplift_by_mech['s_uplift_mech'] / uplift_by_mech['ctr_sales_adj_mech']

    print('\n' + '-'*80 + '\n Uplift by mechanic set at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_by_mech.display()
    
    ##-----------------------------------------------------------
    
    ## Get KPI   -- Pat 8 May 2022
    
    #customers calculated above already 
#     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
#     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
#     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test   = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl   = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                           
    sum_cc_bask_test     = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl     = matching_df.cc_ctr_dur_bask.sum()    
                                                                                   
    ## Unit
    sum_cc_qty_test     = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl     = matching_df.cc_ctr_dur_qty.sum() 

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl 
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    #test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\ 
    
    ## Pat change to use matching_df instead of matching_df2 -- 23 Aug 2022
    
    test_for_graph = pd.DataFrame(matching_df[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
        
    ## end for
    
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
    
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,
                             columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    #sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table, uplift_region, uplift_by_mech
## End def    

#---- Dev Code
def sales_uplift_by_region_mechanic(cmp: CampaignEval,
                                    prd_scope_df: SparkDataFrame,
                                    prd_scope_nm: str):
    """Support multi class, subclass
    change input paramenter to use brand_df and feat_list
    """
    
    from pyspark.sql import functions as F
    import pandas as pd
    
    ## date function -- Pat May 2022
    import datetime
    from datetime import datetime
    from datetime import date
    from datetime import timedelta
    
    txn = cmp.txn
    sales_uplift_lv = cmp.params["cate_lvl"]

    store_matching.get_store_matching_across_region(cmp)
    
    try:
        matching_df_2 = matching_df[['store_id','test_store_region', 'store_mech_set_x', 'ctr_store_cos']].copy()  ## Pat Add store_region - May 2022
        matching_df_2.rename(columns={'store_id':'store_id_test','ctr_store_cos':'store_id_ctr', 'test_store_region':'store_region', 'store_mech_set_x':'store_mech_set'},inplace=True)
        
        print(' Display matching_df_2 \n ')
        matching_df_2.display()
        
        test_ctr_store_id = list(set(matching_df_2['store_id_test']).union(matching_df_2['store_id_ctr'])) ## auto distinct target/control
        
        ## Pat added -- 6 May 2022
        test_store_id_pd = matching_df_2[['store_id_test']].assign(flag_store = 'test').rename(columns={'store_id_test':'store_id'})
        ## create list of target store only
        test_store_list  = list(set(matching_df_2['store_id_test']))
        ## change to use control store count
        ctrl_store_use_cnt         = matching_df_2.groupby('store_id_ctr').agg({'store_id_ctr': ['count']})
        ctrl_store_use_cnt.columns = ['count_used']
        ctrl_store_use_cnt         = ctrl_store_use_cnt.reset_index().rename(columns = {'store_id_ctr':'store_id'})
        
        ctrl_store_id_pd           = ctrl_store_use_cnt[['store_id']].assign(flag_store = 'cont')

        ## create list of control store only
        ctrl_store_list  = list(set(ctrl_store_use_cnt['store_id']))
                
        #print('---- ctrl_store_id_pd - show data ----')
        
        #ctrl_store_id_pd.display()
        
        print('List of control stores = ' + str(ctrl_store_list))
        
        ## concat pandas
        test_ctrl_store_pd = pd.concat([test_store_id_pd, ctrl_store_id_pd])
                        
        ## add drop duplicates for control - to use the same logic to get control factor per store (will merge to matching table and duplicate there instead)
        test_ctrl_store_pd.drop_duplicates()
        ## create_spark_df    
        test_ctrl_store_df = cmp.spark.createDataFrame(test_ctrl_store_pd)
        
    except Exception as e:
        print(e)
        print('Unrecognized store matching methodology')

    # ---------------------------------------------------------------
    ## Pat Adjust -- 6 May 2022 using pandas df to join to have flag of target/control stores
    ## need to get txn of target/control store separately to distinct #customers    
    # ---------------------------------------------------------------
    #---- Uplift
        
    # brand buyer, during campaign
    # Change filter to column offline_online_other_channel - Dec 2022 - Ta
    if sales_uplift_lv.lower() == 'brand':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at BRAND level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        brand_txn = txn.join(brand_df, [txn.upc_id == brand_df.upc_id], 'left_semi')\
                       .join(F.broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                       .where((txn.offline_online_other_channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp'))
                             )

        ## get transcation at brand level for pre period
        pre_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'pre')
        
        ## get transcation at brand level for dur period
        dur_txn_selected_prod_test_ctr_store = brand_txn.where(brand_txn.period_fis_wk == 'cmp')
        
    elif sales_uplift_lv.lower() == 'sku':
        print('-'*50)
        print('Sales uplift based on "OFFLINE" channel only')
        print('Sales uplift at SKU level')
        
        ## get transcation at brand level for all period  - Pat 25 May 2022
        
        feat_txn = txn.join(F.broadcast(test_ctrl_store_df), 'store_id' , 'inner')\
                      .where( (txn.offline_online_other_channel =='OFFLINE') &
                              (txn.period_fis_wk.isin('pre','cmp')) &
                              (txn.upc_id.isin(feat_list))
                            )
        
        ## pre
        pre_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'pre')
        #print(' Check Display(10) pre txn below \n')
        #pre_txn_selected_prod_test_ctr_store.display(10)
        ## during
        dur_txn_selected_prod_test_ctr_store = feat_txn.where(feat_txn.period_fis_wk == 'cmp')
        #print(' Check Display(10) during txn below \n')
        #dur_txn_selected_prod_test_ctr_store.display(10)
    ## end if

    #---- sales by store by period    EPOS
    sales_pre    = pre_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    ## region is in matching table already 
    sales_dur    = dur_txn_selected_prod_test_ctr_store.groupBy('store_id').agg(F.sum('net_spend_amt').alias('sales'))
    
    sales_pre_df = helper.to_pandas(sales_pre)
    sales_dur_df = helper.to_pandas(sales_dur)
    
    
    #---- sales by store by period clubcard
    
    cc_kpi_dur = dur_txn_selected_prod_test_ctr_store.where(F.col('household_id').isNotNull()) \
                                                     .groupBy(F.col('store_id'))\
                                                     .agg( F.sum('net_spend_amt').alias('cc_sales')
                                                          ,F.countDistinct('transaction_uid').alias('cc_bask')
                                                          ,F.sum('pkg_weight_unit').alias('cc_qty')
                                                         )
#    cc_sales_pre_df = to_pandas(cc_sales_pre)
    cc_kpi_dur_df = helper.to_pandas(cc_kpi_dur)
    
    #print('cc_kpi_dur_df')
    #cc_kpi_dur_df.display()
    
    ## create separate test/control txn table for #customers -- Pat May 2022
    
    #pre_txn_test_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    #pre_txn_ctrl_store = pre_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    dur_txn_test_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'test')
    dur_txn_ctrl_store = dur_txn_selected_prod_test_ctr_store.where(F.col('flag_store') == 'cont')
    
    #---- cust by period by store type & period to variable, if customer buy on both target& control, 
    ## will be count in both stores (as need to check market share & behaviour)
    
    cust_dur_test     = dur_txn_test_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_test'))
    
    # get number of customers for test store (customer might dup but will be comparable to control
    
    cust_dur_test_var = cust_dur_test.agg( F.sum('cust_test').alias('cust_test')).collect()[0].cust_test
    
    ## control    
    cust_dur_ctrl     = dur_txn_ctrl_store.groupBy('store_id')\
                                          .agg(F.countDistinct('household_id').alias('cust_ctrl'))
    ## to pandas for join
    cust_dur_ctrl_pd  = helper.to_pandas(cust_dur_ctrl)
    
    ## Danny - convert all store_id to float type
    cust_dur_ctrl_pd["store_id"]   = cust_dur_ctrl_pd["store_id"].astype("float")
    ctrl_store_use_cnt["store_id"] = ctrl_store_use_cnt["store_id"].astype("float")
    
    ## merge with #times control store being used
    cust_dur_ctrl_dup_pd = (ctrl_store_use_cnt.merge (cust_dur_ctrl_pd, how = 'inner', on = ['store_id'])\
                                              .assign(cust_dur_ctrl_dup = lambda x : x['count_used'] * x['cust_ctrl']) \
                           )
    
    print('Display cust_dur_ctrl_dup_pd \n ')
    cust_dur_ctrl_dup_pd.display()
    
    cust_dur_ctrl_var  = cust_dur_ctrl_dup_pd.cust_dur_ctrl_dup.sum()
    
    #### -- End  now geting customers for both test/control which comparable but not correct number of customer ;}  -- Pat 9 May 2022
    
    ## Pat add force datatype - Feb 2023
    
                                                                                    
    #create weekly sales for each item
    wk_sales_pre = helper.to_pandas(pre_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    wk_sales_dur = helper.to_pandas(dur_txn_selected_prod_test_ctr_store.groupBy('store_id').pivot('week_id').agg(F.sum('net_spend_amt').alias('sales')).fillna(0))
    
    #print('wk_sales_pre')
    #wk_sales_pre.display()
    
    #print('wk_sales_dur')
    #wk_sales_dur.display()
    
    #---- Matchingsales_pre_df = to_pandas(sales_pre)

    #### Add visits , unit, customers -- Pat 6 May 2022 
    #print(' Display matching_df_2 Again to check \n ')
    #matching_df_2.display()
    
    ## Replace matching_df Pat add region in matching_df -- May 2022
    
    ## Pat add force datatype -- Feb 2023
    
    matching_df_2["store_id_test"] = matching_df_2["store_id_test"].astype("float")
    matching_df_2["store_id_ctr"]  = matching_df_2["store_id_ctr"].astype("float")
    
    matching_df = (matching_df_2.merge(sales_pre_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={'sales':'test_pre_sum'})
                                .merge(sales_pre_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={'sales':'ctr_pre_sum'})
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                #-- get control factor for all sales, unit,visits
                                .assign(ctr_factor      = lambda x : x['test_pre_sum']  / x['ctr_pre_sum'])
                                #--
                                .fillna({'test_pre_sum':0 , 'ctr_pre_sum':0})
                                
                                #-- during multiply control factor
                                #-- sales
                                .merge(sales_dur_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={'sales':'test_dur_sum'})
                                .merge(sales_dur_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={'sales':'ctr_dur_sum'})
                                .assign(ctr_dur_sum_adjust = lambda x : x['ctr_dur_sum'] * x['ctr_factor'])
                                
                                #-- club card KPI
                                .merge(cc_kpi_dur_df, left_on='store_id_test', right_on='store_id', how = 'left').rename(columns={ 'cc_sales':'cc_test_dur_sum'
                                                                                                                     ,'cc_bask':'cc_test_dur_bask'
                                                                                                                     ,'cc_qty':'cc_test_dur_qty'
                                                                                                                   })
                                .merge(cc_kpi_dur_df, left_on='store_id_ctr', right_on='store_id', how = 'left').rename(columns={ 'cc_sales':'cc_ctr_dur_sum' 
                                                                                                       ,'cc_bask':'cc_ctr_dur_bask'
                                                                                                       ,'cc_qty':'cc_ctr_dur_qty'
                                                                                                        })
                 ).loc[:, [ 'store_id_test'
                           ,'store_id_ctr'
                           , 'store_region'
                           , 'store_mech_set'
                           , 'test_pre_sum'
                           , 'ctr_pre_sum'
                           , 'ctr_factor'
                           , 'test_dur_sum'
                           , 'ctr_dur_sum'
                           , 'ctr_dur_sum_adjust' 
                           , 'cc_test_dur_sum'
                           , 'cc_ctr_dur_sum'
                           , 'cc_test_dur_bask'
                           , 'cc_ctr_dur_bask'
                           , 'cc_test_dur_qty'
                           , 'cc_ctr_dur_qty' ]]
    ##-----------------------------------------------------------
    ## -- Sales uplift at overall level  
    ##-----------------------------------------------------------
    
    print('matching_df')
    
    matching_df_all = matching_df.fillna(0)
    
    matching_df     = matching_df_all.loc[(matching_df_all['ctr_factor'] > 0 ) & (matching_df_all['ctr_pre_sum'] > 0 )]
    
    matching_df.display()
    
    # -- sales uplift overall
    sum_sales_test     = matching_df.test_dur_sum.sum()
    sum_sales_ctrl     = matching_df.ctr_dur_sum.sum()
    sum_sales_ctrl_adj = matching_df.ctr_dur_sum_adjust.sum()

    ## get uplift                                                                  
    sales_uplift     = sum_sales_test - sum_sales_ctrl_adj
    pct_sales_uplift = (sales_uplift / sum_sales_ctrl_adj) 

    print(f'{sales_uplift_lv} sales uplift: {sales_uplift} \n{sales_uplift_lv} %sales uplift: {pct_sales_uplift*100}%')
    ##-----------------------------------------------------------
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Region -- 24 May 2022
    ##-----------------------------------------------------------
    # -- sales uplift by region
    #df4 = df.groupby('Courses', sort=False).agg({'Fee': ['sum'], 'Courses':['count']}).reset_index()
    #df4.columns = ['course', 'c_fee', 'c_count']
    
    ## add sort = false to optimize performance only
    
    sum_sales_test_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'test_dur_sum':['sum'], 'store_region':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_region', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_region         = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_region.columns = ['store_region', 'trg_sales_reg', 'trg_store_cnt', 'ctr_sales_reg', 'ctr_sales_adj_reg']
    
    ## get uplift region
    uplift_region['s_uplift_reg'] = uplift_region['trg_sales_reg'] - uplift_region['ctr_sales_adj_reg']
    uplift_region['pct_uplift']   = uplift_region['s_uplift_reg'] / uplift_region['ctr_sales_adj_reg']

    print('\n' + '-'*80 + '\n Uplift by Region at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_region.display()
    ## ------------------------------------------------------------
    
    ##-----------------------------------------------------------
    ## Pat Add Sales uplift by Mechanic -- 7 Sep 2022 (within campaign mech compare)
    ##-----------------------------------------------------------
    
    sum_sales_test_pd         = matching_df.groupby('store_mech_set', as_index = True, sort=True).agg({'test_dur_sum':['sum'], 'store_mech_set':['count']})
    sum_sales_ctrl_pd         = matching_df.groupby('store_mech_set', as_index = True, sort=False).agg({'ctr_dur_sum':['sum']})
    sum_sales_ctrl_adj_pd     = matching_df.groupby('store_mech_set', as_index = True, sort=False).agg({'ctr_dur_sum_adjust':['sum']})
    
    ## join dataframe
    uplift_by_mech            = pd.concat( [sum_sales_test_pd, sum_sales_ctrl_pd, sum_sales_ctrl_adj_pd], axis = 1, join = 'inner').reset_index()
    uplift_by_mech.columns    = ['store_mech_set', 'trg_sales_mech', 'trg_store_cnt', 'ctr_sales_mech', 'ctr_sales_adj_mech']
    
    ## get uplift by mech
    uplift_by_mech['s_uplift_mech']   = uplift_by_mech['trg_sales_mech'] - uplift_by_mech['ctr_sales_adj_mech']
    uplift_by_mech['pct_uplift_mech'] = uplift_by_mech['s_uplift_mech'] / uplift_by_mech['ctr_sales_adj_mech']

    print('\n' + '-'*80 + '\n Uplift by mechanic set at Level ' + str(sales_uplift_lv) +  '\n' + '-'*80 + '\n')
    uplift_by_mech.display()
    
    ##-----------------------------------------------------------
    
    ## Get KPI   -- Pat 8 May 2022
    
    #customers calculated above already 
#     cust_dur_test     = dur_txn_test_store.agg(F.countDistinct('household_id').alias('cust_test')).collect()[0].['cust_test']
#     cust_dur_ctrl     = dur_txn_ctrl_store.agg(F.countDistinct('household_id').alias('cust_ctrl')).collect().[0].['cust_ctrl']
#     cust_dur_ctrl_adj = cust_dur_ctrl * cf_cust
    
    ## cc_sales                                                                           
    sum_cc_sales_test   = matching_df.cc_test_dur_sum.sum()
    sum_cc_sales_ctrl   = matching_df.cc_ctr_dur_sum.sum()  
    
    ## basket                                                                           
    sum_cc_bask_test     = matching_df.cc_test_dur_bask.sum()
    sum_cc_bask_ctrl     = matching_df.cc_ctr_dur_bask.sum()    
                                                                                   
    ## Unit
    sum_cc_qty_test     = matching_df.cc_test_dur_qty.sum()
    sum_cc_qty_ctrl     = matching_df.cc_ctr_dur_qty.sum() 

    ## Spend per Customers
                                                                                   
    spc_test = (sum_cc_sales_test/cust_dur_test_var)
    spc_ctrl = (sum_cc_sales_ctrl/cust_dur_ctrl_var)
                                                                                   
    #-- VPC (visits frequency)
                                                                                   
    vpc_test = (sum_cc_bask_test/cust_dur_test_var)
    vpc_ctrl = (sum_cc_bask_ctrl/cust_dur_ctrl_var)
                                                    
    #-- SPV (Spend per visits)
                                                                                   
    spv_test = (sum_cc_sales_test/sum_cc_bask_test)
    spv_ctrl = (sum_cc_sales_ctrl/sum_cc_bask_ctrl)

    #-- UPV (Unit per visits - quantity)
                                                                                   
    upv_test = (sum_cc_qty_test/sum_cc_bask_test)
    upv_ctrl = (sum_cc_qty_ctrl/sum_cc_bask_ctrl)

    #-- PPU (Price per unit - avg product price)
                                                                                   
    ppu_test = (sum_cc_sales_test/sum_cc_qty_test)
    ppu_ctrl = (sum_cc_sales_ctrl/sum_cc_qty_ctrl)
    
    ### end get KPI -----------------------------------------------
                                                                                   
    uplift_table = pd.DataFrame([[sales_uplift, pct_sales_uplift]],
                                index=[sales_uplift_lv], columns=['sales_uplift','%uplift'])
    
    ## Add KPI table -- Pat 8 May 2022
    kpi_table = pd.DataFrame( [[ sum_sales_test
                                ,sum_sales_ctrl
                                ,sum_sales_ctrl_adj
                                ,sum_cc_sales_test
                                ,sum_cc_sales_ctrl
                                ,cust_dur_test_var
                                ,cust_dur_ctrl_var
                                ,sum_cc_bask_test
                                ,sum_cc_bask_ctrl
                                ,sum_cc_qty_test
                                ,sum_cc_qty_ctrl 
                                ,spc_test
                                ,spc_ctrl
                                ,vpc_test
                                ,vpc_ctrl
                                ,spv_test
                                ,spv_ctrl
                                ,upv_test
                                ,upv_ctrl
                                ,ppu_test
                                ,ppu_ctrl
                               ]]
                            ,index=[sales_uplift_lv]
                            ,columns=[ 'sum_sales_test'
                                      ,'sum_sales_ctrl'
                                      ,'sum_sales_ctrl_adj'
                                      ,'sum_cc_sales_test'
                                      ,'sum_cc_sales_ctrl'
                                      ,'cust_dur_test_var'
                                      ,'cust_dur_ctrl_var'
                                      ,'sum_cc_bask_test'
                                      ,'sum_cc_bask_ctrl'
                                      ,'sum_cc_qty_test'
                                      ,'sum_cc_qty_ctrl'
                                      ,'spc_test'
                                      ,'spc_ctrl'
                                      ,'vpc_test'
                                      ,'vpc_ctrl'
                                      ,'spv_test'
                                      ,'spv_ctrl'
                                      ,'upv_test'
                                      ,'upv_ctrl'
                                      ,'ppu_test'
                                      ,'ppu_ctrl'
                                     ] 
                            ) ## end pd.DataFrame
    
    #---- Weekly sales for plotting
    # Test store
    #test_for_graph = pd.DataFrame(matching_df_2[['store_id_test']]\ 
    
    ## Pat change to use matching_df instead of matching_df2 -- 23 Aug 2022
    
    test_for_graph = pd.DataFrame(matching_df[['store_id_test']]\
                       .merge(wk_sales_pre,left_on='store_id_test',right_on='store_id')\
                       .merge(wk_sales_dur,on='store_id').iloc[:,2:].sum(axis=0),columns=[f'{sales_uplift_lv}_test']).T
#     test_for_graph.display()
    
    # Control store , control factor
    ctr_for_graph = matching_df[['store_id_ctr','ctr_factor']]\
                    .merge(wk_sales_pre,left_on='store_id_ctr',right_on='store_id')\
                    .merge(wk_sales_dur,on='store_id')
#     ctr_for_graph.head()
    
    # apply control factor for each week and filter only that
    new_c_list=[]
    
    for c in ctr_for_graph.columns[3:]:
        new_c = c+'_adj'
        ctr_for_graph[new_c] = ctr_for_graph[c] * ctr_for_graph.ctr_factor
        new_c_list.append(new_c)
        
    ## end for
    
    ctr_for_graph_2 = ctr_for_graph[new_c_list].sum(axis=0)
    
    #create df with agg value
    ctr_for_graph_final = pd.DataFrame(ctr_for_graph_2,
                             columns=[f'{sales_uplift_lv}_ctr']).T
    #change column name to be the same with test_feature_for_graph
    ctr_for_graph_final.columns = test_for_graph.columns

    sales_uplift = pd.concat([test_for_graph,ctr_for_graph_final])
    #sales_uplift.reset_index().display()
    
    return matching_df, uplift_table, sales_uplift, kpi_table, uplift_region, uplift_by_mech
## End def    