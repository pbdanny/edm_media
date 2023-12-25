# Databricks notebook source
library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(method = "databricks")
sale_uplift_by_store <- spark_read_csv(sc = sc, name="sales_uplift", path = "/FileStore/media/campaign_eval/01_hde/Apr_2023/2023_0035_M06E_Dettol_ShelfDivider/output/sku_sales_matching_df_info.csv")

# COMMAND ----------

sale_uplift_by_store

# COMMAND ----------

colnames(sale_uplift_by_store)

# COMMAND ----------

postv_uplft <- sale_uplift_by_store %>% 
  mutate(uplift = test_dur_sum - ctr_dur_sum_adjust) %>%
  select(store_id_test:store_region, test_dur_sum, ctr_dur_sum_adjust, uplift) %>%
  arrange(desc(uplift)) %>%
  ggplot(aes(uplift)) + geom_histogram()

postv_uplft

# COMMAND ----------

src_databases(sc)

# COMMAND ----------

options(sparklyr.dplyr_distinct.impl = "tbl_lazy")

tbl_change_db(sc, "tdm")

TBL_ITEM <- "v_transaction_item"
TBL_BASK <- "v_transaction_head"
TBL_PROD <- "v_prod_dim_c"
TBL_STORE <- "v_store_dim"
TBL_DATE <- "v_date_dim"
TBL_CUST <- "v_customer_dim"

itm <- spark_read_table(sc, TBL_ITEM, memory=FALSE) 
bask <- spark_read_table(sc, TBL_BASK, memory=FALSE) %>% distinct(transaction_uid, pos_type, .keep_all=FALSE)
store <- spark_read_table(sc, TBL_STORE, memory=FALSE) %>% filter(format_id %in% c(1,2,3,4,5)) %>% distinct(store_id, format_id, format_name)
prd <- spark_read_table(sc, TBL_PROD, memory=FALSE) %>% filter(division_id %in% c(1,2,3,4,9,10,13)) %>% distinct(upc_id)
cust <- spark_read_table(sc, TBL_CUST, memory=FALSE) %>% distinct(customer_id, household_id, .keep_all=FALSE)

# COMMAND ----------

colnames(cust)

# COMMAND ----------

txn <- itm %>% 
  inner_join(prd, by = "upc_id") %>% 
  inner_join(store, by = "store_id") %>% 
  left_join(bask, by = "transaction_uid") %>%
  left_join(cust, by = "customer_id")

# COMMAND ----------

colnames(txn)

# COMMAND ----------

g <- txn %>%
  filter(week_id == 202335) %>%
  group_by(date_id) %>%
  summarise(sales = sum(net_spend_amt)) %>%
  ggplot(aes(x = date_id, y = sales)) + geom_line()

# COMMAND ----------

g

# COMMAND ----------


