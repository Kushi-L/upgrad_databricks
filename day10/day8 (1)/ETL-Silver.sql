-- Databricks notebook source
-- MAGIC %run
-- MAGIC "/Workspace/Users/kushi_1724384751548@npupgradassessment.onmicrosoft.com/day10/day8 (1)/includes"

-- COMMAND ----------

create table if not exists silver.products_silver as (select ProductID as product_id, ProductName as product_name ,Category as category, ListPrice as list_price from demoworkspace_3338812286142257.bronze.products_bronze)

-- COMMAND ----------

create table if not exists gold.category_count as select category, count(*) as count from demoworkspace_3338812286142257.silver.products_silver group by category order by count desc

-- COMMAND ----------

select * from gold.category_count
