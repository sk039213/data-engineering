create or replace table `silver.consumer_silver`
as (
  select
      consumerid as consumer_id,
      sex,
      ethnicity,
      race,
      age
  from `bronze.consumer_bronze`
);

create or replace table `silver.avocado_silver` as (
  select
      purchaseid as purchase_id,
      consumerid as consumer_id,
      avocado_bunch_id,
      plu,
      cast(`ripe index when picked` as int) as ripe_index_when_picked,
      born_date as birth_date,
      case
        when cast(cast(picked_date as datetime) as date) between born_date and sold_date then 
          cast(cast(picked_date as datetime) as date)
        else sold_date
        end as picked_date,
      sold_date
  from `bronze.avocado_bronze`
  order by avocado_bunch_id
);

create or replace table `silver.fertilizer_silver` as (
  select
      fertilizerid as fertilizer_id,
      consumerid as consumer_id,
      purchaseid as purchase_id,
      type,
      mg,
      frequency
  from `bronze.fertilizer_bronze`
);

create or replace table `silver.purchase_silver` as (
  select
      purchaseid as purchase_id,
      consumerid as consumer_id,
      cast(graphed_date as date) as graphed_date,
      avocado_bunch_id,
      reporting_year,
      `QA Process` as qa_process,
      billing_provider_sku,
      grocery_store_id,
      price_index
  from `bronze.purchase_bronze`
  where purchaseid is not null
);
