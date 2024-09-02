create table `gold.final_de_output` as (
  select 
      consumer.consumer_id,
      consumer.sex,
      consumer.age,
      date_diff(avocado.sold_date, avocado.birth_date, day) as avocado_days_old,
      avocado.ripe_index_when_picked as avocado_ripe_index,
      date_diff(avocado.sold_date, avocado.picked_date, day) as avocado_days_picked,
      fertilizer.type as fertilizer_type
  from
    `silver.consumer_silver` as consumer
  inner join 
    `silver.avocado_silver` as avocado
    on consumer.consumer_id = avocado.consumer_id
  inner join
    `silver.fertilizer_silver` as fertilizer
    on avocado.purchase_id = fertilizer.purchase_id
)
