Queries by Section in the dashboard:

#----------------------------- Transactions -----------------------------#

sql_statement_01 = """
select block_timestamp::date as date,
  count(distinct from_address) as total_unique_users,
  count(distinct tx_hash) as total_transactions,
  sum(eth_value) as total_volume_transacted_eth,
  avg(eth_value) as average_eth_per_transaction,
  max(eth_value) as max_eth_transacted,
  sum(tx_fee) as total_volume_fees,
  avg(tx_fee) as average_fees_per_transaction,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_transactions
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  order by 1
"""

#-----

sql_statement_02 = """
with transactions_data as(
  select block_timestamp::date as date,
  count(distinct from_address) as total_unique_users,
  count(distinct tx_hash) as total_transactions,
  sum(eth_value) as total_volume_transacted_eth,
  avg(eth_value) as average_eth_per_transaction,
  max(eth_value) as max_eth_transacted,
  sum(tx_fee) as total_volume_fees,
  avg(tx_fee) as average_fees_per_transaction,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_transactions
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

  select 'Q1' as quarters,
  avg(total_volume_transacted_eth) as average_volume_per_day_eth,
  avg(max_eth_transacted) as average_max_eth_transacted_per_day,
  avg(total_unique_users) as average_users_per_day,
  avg(total_transactions) as average_transactions_per_day,
  avg(average_eth_per_transaction) as average_eth_per_transaction,
  avg(total_volume_fees) as average_total_fees_eth,
  avg(average_fees_per_transaction) as average_fees_per_transaction_eth
  from transactions_data
  where quarters = 'Q1'
  UNION ALL
  select 'Q2' as quarters,
  avg(total_volume_transacted_eth) as average_volume_per_day_eth,
  avg(max_eth_transacted) as average_max_eth_transacted_per_day,
  avg(total_unique_users) as average_users_per_day,
  avg(total_transactions) as average_transactions_per_day,
  avg(average_eth_per_transaction) as average_eth_per_transaction,
  avg(total_volume_fees) as average_total_fees_eth,
  avg(average_fees_per_transaction) as average_fees_per_transaction_eth
  from transactions_data
  where quarters = 'Q2'
  UNION ALL
  select 'Q3' as quarters,
  avg(total_volume_transacted_eth) as average_volume_per_day_eth,
  avg(max_eth_transacted) as average_max_eth_transacted_per_day,
  avg(total_unique_users) as average_users_per_day,
  avg(total_transactions) as average_transactions_per_day,
  avg(average_eth_per_transaction) as average_eth_per_transaction,
  avg(total_volume_fees) as average_total_fees_eth,
  avg(average_fees_per_transaction) as average_fees_per_transaction_eth
  from transactions_data
  where quarters = 'Q3'
  UNION ALL
  select 'Q4' as quarters,
  avg(total_volume_transacted_eth) as average_volume_per_day_eth,
  avg(max_eth_transacted) as average_max_eth_transacted_per_day,
  avg(total_unique_users) as average_users_per_day,
  avg(total_transactions) as average_transactions_per_day,
  avg(average_eth_per_transaction) as average_eth_per_transaction,
  avg(total_volume_fees) as average_total_fees_eth,
  avg(average_fees_per_transaction) as average_fees_per_transaction_eth
  from transactions_data
  where quarters = 'Q4'
"""

#----------------------------- Block Data -----------------------------#

sql_statement_11 = """
with blocks_data_opt as(
  select date_trunc('day',block_timestamp) as date,
  count(block_number) as blocks,
  sum(gas_used)/count(block_number) as average_gwei_per_block,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_blocks
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and tx_count > 0
  group by 1,4
  ),

blocks_data_eth as(
  select date_trunc('day',block_timestamp) as date,
  count(distinct miner) as unique_miners
  from ethereum.core.fact_blocks
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and network = 'mainnet'
  and tx_count > 0
  group by 1
  )
  
select *
  from blocks_data_opt
  join blocks_data_eth using (date)
  order by 1
"""

#-----

sql_statement_12 = """
with blocks_data_opt as(
  select date_trunc('day',block_timestamp) as date,
  count(block_number) as blocks,
  sum(gas_used)/count(block_number) as average_gwei_per_block,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_blocks
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and tx_count > 0
  group by 1,4
  ),

blocks_data_eth as(
  select date_trunc('day',block_timestamp) as date,
  count(distinct miner) as unique_miners
  from ethereum.core.fact_blocks
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and network = 'mainnet'
  and tx_count > 0
  group by 1
  ),

opt_and_eth as (
  select *
  from blocks_data_opt
  join blocks_data_eth using (date)
  )

  select 'Q1' as quarters,
  avg(blocks) as average_blocks_per_day,
  avg(unique_miners) as average_miners_per_day,
  avg(average_gwei_per_block) as average_gwei_per_block_per_day
  from opt_and_eth
  where quarters = 'Q1'
  UNION ALL
  select 'Q2' as quarters,
  avg(blocks) as average_blocks_per_day,
  avg(unique_miners) as average_miners_per_day,
  avg(average_gwei_per_block) as average_gwei_per_block_per_day
  from opt_and_eth
  where quarters = 'Q2'
  UNION ALL
  select 'Q3' as quarters,
  avg(blocks) as average_blocks_per_day,
  avg(unique_miners) as average_miners_per_day,
  avg(average_gwei_per_block) as average_gwei_per_block_per_day
  from opt_and_eth
  where quarters = 'Q3'
  UNION ALL
  select 'Q4' as quarters,
  avg(blocks) as average_blocks_per_day,
  avg(unique_miners) as average_miners_per_day,
  avg(average_gwei_per_block) as average_gwei_per_block_per_day
  from opt_and_eth
  where quarters = 'Q4'
"""

#----------------------------- Swap Data -----------------------------#

#SUSHISWAP
sql_statement_211 = """
select block_timestamp::date as date,
  'Sushiswap' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.sushi.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
"""

#-----

#UNISWAP
sql_statement_212 = """
select a.block_timestamp::date as date,
  'Uniswap' as exchange,
  count(distinct a.tx_hash) as swaps,
  count(distinct a.origin_from_address) as users_swapping,
  sum((c.price * b.raw_amount)/pow(10, c.decimals)) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_event_logs a
  join optimism.core.fact_token_transfers b using(tx_hash)
  join optimism.core.fact_hourly_token_prices c on c.token_address = b.contract_address and date_trunc('hour', b.block_timestamp) = c.hour
  where a.event_name = 'Swap'
  and a.origin_to_address in ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45','0xe592427a0aece92de3edee1f18e0157c05861564')
  and a.tx_status ='SUCCESS'
  and a.block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
"""

#-----

#VELODROME
sql_statement_213 = """
select block_timestamp::date as date,
  'Velodrome' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.velodrome.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
"""

#-----  -----#

#SUSHISWAP
sql_statement_221 = """
with sushiswap_data as (
  select block_timestamp::date as date,
  'Sushiswap' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.sushi.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

  select 'Q1' as quarters,
  COALESCE(sum(swaps),0) as total_swaps,
  COALESCE(sum(users_swapping),0) as total_users_swapping,
  COALESCE(sum(volume_usd),0) as total_volume
  from sushiswap_data
  where quarters='Q1'
  UNION ALL
  select 'Q2' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from sushiswap_data
  where quarters='Q2'
  UNION ALL
  select 'Q3' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from sushiswap_data
  where quarters='Q3'
  UNION ALL
  select 'Q4' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from sushiswap_data
  where quarters='Q4'
"""

#-----

#UNISWAP
sql_statement_222 = """
with uniswap_data as (
  select a.block_timestamp::date as date,
  'Uniswap' as exchange,
  count(distinct a.tx_hash) as swaps,
  count(distinct a.origin_from_address) as users_swapping,
  sum((c.price * b.raw_amount)/pow(10, c.decimals)) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.fact_event_logs a
  join optimism.core.fact_token_transfers b using(tx_hash)
  join optimism.core.fact_hourly_token_prices c on c.token_address = b.contract_address and date_trunc('hour', b.block_timestamp) = c.hour
  where a.event_name = 'Swap'
  and a.origin_to_address in ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45','0xe592427a0aece92de3edee1f18e0157c05861564')
  and a.tx_status ='SUCCESS'
  and a.block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

  select 'Q1' as quarters,
  COALESCE(sum(swaps),0) as total_swaps,
  COALESCE(sum(users_swapping),0) as total_users_swapping,
  COALESCE(sum(volume_usd),0) as total_volume
  from uniswap_data
  where quarters='Q1'
  UNION ALL
  select 'Q2' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from uniswap_data
  where quarters='Q2'
  UNION ALL
  select 'Q3' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from uniswap_data
  where quarters='Q3'
  UNION ALL
  select 'Q4' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from uniswap_data
  where quarters='Q4'
"""

#-----

#VELODROME
sql_statement_223 = """
with velodrome_data as (
  select block_timestamp::date as date,
  'Velodrome' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.velodrome.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

  select 'Q1' as quarters,
  COALESCE(sum(swaps),0) as total_swaps,
  COALESCE(sum(users_swapping),0) as total_users_swapping,
  COALESCE(sum(volume_usd),0) as total_volume
  from velodrome_data
  where quarters='Q1'
  UNION ALL
  select 'Q2' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from velodrome_data
  where quarters='Q2'
  UNION ALL
  select 'Q3' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from velodrome_data
  where quarters='Q3'
  UNION ALL
  select 'Q4' as quarters,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume
  from velodrome_data
  where quarters='Q4'
"""

#-----

#Comparison1
sql_statement_231 = """
with sushiswap_data as (
  select block_timestamp::date as date,
  'Sushiswap' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd
  from optimism.sushi.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  ),

uniswap_data as (
  select a.block_timestamp::date as date,
  'Uniswap' as exchange,
  count(distinct a.tx_hash) as swaps,
  count(distinct a.origin_from_address) as users_swapping,
  sum((c.price * b.raw_amount)/pow(10, c.decimals)) as volume_usd
  from optimism.core.fact_event_logs a
  join optimism.core.fact_token_transfers b using(tx_hash)
  join optimism.core.fact_hourly_token_prices c on c.token_address = b.contract_address and date_trunc('hour', b.block_timestamp) = c.hour
  where a.event_name = 'Swap'
  and a.origin_to_address in ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45','0xe592427a0aece92de3edee1f18e0157c05861564')
  and a.tx_status ='SUCCESS'
  and a.block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  ),

velodrome_data as (
  select block_timestamp::date as date,
  'Velodrome' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd
  from optimism.velodrome.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

select *
from sushiswap_data
UNION ALL
select *
from uniswap_data
UNION ALL
select *
from velodrome_data
order by 1
"""

#-----

#Comparison2
sql_statement_232 = """
with sushiswap_data as (
  select block_timestamp::date as date,
  'Sushiswap' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd
  from optimism.sushi.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  ),

uniswap_data as (
  select a.block_timestamp::date as date,
  'Uniswap' as exchange,
  count(distinct a.tx_hash) as swaps,
  count(distinct a.origin_from_address) as users_swapping,
  sum((c.price * b.raw_amount)/pow(10, c.decimals)) as volume_usd
  from optimism.core.fact_event_logs a
  join optimism.core.fact_token_transfers b using(tx_hash)
  join optimism.core.fact_hourly_token_prices c on c.token_address = b.contract_address and date_trunc('hour', b.block_timestamp) = c.hour
  where a.event_name = 'Swap'
  and a.origin_to_address in ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45','0xe592427a0aece92de3edee1f18e0157c05861564')
  and a.tx_status ='SUCCESS'
  and a.block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  ),

velodrome_data as (
  select block_timestamp::date as date,
  'Velodrome' as exchange,
  count(distinct tx_hash) as swaps,
  count(distinct origin_from_address) as users_swapping,
  sum(amount_in_usd) as volume_usd
  from optimism.velodrome.ez_swaps
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  ),

main as (
  select *
  from sushiswap_data
  UNION ALL
  select *
  from uniswap_data
  UNION ALL
  select *
  from velodrome_data
  )

select exchange,
  sum(swaps) as total_swaps,
  sum(users_swapping) as total_users_swapping,
  sum(volume_usd) as total_volume,
  row_number() over (order by total_volume desc) as rank1,
  row_number() over (order by total_users_swapping desc) as rank2,
  row_number() over (order by total_swaps desc) as rank3
  from main
  group by 1
"""

#----------------------------- NFTs -----------------------------#

sql_statement_31 = """
select block_timestamp::date as date,
  sum(price_usd) as sales_volume_usd,
  count(distinct project_name) as unique_collections_sold,
  count(tx_hash) as NFTs_sold,
  count(distinct seller_address) as unique_sellers,
  NFTs_sold/unique_sellers as average_nfts_sold_per_seller,
  sales_volume_usd/unique_collections_sold as average_sales_volume_per_collection,
  NFTs_sold/unique_collections_sold as average_NFTs_sold_per_collection,
  sales_volume_usd/NFTs_sold as average_sales_price,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.ez_nft_sales
  join optimism.core.dim_labels on nft_address = address
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
"""

#-----

sql_statement_32 = """
with NFT_data as (
  select block_timestamp::date as date,
  sum(price_usd) as sales_volume_usd,
  count(distinct project_name) as unique_collections_sold,
  count(tx_hash) as NFTs_sold,
  count(distinct seller_address) as unique_sellers,
  NFTs_sold/unique_sellers as average_nfts_sold_per_seller,
  sales_volume_usd/unique_collections_sold as average_sales_volume_per_collection,
  NFTs_sold/unique_collections_sold as average_NFTs_sold_per_collection,
  sales_volume_usd/NFTs_sold as average_sales_price,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from optimism.core.ez_nft_sales
  join optimism.core.dim_labels on nft_address = address
  where block_timestamp between '2022-01-01' and '2022-12-31'
  group by 1
  )

  select 'Q1' as quarters,
  avg(sales_volume_usd) as sales_volume_usd_per_day,
  avg(unique_collections_sold) as average_unique_collections_sold_per_day,
  avg(NFTs_sold) as average_NFTs_sold_per_day,
  avg(unique_sellers) as average_unique_sellers_per_day,
  avg(average_nfts_sold_per_seller) as average_nfts_sold_per_seller_per_day,
  avg(average_sales_volume_per_collection) as average_sales_volume_per_collection_per_day,
  avg(average_NFTs_sold_per_collection) as average_NFTs_sold_per_collection_per_day,
  avg(average_sales_price) as average_sales_price_per_day
  from NFT_data
  where quarters = 'Q1'
  UNION ALL
  select 'Q2' as quarters,
  avg(sales_volume_usd) as sales_volume_usd_per_day,
  avg(unique_collections_sold) as average_unique_collections_sold_per_day,
  avg(NFTs_sold) as average_NFTs_sold_per_day,
  avg(unique_sellers) as average_unique_sellers_per_day,
  avg(average_nfts_sold_per_seller) as average_nfts_sold_per_seller_per_day,
  avg(average_sales_volume_per_collection) as average_sales_volume_per_collection_per_day,
  avg(average_NFTs_sold_per_collection) as average_NFTs_sold_per_collection_per_day,
  avg(average_sales_price) as average_sales_price_per_day
  from NFT_data
  where quarters = 'Q2'
  UNION ALL
  select 'Q3' as quarters,
  avg(sales_volume_usd) as sales_volume_usd_per_day,
  avg(unique_collections_sold) as average_unique_collections_sold_per_day,
  avg(NFTs_sold) as average_NFTs_sold_per_day,
  avg(unique_sellers) as average_unique_sellers_per_day,
  avg(average_nfts_sold_per_seller) as average_nfts_sold_per_seller_per_day,
  avg(average_sales_volume_per_collection) as average_sales_volume_per_collection_per_day,
  avg(average_NFTs_sold_per_collection) as average_NFTs_sold_per_collection_per_day,
  avg(average_sales_price) as average_sales_price_per_day
  from NFT_data
  where quarters = 'Q3'
  UNION ALL
  select 'Q4' as quarters,
  avg(sales_volume_usd) as sales_volume_usd_per_day,
  avg(unique_collections_sold) as average_unique_collections_sold_per_day,
  avg(NFTs_sold) as average_NFTs_sold_per_day,
  avg(unique_sellers) as average_unique_sellers_per_day,
  avg(average_nfts_sold_per_seller) as average_nfts_sold_per_seller_per_day,
  avg(average_sales_volume_per_collection) as average_sales_volume_per_collection_per_day,
  avg(average_NFTs_sold_per_collection) as average_NFTs_sold_per_collection_per_day,
  avg(average_sales_price) as average_sales_price_per_day
  from NFT_data
  where quarters = 'Q4'
"""

#-----

#Other Metrics 1
sql_statement_41 = """
select dayname(to_date(block_timestamp::date)) as day_of_the_week,
  'Q1' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-04-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select dayname(to_date(block_timestamp::date)) as day_of_the_week,
  'Q2' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-04-01' and '2022-07-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select dayname(to_date(block_timestamp::date)) as day_of_the_week,
  'Q3' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-07-01' and '2022-10-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select dayname(to_date(block_timestamp::date)) as day_of_the_week,
  'Q4' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-10-01' and '2022-12-31'
  and price_usd > 0
  group by 1
"""

#-----

#Other Metrics 2
sql_statement_42 = """
select date_part('hour', block_timestamp) as hour_of_the_day,
  'Q1' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-04-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select date_part('hour', block_timestamp) as hour_of_the_day,
  'Q2' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-04-01' and '2022-07-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select date_part('hour', block_timestamp) as hour_of_the_day,
  'Q3' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-07-01' and '2022-10-01'
  and price_usd > 0
  group by 1
  UNION ALL
  select date_part('hour', block_timestamp) as hour_of_the_day,
  'Q4' as quarter,
  count(distinct tx_hash) as nft_sales,
  sum(price_usd) as total_volume_usd,
  avg(price_usd) as average_volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-10-01' and '2022-12-31'
  and price_usd > 0
  group by 1
"""

#-----
#Other Metrics 3
sql_statement_43 = """
with buys as (
  select min(block_timestamp::date) as buy_date,
  buyer_address,
  concat(nft_address, '|', tokenid) as nft
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 2,3
  ),
  
  sales as (
  select min(block_timestamp::date) as sale_date,
  seller_address,
  concat(nft_address, '|', tokenid) as nft
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  and seller_address in (select buyer_address from buys) and nft in (select nft from buys) 
  group by 2,3
  ),
  
  main as (
  select datediff(day, buy_date, sale_date) as time_holding,
  a.nft,
  buyer_address,
  seller_address
  from sales a
  inner join buys b on a.seller_address = b.buyer_address and a.nft = b.nft
  where time_holding >= 0
  )
  
  select avg(time_holding) as average_time_holding
  from main
"""

import math

millnames = ['',' k',' M',' B',' T']

def millify(n):
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

fig_43 = millify(df_43['average_time_holding'][0])

#-----

#Other Metrics 4
sql_statement_44 = """

with buys as (
  select min(block_timestamp::date) as buy_date,
  buyer_address,
  concat(nft_address, '|', tokenid) as nft
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 2,3
  ),
  
  sales as (
  select min(block_timestamp::date) as sale_date,
  seller_address,
  concat(nft_address, '|', tokenid) as nft
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  and seller_address in (select buyer_address from buys) and nft in (select nft from buys) 
  group by 2,3
  ),
  
  main as (
  select datediff(day, buy_date, sale_date) as time_holding,
  a.nft,
  buyer_address,
  seller_address
  from sales a
  inner join buys b on a.seller_address = b.buyer_address and a.nft = b.nft
  where time_holding >= 0
  )
  
select count(buyer_address) as buyers,
  case when time_holding <= 1 then 'Held for less than 1 Day'
  when time_holding > 1 and time_holding <= 7 then 'Held Between 1 Day and 1 Week'
  when time_holding > 7 and time_holding <= 30 then 'Held Between 1 Week and 1 Month'
  else 'Held for Over 1 Month' end as category
  from main
  group by 2
"""

#-----

#Other Metrics 5
sql_statement_45 = """
with holders as (
  select block_timestamp, 
  tx_hash,
  concat(nft_address, '|', tokenid) as nft,
  buyer_address,
  price_usd as amount
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  qualify row_number() over(partition by nft order by block_timestamp desc) = 1
  ),

main as (
  select buyer_address,
  count(nft) as nfts_owned
  from holders
  group by 1
  )

  select count(*) as holders,
  case when nfts_owned = 1 then '1 NFT Held'
  when nfts_owned > 1 and nfts_owned <= 3 then 'Holding Between 2 and 3 NFTs'
  when nfts_owned > 3 and nfts_owned <= 5 then 'Holding Between 4 and 5 NFTs'
  when nfts_owned > 5 and nfts_owned <= 10 then 'Holding Between 6 and 10 NFTs'
  else 'Over 10 NFTs Held' end as holder_category
  from main
  group by 2
"""

#-----

#Other Metrics 6
sql_statement_46 = """
select buyer_address,
  sum(price_usd) as volume_usd,
  count(distinct tx_hash) as sales
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  qualify row_number() over (order by sales desc) <= 10
"""

#-----

#Other Metrics 7
sql_statement_47 = """
select buyer_address,
  sum(price_usd) as volume_usd,
  count(distinct tx_hash) as sales
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  qualify row_number() over (order by volume_usd desc) <= 10
"""

#-----

#Other Metrics 8
sql_statement_48 = """
select seller_address,
  sum(price_usd) as volume_usd,
  count(distinct tx_hash) as sales
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  qualify row_number() over (order by sales desc) <= 10
"""

#-----

#Other Metrics 9
sql_statement_49 = """
select seller_address,
  sum(price_usd) as volume_usd,
  count(distinct tx_hash) as sales
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  qualify row_number() over (order by volume_usd desc) <= 10
"""

#-----

#Other Metrics 10
sql_statement_410 = """
with main as (
  select nft_address as project, 
  count(tx_hash) as sales,
  sum(price_usd) as volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  )
  
select project,
  project_name,
  sales,
  volume_usd
  from optimism.core.dim_labels
  inner join main on project = address
  qualify row_number() over (order by sales desc) <= 10
"""

#-----

#Other Metrics 11
sql_statement_411 = """
with main as (
  select nft_address as project, 
  count(tx_hash) as sales,
  sum(price_usd) as volume_usd
  from optimism.core.ez_nft_sales
  where block_timestamp between '2022-01-01' and '2022-12-31'
  and price_usd > 0
  group by 1
  )
  
select project,
  project_name,
  sales,
  volume_usd
  from optimism.core.dim_labels
  inner join main on project = address
  qualify row_number() over (order by volume_usd desc) <= 10
"""

#----------------------------- Introduction -----------------------------#

#ETH's Price
sql_statement_5 = """
select date_trunc('day', hour) as date,
  avg(price) as eth_price_usd,
  case when date >= '2022-01-01' and  date < '2022-04-01' then 'Q1'
  when date >= '2022-04-01' and  date < '2022-07-01' then 'Q2'
  when date >= '2022-07-01' and  date < '2022-10-01' then 'Q3'
  else 'Q4'
  end as quarters
  from ethereum.core.fact_hourly_token_prices
  where token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
  and hour::date between '2022-01-01' and '2022-12-31'
  group by 1
"""
