-- Title: HoprBoost NFT Analysis

WITH 

mintCall as (
    select "boostType" ,"call_tx_hash" from hopr_protocol."HoprBoost_call_mint" 
    where "call_success" = true
    union 
    select "boostType" ,"call_tx_hash" from hopr_protocol."HoprBoost_call_batchMint" 
    where "call_success" = true
)

,allMinted as ( 
    select "to", "tokenId", "evt_tx_hash", "evt_block_time", "boostType"  
    from hopr_protocol."HoprBoost_evt_Transfer" 
    inner join mintCall on "call_tx_hash" = "evt_tx_hash"
    where "from" = '\x0000000000000000000000000000000000000000' 
)

,whitelistTokenId as (
    select distinct "tokenId" from allMinted where "boostType" != 'HODLr'
)

,minted as (
    select * from allMinted 
    where "tokenId" in (select "tokenId" from whitelistTokenId)
)

,traded as ( 
    select "to","from", "tokenId", "evt_tx_hash", "evt_block_time"  
    from hopr_protocol."HoprBoost_evt_Transfer"
    where "from" <> '\x0000000000000000000000000000000000000000'  -- exclude minted
    and "to" not in (
        '\x912F4d6607160256787a2AD40dA098Ac2aFE57AC' -- s 1
        ,'\x2cDD13ddB0346E0F620C8E5826Da5d7230341c6E' -- s 2
        ,'\xae933331ef0be122f9499512d3ed4fa3896dcf20' -- s 3
    )
    and "tokenId" in (select "tokenId" from whitelistTokenId)
)

-- # How many are earned vs transferred
,earned as ( 
    select distinct "tokenId" from minted
    EXCEPT
    select distinct "tokenId" from traded
)

,transferred as ( 
    select distinct "tokenId" from traded  
)


,staked as (
    select distinct "tokenId" from (
        SELECT "boostTokenId" as "tokenId"  FROM hopr_protocol."HoprStake_evt_Redeemed"
        WHERE "factorRegistered" = true
        union
        SELECT "boostTokenId" as "tokenId"   FROM hopr_protocol."HoprStake2_evt_Redeemed"
        WHERE "factorRegistered" = true
        union
        SELECT "boostTokenId" as "tokenId"  FROM hopr_protocol."HoprStakeSeason3_evt_Redeemed"
        WHERE "factorRegistered" = true
    ) s
    where "tokenId" in (select "tokenId" from whitelistTokenId)
)

,stakedEarned as ( 
    select distinct "tokenId" from staked
    intersect
    select distinct "tokenId" from earned
)

,holder as (
    select distinct "tokenId", first_value("to") OVER (partition by "tokenId" order by evt_block_number desc, evt_index desc) as address
    from hopr_protocol."HoprBoost_evt_Transfer"
    where "to" not in 
    (
    '\xae933331ef0be122f9499512d3ed4fa3896dcf20',
    '\x2cdd13ddb0346e0f620c8e5826da5d7230341c6e',
    '\x912f4d6607160256787a2ad40da098ac2afe57ac'
    )
    and "tokenId" in (select "tokenId" from whitelistTokenId)
)

,rawMetrics as (
    SELECT
        "boostType",
        m."tokenId",
        1 as minted,
        (CASE WHEN earned."tokenId" is not null THEN 1 ELSE null END) AS earned,
        (CASE WHEN transferred."tokenId" is not null THEN 1 ELSE null END) AS transferred,
        (CASE WHEN staked."tokenId" is not null THEN 1 ELSE null END) AS staked,
        (CASE WHEN stakedEarned."tokenId" is not null THEN 1 ELSE null END) AS stakedEarned,
        (CASE WHEN staked."tokenId" is not null 
            and earned."tokenId" is not null THEN 1 ELSE null END ) AS stakedEarned_d,
        1
    from
      minted m
      left outer join earned on (m."tokenId" = earned."tokenId")
      left outer join transferred on (m."tokenId" = transferred."tokenId")
      left outer join staked on (m."tokenId" = staked."tokenId")
      left outer join stakedEarned on (m."tokenId" = stakedEarned."tokenId")
  ),
  
  metrics as (
    select
      "boostType",
      count(1) as minted,
      count(earned) as earned,
       100 * count(earned) / count(1) as earned_percent,
      count(transferred) as transferred,
       100 - 100 * count(earned) / count(1) as transferred_percent,
      count(staked) as staked,
       100 * count(staked) / count(1) as staked_percent,
      count(1) - count(staked) as no_staked,
       100 - 100 * count(staked) / count(1) as no_staked_percent,
      count(stakedEarned) as staked_earned,
      100 * count(stakedEarned) / count(1) as staked_earned_percent,
      count(staked) - count(stakedEarned) as staked_no_earned,
      100 * ( count(staked) - count(stakedEarned) ) / count(1) as staked_no_earned_percent
      
    from
      rawMetrics
    group by
      "boostType"
    order by
      "boostType"
  )

select * from metrics
union
select ' TOTAL' as "boostType",
    sum(minted) as minted, 
    sum(earned) as earned,
    100 * sum(earned) / sum(minted) as earned_percent,
    sum(transferred) as transferred,
    100 - 100 * sum(earned) / sum(minted) as transferred_percent,
    sum(staked) as staked,
    100 * sum(staked) / sum(minted) as staked_percent,
    sum(no_staked) as no_staked,
    100 - 100 * sum(staked) / sum(minted) as no_staked_percent,
    sum(staked_earned) as staked_earned,
    100 * sum(staked_earned) / sum(minted) as staked_earned_percent,
    sum(staked_no_earned) as staked_no_earned,
    100 * ( sum(staked) - sum(staked_earned) ) / sum(minted) as staked_no_earned_percent
from metrics
order by "boostType"



  
  
