-- Title: HoprBoost Earned vs Transferred vs Mix

WITH

mintCall as (
    select "boostType" ,"call_tx_hash" from hopr_protocol."HoprBoost_call_mint" 
    where "call_success" = true
    union 
    select "boostType" ,"call_tx_hash" from hopr_protocol."HoprBoost_call_batchMint" 
    where "call_success" = true
)

,minted as ( 
    select "to", "tokenId", "evt_tx_hash", "evt_block_time", "boostType"  
    from hopr_protocol."HoprBoost_evt_Transfer" 
    inner join mintCall on "call_tx_hash" = "evt_tx_hash"
    where "from" = '\x0000000000000000000000000000000000000000' 
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
)


,earned as ( 
    select distinct "tokenId" from minted
    EXCEPT
    select distinct "tokenId" from traded
)

,transferred as ( 
    select distinct "tokenId" from traded  
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
)

,NFT_Holders as (
    select address, 
        count(earned."tokenId") earned,
        count(transferred."tokenId") transferred, 
        count(1) total 
    from holder
        left outer join earned on (holder."tokenId" = earned."tokenId")
        left outer join transferred on (holder."tokenId" = transferred."tokenId")
    group by address order by earned desc
)

select 'Earned', count(1) from NFT_Holders where earned > 0 and transferred = 0
union
select 'Transferred', count(1) from NFT_Holders where earned = 0 and transferred > 0
union
select 'Earned and Transferred', count(1) from NFT_Holders where earned > 0 and transferred > 0
