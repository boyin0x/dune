-- Title: HoprBoost Ownership of Different NFT Types

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

, holderBoostType as (
    select address, "boostType" , count(1)
    from holder
    inner join minted on (holder."tokenId" = minted."tokenId")
    group by address, "boostType"
)

,boostTypeBoostType as  (
    select h."boostType" as t1, h2."boostType" as t2
    from holderBoostType h inner join holderBoostType as h2 on (h.address = h2.address)
    where h."boostType" != h2."boostType"
)

select t1 as "typeA", t2 as "typeB" ,count(1) holders from boostTypeBoostType group by t1,t2 order by t1, count(1) desc
