-- Title: HoprBoost Address Which Have Received Transferred NFTs

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

select "to" as "address", count(distinct "tokenId") from traded group by "to" order by count(1) desc

