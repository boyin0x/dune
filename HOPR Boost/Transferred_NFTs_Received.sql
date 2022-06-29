-- Title: HoprBoost Address Which Have Received Transferred NFTs

WITH

traded as ( 
    select "to","from", "tokenId", "evt_tx_hash", "evt_block_time"  
    from hopr_protocol."HoprBoost_evt_Transfer"
    where "from" <> '\x0000000000000000000000000000000000000000'  -- exclude minted
    and "to" not in (
        '\x912F4d6607160256787a2AD40dA098Ac2aFE57AC' -- s 1
        ,'\x2cDD13ddB0346E0F620C8E5826Da5d7230341c6E' -- s 2
        ,'\xae933331ef0be122f9499512d3ed4fa3896dcf20' -- s 3
    )
)

select "to" as "address", count(distinct "tokenId") from traded group by "to" order by count(1) desc

