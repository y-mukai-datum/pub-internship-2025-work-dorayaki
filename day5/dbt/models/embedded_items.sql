with items as (
    select
        *
    from
        {{ source('rakuten_ec', 'item') }}
),

chunked_captions as (
    select
        item_id,
        text_chunker(item_caption) as item_caption
    from
        items
),

flatten_chunked_captions as (
    select
        item_id,
        value
    from
        chunked_captions,
        lateral flatten (input => item_caption)
),

embedded_captions as (
    select
        item_id,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', value) as embedded_item_caption
    from
        flatten_chunked_captions
)

select * from embedded_captions