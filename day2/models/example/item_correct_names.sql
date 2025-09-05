WITH
items AS (
    SELECT
            *
    FROM
        {{ source('rakuten_ec', 'items') }}
),

ranked_names AS (    
        SELECT
            item_url,
            item_name,
            -- 各URL内で、IDが大きい順にランク付け
            ROW_NUMBER() OVER (PARTITION BY item_url ORDER BY item_id DESC) as name_rank
        FROM
            items
),

correct_name AS (
        SELECT
            item_url,
            item_name AS correct_name
        FROM
             ranked_names
        WHERE
            -- IDが新しいものに名前を合わせる
            name_rank = 1
),

final AS (
SELECT
        item_id,
        correct_name AS item_name,
        items.item_url,
        store_id,
        category_id
FROM
        items
        LEFT JOIN correct_name
        ON items.item_url = correct_name.item_url
)

SELECT *
FROM final