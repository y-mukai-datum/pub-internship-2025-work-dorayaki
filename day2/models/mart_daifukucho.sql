{{ config(materialized='table') }}

with
-- 元テーブル
purchases       as (select * from {{ source('rakuten_ec_raw', 'PURCHASES') }}),
purchase_items  as (select * from {{ source('rakuten_ec_raw', 'PURCHASE_ITEMS') }}),
items           as (select * from {{ source('rakuten_ec_raw', 'ITEMS') }}),
categories      as (select * from {{ source('rakuten_ec_raw', 'CATEGORIES') }}),
users           as (select * from {{ source('rakuten_ec_raw', 'USERS') }}),
stores          as (select * from {{ source('rakuten_ec_raw', 'STORES') }}),
ec_sites        as (select * from {{ source('rakuten_ec_raw', 'EC_SITES') }}),

-- ★ seed: 都道府県 → 地方
locate_def as (
  select todofukenn, region_name
  from {{ ref('locate_definition') }}
),

-- ★ seed: 年齢範囲 → 年齢カテゴリ
age_def as (
  select
    cast(age_lower_limit as number) as age_lower_limit,
    cast(age_upper_limit as number) as age_upper_limit,
    gene as age_category
  from {{ ref('gene_definition') }}
),

-- カテゴリ階層（最大4段）
cat as (
  select
    c4.CATEGORY_ID    as category_id_lv4,
    c4.CATEGORY_NAME  as category_name_lv4,
    c4.CATEGORY_LEVEL as category_level_lv4,
    c3.CATEGORY_ID    as category_id_lv3,
    c3.CATEGORY_NAME  as category_name_lv3,
    c3.CATEGORY_LEVEL as category_level_lv3,
    c2.CATEGORY_ID    as category_id_lv2,
    c2.CATEGORY_NAME  as category_name_lv2,
    c2.CATEGORY_LEVEL as category_level_lv2,
    c1.CATEGORY_ID    as category_id_lv1,
    c1.CATEGORY_NAME  as category_name_lv1,
    c1.CATEGORY_LEVEL as category_level_lv1
  from categories c4
  left join categories c3 on c4.PARENT_CATEGORY_ID = c3.CATEGORY_ID
  left join categories c2 on c3.PARENT_CATEGORY_ID = c2.CATEGORY_ID
  left join categories c1 on c2.PARENT_CATEGORY_ID = c1.CATEGORY_ID
),

-- 商品にカテゴリ名（1～4階層）を付与
item_with_cats as (
  select
    i.ITEM_ID,
    i.ITEM_NAME,
    i.ITEM_URL,
    i.STORE_ID,
    i.CATEGORY_ID as cat_lv4_id,

    coalesce(
      iff(c.category_level_lv1 = 1, c.category_name_lv1, null),
      iff(c.category_level_lv2 = 1, c.category_name_lv2, null),
      iff(c.category_level_lv3 = 1, c.category_name_lv3, null),
      iff(c.category_level_lv4 = 1, c.category_name_lv4, null)
    ) as category_level_1,

    coalesce(
      iff(c.category_level_lv1 = 2, c.category_name_lv1, null),
      iff(c.category_level_lv2 = 2, c.category_name_lv2, null),
      iff(c.category_level_lv3 = 2, c.category_name_lv3, null),
      iff(c.category_level_lv4 = 2, c.category_name_lv4, null)
    ) as category_level_2,

    coalesce(
      iff(c.category_level_lv1 = 3, c.category_name_lv1, null),
      iff(c.category_level_lv2 = 3, c.category_name_lv2, null),
      iff(c.category_level_lv3 = 3, c.category_name_lv3, null),
      iff(c.category_level_lv4 = 3, c.category_name_lv4, null)
    ) as category_level_3,

    coalesce(
      iff(c.category_level_lv1 = 4, c.category_name_lv1, null),
      iff(c.category_level_lv2 = 4, c.category_name_lv2, null),
      iff(c.category_level_lv3 = 4, c.category_name_lv3, null),
      iff(c.category_level_lv4 = 4, c.category_name_lv4, null)
    ) as category_level_4

  from items i
  left join cat c on i.CATEGORY_ID = c.category_id_lv4
),

-- 商品名から割引フラグ（語彙は最小限＋α）
discount_items as (
  select
    i.ITEM_ID,
    case
      when regexp_like(
        lower(replace(i.ITEM_NAME, '　', ' ')),
        'セール|ｾｰﾙ|sale|off|オフ|ｵﾌ|割引|値下げ|タイムセール|半額'
      )
      then true else false
    end as discount_flag
  from items i
),

-- ベース
base as (
  select
    pi.PURCHASE_ITEM_ID   as id,
    p.PURCHASE_ID         as purchased_id,
    p.PURCHASED_AT        as purchased_at,
    u.USER_ID             as user_id_hash,
    u.GENDER_NAME         as gender_name,
    u.AGE                 as age,
    u.STATE_NAME          as state_name,
    u.MARRIAGE_STATUS     as marriage_status,
    u.PROFESSION_NAME     as profession_name,
    u.OCCUPATION_NAME     as occupation_name,
    s.STORE_NAME          as store_name,
    es.EC_SITE_NAME       as ec_site_name,
    iwc.ITEM_ID           as item_id,
    iwc.ITEM_NAME         as item_name_raw,

    -- 名寄せ用（出力には含めない）
    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              LOWER(REPLACE(iwc.ITEM_NAME, '　', ' ')),
              '【[^】]*】', ''
            ),
            '（[^）]*）', ''
          ),
          '\([^)]*\)', ''
        ),
        '[[:space:]]+', ' '
      )
    ) as item_name_canonical,

    iwc.ITEM_URL                                  as item_url,
    REGEXP_SUBSTR(iwc.ITEM_URL, '^[^?#]+')        as item_url_canonical,  -- ★追加: URL正規化
    TRY_TO_NUMBER(pi.UNIT_PRICE)                  as unit_price,          -- ★数値化
    TRY_TO_NUMBER(pi.AMOUNT)                      as amount,              -- ★数値化
    p.DESTINATION_POSTAL_CODE                    as destination_postal,
    iwc.category_level_1,
    iwc.category_level_2,
    iwc.category_level_3,
    iwc.category_level_4
  from purchase_items pi
  join purchases p on pi.PURCHASE_ID = p.PURCHASE_ID
  left join users u on p.USER_ID = u.USER_ID
  left join item_with_cats iwc on pi.ITEM_ID = iwc.ITEM_ID
  left join stores s on iwc.STORE_ID = s.STORE_ID
  left join ec_sites es on s.EC_SITE_ID = es.EC_SITE_ID
),

-- 付加項目
enriched as (
  select
    b.*,
    floor(try_to_number(b.age) / 10) * 10 as age_decade,
    (b.unit_price * b.amount)             as total_price,

    -- パーティション内の最大単価を列としてもつ
    MAX(b.unit_price) OVER (
      PARTITION BY COALESCE(b.item_url_canonical, b.item_name_canonical)
    ) as max_unit_price_same_item,

    -- 割引フラグ（最低条件「セール」含む + α）
    IFF(
      COALESCE(di.discount_flag, false)
      or b.item_name_raw ilike '%セール%'
      or b.item_name_raw ilike '%ｾｰﾙ%'
      or b.item_name_raw ilike '%sale%'
      or b.item_name_raw ilike '%off%'
      or b.item_name_raw ilike '%オフ%'
      or b.item_name_raw ilike '%ｵﾌ%'
      or b.item_name_raw ilike '%割引%'
      or b.item_name_raw ilike '%値下げ%'
      or b.item_name_raw ilike '%タイムセール%'
      or b.item_name_raw ilike '%半額%',
      true, false
    ) as is_discount,

    -- 同一商品内の最大単価との差×数量（下限0）
    GREATEST( (max_unit_price_same_item - b.unit_price) * b.amount, 0 ) as discount_amount,

    ld.region_name  as region_name,
    ad.age_category as age_category
  from base b
  left join discount_items di on b.item_id = di.ITEM_ID
  left join locate_def   ld on trim(b.state_name) = trim(ld.todofukenn)
  left join age_def      ad on try_to_number(b.age) between ad.age_lower_limit and ad.age_upper_limit
),


final as (
  select
    id,
    purchased_id,
    purchased_at,
    ec_site_name,
    unit_price,
    amount,
    total_price,
    user_id_hash,
    item_name_raw as item_name,
    item_url,
    destination_postal  as destination_postal_code,
    store_name,
    gender_name,
    age,
    age_decade,
    state_name,
    region_name,
    age_category,
    marriage_status,
    profession_name,
    occupation_name,
    category_level_1,
    category_level_2,
    category_level_3,
    category_level_4,
    is_discount,
    discount_amount
  from enriched
  where category_level_1 is not null
)

select * from final
