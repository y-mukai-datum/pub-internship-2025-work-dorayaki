select *
from {{ ref('mart_daifukucho') }}
where category_level_1 is null
