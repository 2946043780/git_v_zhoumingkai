-- 用户访问记录表
drop table if exists ods_user_visits;
CREATE TABLE ods_user_visits (
                                 visit_id STRING COMMENT '访问记录ID',
                                 user_id STRING COMMENT '用户ID',
                                 item_id STRING COMMENT '商品ID',
                                 visit_time TIMESTAMP COMMENT '访问时间',
                                 stay_duration INT COMMENT '停留时长（秒）',
                                 is_bounce BOOLEAN COMMENT '是否跳出'
)
    COMMENT '用户访问商品记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/user_visits/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_visits/2025-03-31" overwrite into table dv_dont2.ods_user_visits partition (ds = '2025-03-31');

-- 用户收藏记录表
drop table if exists ods_user_collections;
CREATE TABLE ods_user_collections (
                                      collection_id STRING COMMENT '收藏记录ID',
                                      user_id STRING COMMENT '用户ID',
                                      item_id STRING COMMENT '商品ID',
                                      collection_time TIMESTAMP COMMENT '收藏时间'
)
    COMMENT '用户收藏商品记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/user_collections/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_collections/2025-03-31" overwrite into table dv_dont2.ods_user_collections partition (ds = '2025-03-31');

-- 用户加购记录表
drop table if exists ods_user_add_to_cart;
CREATE TABLE ods_user_add_to_cart (
                                      cart_id STRING COMMENT '加购记录ID',
                                      user_id STRING COMMENT '用户ID',
                                      item_id STRING COMMENT '商品ID',
                                      item_count INT COMMENT '加购商品数量',
                                      add_time TIMESTAMP COMMENT '加购时间'
)
    COMMENT '用户将商品加入购物车记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/user_add_to_cart/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_add_to_cart/2025-03-31" overwrite into table dv_dont2.ods_user_add_to_cart partition (ds = '2025-03-31');

-- 用户下单记录表
CREATE TABLE ods_user_orders (
                                 order_id STRING COMMENT '订单ID',
                                 user_id STRING COMMENT '用户ID',
                                 item_id STRING COMMENT '商品ID',
                                 item_count INT COMMENT '下单商品数量',
                                 order_amount DECIMAL(10, 2) COMMENT '下单金额',
                                 order_time TIMESTAMP COMMENT '下单时间',
                                 is_new_buyer BOOLEAN COMMENT '是否为新买家'
)
    COMMENT '用户下单商品记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/user_orders/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_orders/2025-03-31" overwrite into table dv_dont2.ods_user_orders partition (ds = '2025-03-31');

-- 用户支付记录表
drop table if exists ods_user_payments;
CREATE TABLE ods_user_payments (
                                   payment_id STRING COMMENT '支付记录ID',
                                   order_id STRING COMMENT '关联订单ID',
                                   user_id STRING COMMENT '用户ID',
                                   item_id STRING COMMENT '商品ID',
                                   payment_amount DECIMAL(10, 2) COMMENT '支付金额',
                                   payment_time TIMESTAMP COMMENT '支付时间',
                                   is_refunded BOOLEAN COMMENT '是否退款',
                                   refund_amount DECIMAL(10, 2) COMMENT '退款金额'
)
    COMMENT '用户支付商品记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/user_payments/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_payments/2025-03-30" overwrite into table dv_dont2.ods_user_payments partition (ds = '2025-03-30');

-- 创建商品微详情访客表
CREATE TABLE ods_item_micro_detail_visits (
                                              micro_visit_id STRING COMMENT '微详情访问记录ID',
                                              user_id STRING COMMENT '用户ID',
                                              item_id STRING COMMENT '商品ID',
                                              micro_visit_time TIMESTAMP COMMENT '微详情访问时间'
)
    COMMENT '商品微详情访客记录'
PARTITIONED BY ( `ds` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/item_micro_detail_visits/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/item_micro_detail_visits/2025-03-30" overwrite into table dv_dont2.ods_item_micro_detail_visits partition (ds = '2025-03-30');
------------------------------------------------           指标               ----------------------------------------------------------------

-- 按天统计每个商品的商品访客数
SELECT
    item_id,
    ds,
    COUNT(DISTINCT user_id) AS item_visitor_count
FROM
    dv_dont2.ods_user_visits
GROUP BY
    item_id, ds;

-- 按 7 天统计每个商品的商品访客数
SELECT
    item_id,
    -- 使用date_sub函数来计算7天周期，这里以当前日期往前推7天为例
    COUNT(DISTINCT user_id) AS item_visitor_count
FROM
    dv_dont2.ods_user_visits
WHERE
        visit_time >= date_sub(current_date, 7)
  AND visit_time < current_date
GROUP BY
    item_id;

-- 按天求出的跳出率
SELECT
    item_id,
    ds,
    -- 计算跳出率，1 - 点击详情页人数/详情页访客数
    1 - CAST(SUM(CASE WHEN is_bounce = false THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(DISTINCT user_id) AS item_detail_page_bounce_rate
FROM
    dv_dont2.ods_user_visits
GROUP BY
    item_id, ds;

-- 按天求出访问收藏转化率
SELECT
    uv.item_id,
    uv.ds,
    -- 计算访问收藏转化率，收藏人数/访客数
    COUNT(DISTINCT uc.user_id) / COUNT(DISTINCT uv.user_id) AS visit_collection_conversion_rate
FROM
    dv_dont2.ods_user_visits uv
        LEFT JOIN
    dv_dont2.ods_user_collections uc ON uv.user_id = uc.user_id AND uv.item_id = uc.item_id AND uv.ds = uc.ds
GROUP BY
    uv.item_id, uv.ds;


-- 基础指标统计
-- 按天统计每个商品的各项指标
-- 按天统计每个商品的各项指标（包含商品微详情访客数）
SELECT
    uv.item_id,
    uv.ds,
    -- 商品访客数
    COUNT(DISTINCT uv.user_id) AS item_visitor_count,
    -- 商品浏览量
    COUNT(uv.visit_id) AS item_view_count,
    -- 有访问商品数（这里假设只要有访问记录就算有访问，按商品ID计数）
    COUNT(DISTINCT uv.item_id) AS item_count_with_visit,
    -- 商品平均停留时长
    AVG(uv.stay_duration) AS avg_stay_duration,
    -- 商品详情页跳出率
    1 - CAST(SUM(CASE WHEN uv.is_bounce = false THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(DISTINCT uv.user_id) AS item_detail_page_bounce_rate,
    -- 商品收藏人数
    COUNT(DISTINCT uc.user_id) AS item_collection_count,
    -- 商品加购件数
    SUM(uatc.item_count) AS item_add_to_cart_count,
    -- 商品加购人数
    COUNT(DISTINCT uatc.user_id) AS item_add_to_cart_person_count,
    -- 访问收藏转化率
    COUNT(DISTINCT uc.user_id) / COUNT(DISTINCT uv.user_id) AS visit_collection_conversion_rate,
    -- 访问加购转化率
    COUNT(DISTINCT uatc.user_id) / COUNT(DISTINCT uv.user_id) AS visit_add_to_cart_conversion_rate,
    -- 下单买家数
    COUNT(DISTINCT uo.user_id) AS order_buyer_count,
    -- 下单件数
    SUM(uo.item_count) AS order_item_count,
    -- 下单金额
    SUM(uo.order_amount) AS order_amount,
    -- 下单转化率（假设下单转化率 = 下单买家数 / 商品访客数）
    COUNT(DISTINCT uo.user_id) / COUNT(DISTINCT uv.user_id) AS order_conversion_rate,
    -- 支付买家数
    COUNT(DISTINCT up.user_id) AS payment_buyer_count,
    -- 支付件数（这里假设一个支付记录对应一件商品支付，若实际情况不同需调整）
    COUNT(up.payment_id) AS payment_item_count,
    -- 支付金额
    SUM(up.payment_amount) AS payment_amount,
    -- 有支付商品数（按商品ID计数）
    COUNT(DISTINCT up.item_id) AS item_count_with_payment,
    -- 支付转化率（支付买家数 / 商品访客数）
    COUNT(DISTINCT up.user_id) / COUNT(DISTINCT uv.user_id) AS payment_conversion_rate,
    -- 支付新买家数
    SUM(CASE WHEN uo.is_new_buyer THEN 1 ELSE 0 END) AS payment_new_buyer_count,
    -- 支付老买家数
    COUNT(DISTINCT up.user_id) - SUM(CASE WHEN uo.is_new_buyer THEN 1 ELSE 0 END) AS payment_old_buyer_count,
    -- 老买家支付金额（假设先通过下单表判断新老买家，再关联到支付表计算金额）
    SUM(CASE WHEN NOT uo.is_new_buyer THEN up.payment_amount ELSE 0 END) AS old_buyer_payment_amount,
    -- 客单价（支付金额 / 支付件数，需注意分母为0的情况，这里简单处理）
    CASE WHEN COUNT(up.payment_id) = 0 THEN 0
         ELSE SUM(up.payment_amount) / COUNT(up.payment_id)
        END AS unit_price,
    -- 成功退款退货金额
    SUM(CASE WHEN up.is_refunded THEN up.refund_amount ELSE 0 END) AS successful_refund_amount,
    -- 年累计支付金额（这里假设没有年累计相关逻辑，暂无法准确计算，可根据实际需求补充时间范围条件）
    SUM(up.payment_amount) AS annual_accumulated_payment_amount,
    -- 访客平均价值（支付金额 / 商品访客数）
    CASE WHEN COUNT(DISTINCT uv.user_id) = 0 THEN 0
         ELSE SUM(up.payment_amount) / COUNT(DISTINCT uv.user_id)
        END AS visitor_average_value,
    -- 竞争力评分（这里没有相关计算逻辑，暂设为NULL，需根据实际业务补充）
    NULL AS competitive_score,
    -- 商品微详情访客数
    COUNT(DISTINCT oimdv.user_id) AS micro_detail_visitor_count
FROM
    dv_dont2.ods_user_visits uv
        LEFT JOIN
    dv_dont2.ods_user_collections uc ON uv.user_id = uc.user_id AND uv.item_id = uc.item_id AND uv.ds = uc.ds
        LEFT JOIN
    dv_dont2.ods_user_add_to_cart uatc ON uv.user_id = uatc.user_id AND uv.item_id = uatc.item_id AND uv.ds = uatc.ds
        LEFT JOIN
    dv_dont2.ods_user_orders uo ON uv.user_id = uo.user_id AND uv.item_id = uo.item_id AND uv.ds = uo.ds
        LEFT JOIN
    dv_dont2.ods_user_payments up ON uo.order_id = up.order_id AND uv.user_id = up.user_id AND uv.item_id = up.item_id AND uv.ds = up.ds
        LEFT JOIN
    dv_dont2.ods_item_micro_detail_visits oimdv ON uv.user_id = oimdv.user_id AND uv.item_id = oimdv.item_id AND uv.ds = oimdv.ds
GROUP BY
    uv.item_id, uv.ds;


CREATE TABLE IF NOT EXISTS ads_item_metrics (
                                                item_id STRING,
                                                ds STRING,
                                                item_visitor_count BIGINT,
                                                item_view_count BIGINT,
                                                item_count_with_visit BIGINT,
                                                avg_stay_duration DOUBLE,
                                                item_detail_page_bounce_rate DOUBLE,
                                                item_collection_count BIGINT,
                                                item_add_to_cart_count BIGINT,
                                                item_add_to_cart_person_count BIGINT,
                                                visit_collection_conversion_rate DOUBLE,
                                                visit_add_to_cart_conversion_rate DOUBLE,
                                                order_buyer_count BIGINT,
                                                order_item_count BIGINT,
                                                order_amount DOUBLE,
                                                order_conversion_rate DOUBLE,
                                                payment_buyer_count BIGINT,
                                                payment_item_count BIGINT,
                                                payment_amount DOUBLE,
                                                item_count_with_payment BIGINT,
                                                payment_conversion_rate DOUBLE,
                                                payment_new_buyer_count BIGINT,
                                                payment_old_buyer_count BIGINT,
                                                old_buyer_payment_amount DOUBLE,
                                                unit_price DOUBLE,
                                                successful_refund_amount DOUBLE,
                                                annual_accumulated_payment_amount DOUBLE,
                                                visitor_average_value DOUBLE,
                                                competitive_score DOUBLE,
                                                micro_detail_visitor_count BIGINT
)
    row format delimited  fields terminated by '\t'
    location '/2207A/mingkai_Zhou/warehouse/gmall/ads/item_metrics/'
    tblproperties ("parquet.compression"="snappy");

INSERT INTO TABLE ads_item_metrics
SELECT
    uv.item_id,
    uv.ds,
    -- 商品访客数
    COUNT(DISTINCT uv.user_id) AS item_visitor_count,
    -- 商品浏览量
    COUNT(uv.visit_id) AS item_view_count,
    -- 有访问商品数（这里假设只要有访问记录就算有访问，按商品ID计数）
    COUNT(DISTINCT uv.item_id) AS item_count_with_visit,
    -- 商品平均停留时长
    AVG(uv.stay_duration) AS avg_stay_duration,
    -- 商品详情页跳出率
    1 - CAST(SUM(CASE WHEN uv.is_bounce = false THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(DISTINCT uv.user_id) AS item_detail_page_bounce_rate,
    -- 商品收藏人数
    COUNT(DISTINCT uc.user_id) AS item_collection_count,
    -- 商品加购件数
    SUM(uatc.item_count) AS item_add_to_cart_count,
    -- 商品加购人数
    COUNT(DISTINCT uatc.user_id) AS item_add_to_cart_person_count,
    -- 访问收藏转化率
    COUNT(DISTINCT uc.user_id) / COUNT(DISTINCT uv.user_id) AS visit_collection_conversion_rate,
    -- 访问加购转化率
    COUNT(DISTINCT uatc.user_id) / COUNT(DISTINCT uv.user_id) AS visit_add_to_cart_conversion_rate,
    -- 下单买家数
    COUNT(DISTINCT uo.user_id) AS order_buyer_count,
    -- 下单件数
    SUM(uo.item_count) AS order_item_count,
    -- 下单金额
    SUM(uo.order_amount) AS order_amount,
    -- 下单转化率（假设下单转化率 = 下单买家数 / 商品访客数）
    COUNT(DISTINCT uo.user_id) / COUNT(DISTINCT uv.user_id) AS order_conversion_rate,
    -- 支付买家数
    COUNT(DISTINCT up.user_id) AS payment_buyer_count,
    -- 支付件数（这里假设一个支付记录对应一件商品支付，若实际情况不同需调整）
    COUNT(up.payment_id) AS payment_item_count,
    -- 支付金额
    SUM(up.payment_amount) AS payment_amount,
    -- 有支付商品数（按商品ID计数）
    COUNT(DISTINCT up.item_id) AS item_count_with_payment,
    -- 支付转化率（支付买家数 / 商品访客数）
    COUNT(DISTINCT up.user_id) / COUNT(DISTINCT uv.user_id) AS payment_conversion_rate,
    -- 支付新买家数
    SUM(CASE WHEN uo.is_new_buyer THEN 1 ELSE 0 END) AS payment_new_buyer_count,
    -- 支付老买家数
    COUNT(DISTINCT up.user_id) - SUM(CASE WHEN uo.is_new_buyer THEN 1 ELSE 0 END) AS payment_old_buyer_count,
    -- 老买家支付金额（假设先通过下单表判断新老买家，再关联到支付表计算金额）
    SUM(CASE WHEN NOT uo.is_new_buyer THEN up.payment_amount ELSE 0 END) AS old_buyer_payment_amount,
    -- 客单价（支付金额 / 支付件数，需注意分母为0的情况，这里简单处理）
    CASE WHEN COUNT(up.payment_id) = 0 THEN 0
         ELSE SUM(up.payment_amount) / COUNT(up.payment_id)
        END AS unit_price,
    -- 成功退款退货金额
    SUM(CASE WHEN up.is_refunded THEN up.refund_amount ELSE 0 END) AS successful_refund_amount,
    -- 年累计支付金额（这里假设没有年累计相关逻辑，暂无法准确计算，可根据实际需求补充时间范围条件）
    SUM(up.payment_amount) AS annual_accumulated_payment_amount,
    -- 访客平均价值（支付金额 / 商品访客数）
    CASE WHEN COUNT(DISTINCT uv.user_id) = 0 THEN 0
         ELSE SUM(up.payment_amount) / COUNT(DISTINCT uv.user_id)
        END AS visitor_average_value,
    -- 竞争力评分（这里没有相关计算逻辑，暂设为NULL，需根据实际业务补充）
    NULL AS competitive_score,
    -- 商品微详情访客数
    COUNT(DISTINCT oimdv.user_id) AS micro_detail_visitor_count
FROM
    dv_dont2.ods_user_visits uv
        LEFT JOIN
    dv_dont2.ods_user_collections uc ON uv.user_id = uc.user_id AND uv.item_id = uc.item_id AND uv.ds = uc.ds
        LEFT JOIN
    dv_dont2.ods_user_add_to_cart uatc ON uv.user_id = uatc.user_id AND uv.item_id = uatc.item_id AND uv.ds = uatc.ds
        LEFT JOIN
    dv_dont2.ods_user_orders uo ON uv.user_id = uo.user_id AND uv.item_id = uo.item_id AND uv.ds = uo.ds
        LEFT JOIN
    dv_dont2.ods_user_payments up ON uo.order_id = up.order_id AND uv.user_id = up.user_id AND uv.item_id = up.item_id AND uv.ds = up.ds
        LEFT JOIN
    dv_dont2.ods_item_micro_detail_visits oimdv ON uv.user_id = oimdv.user_id AND uv.item_id = oimdv.item_id AND uv.ds = oimdv.ds
GROUP BY
    uv.item_id, uv.ds;


--统计日期内收藏的商品人数

-- 统计指定日期内商品收藏人数
SELECT
    item_id,
    COUNT(DISTINCT user_id) AS collection_user_count
FROM
    ods_user_collections
WHERE
        ds = '2025-03-31'  -- 统计日期
GROUP BY
    item_id;

-- 创建用于存储商品收藏人数统计结果的表
CREATE TABLE ads_item_collection_stats (
                                           item_id STRING COMMENT '商品ID',
                                           collection_user_count BIGINT COMMENT '收藏商品的去重人数'
)
    COMMENT '商品收藏人数统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE ads_item_collection_stats
SELECT
    item_id,
    COUNT(DISTINCT user_id) AS collection_user_count
FROM
    ods_user_collections
WHERE
        ds = '2025-03-31'  -- 统计日期
GROUP BY
    item_id;

-- 统计所有终端支付买家数
SELECT COUNT(DISTINCT user_id) AS all_end_payment_buyer_count
FROM ods_user_payments
WHERE ds = '2025-03-30'; -- 根据实际统计日期调整

-- 创建用于存储所有终端支付买家数统计结果的表
CREATE TABLE ads_all_end_payment_buyer_stats (
    all_end_payment_buyer_count BIGINT COMMENT '所有终端支付买家数'
)
    COMMENT '所有终端支付买家数统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE ads_all_end_payment_buyer_stats
SELECT COUNT(DISTINCT user_id) AS all_end_payment_buyer_count
FROM ods_user_payments
WHERE ds = '2025-03-30';


-- 创建表用于存储商品平均停留时长统计结果
CREATE TABLE ads_avg_stay_duration_stats (
    overall_avg_stay_duration DOUBLE COMMENT '商品平均停留时长（秒）'
)
    COMMENT '商品平均停留时长统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- 插入多天的商品平均停留时长统计结果
INSERT INTO TABLE ads_avg_stay_duration_stats
SELECT
    AVG(avg_stay_duration) AS overall_avg_stay_duration
FROM (
         SELECT
             ds,
             SUM(stay_duration) / COUNT(DISTINCT user_id) AS avg_stay_duration
         FROM
             ods_user_visits
         GROUP BY
             ds
     ) subquery;


-- 创建表用于存储商品浏览量统计结果
CREATE TABLE ads_item_view_count_stats (
                                           item_id STRING COMMENT '商品ID',
                                           item_view_count BIGINT COMMENT '商品浏览量'
)
    COMMENT '商品浏览量统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE ads_item_view_count_stats
SELECT
    item_id,
    COUNT(1) AS item_view_count
FROM
    ods_user_visits
GROUP BY
    item_id;



-- 创建用于存储商品访客数统计结果的表
CREATE TABLE dw_item_visitor_stats (
                                       all_end_item_visitor_count BIGINT COMMENT '所有终端商品访客数',
                                       pc_item_visitor_count BIGINT COMMENT 'PC端商品访客数',
                                       wireless_item_visitor_count BIGINT COMMENT '无线端商品访客数'
)
    COMMENT '商品访客数统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- 插入所有终端商品访客数统计结果
INSERT INTO TABLE dw_item_visitor_stats (all_end_item_visitor_count)
SELECT COUNT(DISTINCT user_id)
FROM ods_user_visits
WHERE ds = '2025-03-31';

-- 插入PC端商品访客数统计结果
INSERT INTO TABLE dw_item_visitor_stats (pc_item_visitor_count)
SELECT COUNT(DISTINCT user_id)
FROM ods_user_visits
WHERE device_type = 'PC'
  AND ds = '2025-03-31';

-- 插入无线端商品访客数统计结果
INSERT INTO TABLE dw_item_visitor_stats (wireless_item_visitor_count)
SELECT COUNT(DISTINCT user_id)
FROM ods_user_visits
WHERE device_type IN ('Mobile', 'Pad')
  AND ds = '2025-03-31';


-- 创建用于存储按访客分组的访客平均价值统计结果的表
CREATE TABLE dw_visitor_avg_value_group_stats (
                                                  user_id STRING COMMENT '访客ID',
                                                  visitor_avg_value DECIMAL(10, 2) COMMENT '访客平均价值'
)
    COMMENT '按访客分组的访客平均价值统计结果表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


-- 将按访客分组的访客平均价值统计结果插入表中
INSERT INTO TABLE dw_visitor_avg_value_group_stats
SELECT
    ou.user_id,
    SUM(op.payment_amount) / COUNT(1) AS visitor_avg_value
FROM
    ods_user_orders ou
        JOIN
    ods_user_payments op ON ou.order_id = op.order_id
WHERE
        op.ds = '2025-03-30'
GROUP BY
    ou.user_id;


----------   下单件数   -----------
-- 创建存储加购件数指标的表
CREATE TABLE ads_add_to_cart_item_count (
                                            stat_date STRING COMMENT '统计日期',
                                            total_add_to_cart_item_count INT COMMENT '加购商品总件数'
)
    COMMENT '每日加购商品件数统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_add_to_cart_item_count/'
tblproperties ("parquet.compression"="snappy");
--- 添加数据
INSERT INTO TABLE ads_add_to_cart_item_count
SELECT
    '2025-03-30' AS stat_date,
    SUM(item_count) AS total_add_to_cart_item_count
FROM
    dv_dont2.ods_user_add_to_cart
WHERE
        ds = '2025-03-30';

------------   下单金额   -----------------
CREATE TABLE ads_order_amount_stat (
                                       stat_date STRING COMMENT '统计日期',
                                       total_order_amount DECIMAL(10, 2) COMMENT '下单商品累计金额'
)
    COMMENT '每日下单商品金额统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_order_amount_stat/'
tblproperties ("parquet.compression"="snappy");
------  添加数据  ------
INSERT INTO TABLE ads_order_amount_stat
SELECT
    '2025-03-31' AS stat_date,
    SUM(order_amount) AS total_order_amount
FROM
    dv_dont2.ods_user_orders
WHERE
        ds = '2025-03-31';

----------   下单转化率   --------------
CREATE TABLE ads_order_conversion_rate (
                                           stat_date STRING COMMENT '统计日期',
                                           order_conversion_rate DECIMAL(10, 4) COMMENT '下单转化率'
)
    COMMENT '每日下单转化率统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_order_conversion_rate/'
tblproperties ("parquet.compression"="snappy");
-------  添加数据  -------------
INSERT INTO TABLE ads_order_conversion_rate
SELECT
    '2025-03-31' AS stat_date,
    -- 使用distinct统计下单买家数和访客数
    CASE
        WHEN COUNT(DISTINCT ov.user_id) = 0 THEN 0
        ELSE COUNT(DISTINCT o.user_id) / COUNT(DISTINCT ov.user_id)
        END AS order_conversion_rate
FROM
    dv_dont2.ods_user_orders o
        JOIN
    dv_dont2.ods_user_visits ov ON o.user_id = ov.user_id AND o.ds = ov.ds
WHERE
        o.ds = '2025-03-31' AND ov.ds = '2025-03-31';


------------   支付件数   --------------------
drop table ads_payment_item_count;
CREATE TABLE ads_payment_item_count (
                                        stat_date STRING COMMENT '统计日期',
                                        total_payment_item_count INT COMMENT '支付商品总件数'
)
    COMMENT '每日支付商品件数统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_payment_item_count/'
tblproperties ("parquet.compression"="snappy");
----------  添加数据  ---------------
INSERT INTO TABLE ads_payment_item_count
SELECT
    '2025-03-31' AS stat_date,
    SUM(oi.item_count) AS total_payment_item_count
FROM
    dv_dont2.ods_user_orders oi
        JOIN
    dv_dont2.ods_user_payments op ON oi.order_id = op.order_id
WHERE
        oi.ds = '2025-03-31' AND op.ds = '2025-03-31';

------------   支付金额    ----------------
CREATE TABLE ads_payment_amount (
                                    stat_date STRING COMMENT '统计日期',
                                    total_payment_amount DECIMAL(10, 2) COMMENT '支付总金额'
)
    COMMENT '每日支付金额统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_payment_amount/'
tblproperties ("parquet.compression"="snappy");
--------   添加语句   -------------
INSERT INTO TABLE ads_payment_amount
SELECT
    '2025-03-30' AS stat_date,
    SUM(payment_amount) AS total_payment_amount
FROM
    dv_dont2.ods_user_payments
WHERE
        ds = '2025-03-30';

--------  有支付商品数  -------------
CREATE TABLE ads_paid_item_count (
                                     stat_date STRING COMMENT '统计日期',
                                     total_paid_item_count INT COMMENT '有支付的商品数累加'
)
    COMMENT '每日有支付商品数统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_paid_item_count/'
tblproperties ("parquet.compression"="snappy");
-------  添加数据  ---------
INSERT INTO TABLE ads_paid_item_count
SELECT
    '2025-03-30' AS stat_date,
    SUM(oi.item_count) AS total_paid_item_count
FROM
    dv_dont2.ods_user_orders oi
        JOIN
    dv_dont2.ods_user_payments op ON oi.order_id = op.order_id
WHERE
        oi.ds = '2025-03-31' AND op.ds = '2025-03-30';



----------   支付转化率   -----------
-- 创建存储支付转化率指标的表
CREATE TABLE ads_payment_conversion_rate (
                                             stat_date STRING COMMENT '统计日期',
                                             payment_conversion_rate DECIMAL(10, 4) COMMENT '支付转化率'
)
    COMMENT '每日支付转化率统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_payment_conversion_rate/'
tblproperties ("parquet.compression"="snappy");

-----  添加语句  -----
INSERT INTO TABLE ads_payment_conversion_rate
SELECT
    '2025-03-31' AS stat_date,
    CASE
        WHEN COUNT(DISTINCT ov.user_id) = 0 THEN 0
        ELSE COUNT(DISTINCT p.user_id) / COUNT(DISTINCT ov.user_id)
        END AS payment_conversion_rate
FROM
    dv_dont2.ods_user_visits ov
        LEFT JOIN
    dv_dont2.ods_user_payments p ON ov.user_id = p.user_id
WHERE
        ov.ds = '2025-03-31' AND (p.ds = '2025-03-30' OR p.ds IS NULL);


-------------  支付新买家数  -----------------
CREATE TABLE ads_payment_new_buyer_count (
                                             stat_date STRING COMMENT '统计日期',
                                             new_buyer_count INT COMMENT '支付新买家数'
)
    COMMENT '每日支付新买家数统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_payment_new_buyer_count/'
tblproperties ("parquet.compression"="snappy");

----------  添加数据  -----------
INSERT INTO TABLE ads_payment_new_buyer_count
SELECT
    '2025-03-31' AS stat_date,
    COUNT(DISTINCT p.user_id) AS new_buyer_count
FROM
    dv_dont2.ods_user_payments p
-- 子查询找出在2025-03-31前365天内无支付行为的用户ID集合
        LEFT JOIN (
        SELECT
            user_id
        FROM
            dv_dont2.ods_user_payments
        WHERE
                payment_time >= '2024-03-31' AND payment_time < '2025-03-31'
        GROUP BY
            user_id
    ) AS pre_pay_users ON p.user_id = pre_pay_users.user_id
WHERE
        p.ds = '2025-03-30'
  AND pre_pay_users.user_id IS NULL;


--------------  支付老买家数   ---------------------
CREATE TABLE ads_payment_old_buyer_count (
                                             stat_date STRING COMMENT '统计日期',
                                             old_buyer_count INT COMMENT '支付老买家数'
)
    COMMENT '每日支付老买家数统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_payment_old_buyer_count/'
tblproperties ("parquet.compression"="snappy");
-------  添加数据  -----------
INSERT INTO TABLE ads_payment_old_buyer_count
SELECT
    '2025-03-31' AS stat_date,
    COUNT(DISTINCT p.user_id) AS old_buyer_count
FROM
    dv_dont2.ods_user_payments p
-- 子查询找出在2024-03-31到2025-03-31之间有过支付行为的用户ID集合
        JOIN (
        SELECT
            user_id
        FROM
            dv_dont2.ods_user_payments
        WHERE
                payment_time >= '2024-03-31' AND payment_time < '2025-03-31'
        GROUP BY
            user_id
    ) AS pre_pay_users ON p.user_id = pre_pay_users.user_id
WHERE
        p.ds = '2025-03-30';



----------  老买家的支付金额   ---------------
CREATE TABLE ads_old_buyer_payment_amount (
                                              stat_date STRING COMMENT '统计日期',
                                              total_payment_amount DECIMAL(10, 2) COMMENT '老买家累计支付金额'
)
    COMMENT '每日老买家支付金额统计'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_old_buyer_payment_amount/'
tblproperties ("parquet.compression"="snappy");



----------   添加数据    --------------
INSERT INTO TABLE ads_old_buyer_payment_amount
SELECT
    '2025-03-31' AS stat_date,
    SUM(p.payment_amount) AS total_payment_amount
FROM
    dv_dont2.ods_user_payments p
-- 子查询找出在2024-03-31到2025-03-31之间有过支付行为的用户ID集合
        JOIN (
        SELECT
            user_id
        FROM
            dv_dont2.ods_user_payments
        WHERE
                payment_time >= '2024-03-31' AND payment_time < '2025-03-31'
        GROUP BY
            user_id
    ) AS pre_pay_users ON p.user_id = pre_pay_users.user_id
WHERE
        p.ds = '2025-03-30';

