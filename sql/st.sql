-------1.用户访问记录表
CREATE TABLE ods_user_visits (
                                 visit_id STRING COMMENT '访问记录ID',
                                 user_id STRING COMMENT '用户ID',
                                 item_id STRING COMMENT '商品ID',
                                 visit_time TIMESTAMP COMMENT '访问时间',
                                 stay_duration INT COMMENT '停留时长（秒）',
                                 is_bounce BOOLEAN COMMENT '是否跳出',
                                 device_type STRING COMMENT '设备类型，如PC、手机、Pad等'
)
    COMMENT '用户访问商品记录'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_visits/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/ods_user_visits/2025-04-02" overwrite into table dv_dont2.ods_user_visits partition (dt = '2025-04-02');





---------2.用户收藏记录表
CREATE TABLE ods_user_collections (
                                      collection_id STRING COMMENT '收藏记录ID',
                                      user_id STRING COMMENT '用户ID',
                                      item_id STRING COMMENT '商品ID',
                                      collection_time TIMESTAMP COMMENT '收藏时间'
)
    COMMENT '用户收藏商品记录'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_collections/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/ods_user_collections/2025-04-02" overwrite into table dv_dont2.ods_user_collections partition (dt = '2025-04-02');


----------3.用户加购记录表
CREATE TABLE ods_user_add_to_cart (
                                      cart_id STRING COMMENT '加购记录ID',
                                      user_id STRING COMMENT '用户ID',
                                      item_id STRING COMMENT '商品ID',
                                      item_count INT COMMENT '加购商品数量',
                                      add_time TIMESTAMP COMMENT '加购时间'
)
    COMMENT '用户将商品加入购物车记录'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_add_to_cart/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/ods_user_add_to_cart/2025-04-02" overwrite into table dv_dont2.ods_user_add_to_cart partition (dt = '2025-04-02');


------------4.用户下单记录表
CREATE TABLE ods_user_orders (
                                 order_id STRING COMMENT '订单ID',
                                 user_id STRING COMMENT '用户ID',
                                 item_id STRING COMMENT '商品ID',
                                 item_count INT COMMENT '下单商品数量',
                                 order_amount DECIMAL(10, 2) COMMENT '下单金额',
                                 order_time TIMESTAMP COMMENT '下单时间',
                                 is_new_buyer BOOLEAN COMMENT '是否为新买家',
                                 device_type STRING COMMENT '下单设备类型'
)
    COMMENT '用户下单商品记录'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_orders/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/ods_user_orders/2025-04-02" overwrite into table dv_dont2.ods_user_orders partition (dt = '2025-04-02');




-------------5.用户支付记录表
CREATE TABLE ods_user_payments (
                                   payment_id STRING COMMENT '支付记录ID',
                                   order_id STRING COMMENT '关联订单ID',
                                   user_id STRING COMMENT '用户ID',
                                   item_id STRING COMMENT '商品ID',
                                   payment_amount DECIMAL(10, 2) COMMENT '支付金额',
                                   payment_time TIMESTAMP COMMENT '支付时间',
                                   is_refunded BOOLEAN COMMENT '是否退款',
                                   refund_amount DECIMAL(10, 2) COMMENT '退款金额',
                                   device_type STRING COMMENT '支付设备类型'
)
    COMMENT '用户支付商品记录'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_payments/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/ods_user_payments/2025-04-02" overwrite into table dv_dont2.ods_user_payments partition (dt = '2025-04-02');






-------------6.商品记录表
CREATE TABLE dim_item_info (
                               item_id STRING COMMENT '商品ID',
                               item_price DECIMAL(10, 2) COMMENT '商品价格',
                               category_id STRING COMMENT '商品类目ID',
                               leaf_category_id STRING COMMENT '商品叶子类目ID',
                               item_name STRING COMMENT '商品名称'
)
    COMMENT '商品基本信息表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/dim_item_info/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/dim_item_info/2025-04-02" overwrite into table dv_dont2.dim_item_info partition (dt = '2025-04-02');






----------------7.商品指标汇总表
-- 创建大表
drop table integrated_metrics_table;
CREATE TABLE integrated_metrics_table (
                                          item_id STRING COMMENT '商品ID',
                                          time_period STRING COMMENT '时间周期，取值为day、7d、30d',
    -- 访问指标
                                          item_visitor_count BIGINT COMMENT '商品访客数',
                                          item_view_count BIGINT COMMENT '商品浏览量',
                                          item_count_with_visit BIGINT COMMENT '有访问商品数',
                                          avg_stay_duration DOUBLE COMMENT '商品平均停留时长',
                                          item_detail_page_bounce_rate DOUBLE COMMENT '商品详情页跳出率',
                                          micro_detail_visitor_count BIGINT COMMENT '商品微详情访客数',
    -- 收藏指标
                                          item_collection_count BIGINT COMMENT '商品收藏人数',
    -- 加购指标
                                          item_add_to_cart_count BIGINT COMMENT '商品加购件数',
                                          item_add_to_cart_person_count BIGINT COMMENT '商品加购人数',
    -- 销售指标
                                          order_buyer_count BIGINT COMMENT '下单买家数',
                                          order_item_count BIGINT COMMENT '下单件数',
                                          order_amount DOUBLE COMMENT '下单金额',
                                          order_conversion_rate DOUBLE COMMENT '下单转化率',
                                          payment_buyer_count BIGINT COMMENT '支付买家数',
                                          payment_item_count BIGINT COMMENT '支付件数',
                                          payment_amount DOUBLE COMMENT '支付金额',
                                          item_count_with_payment BIGINT COMMENT '有支付商品数',
                                          payment_conversion_rate DOUBLE COMMENT '支付转化率',
                                          payment_new_buyer_count BIGINT COMMENT '支付新买家数',
                                          payment_old_buyer_count BIGINT COMMENT '支付老买家数',
                                          old_buyer_payment_amount DOUBLE COMMENT '老买家支付金额',
                                          unit_price DOUBLE COMMENT '客单价',
                                          successful_refund_amount DOUBLE COMMENT '成功退款退货金额',
                                          annual_accumulated_payment_amount DOUBLE COMMENT '年累计支付金额',
                                          visitor_average_value DOUBLE COMMENT '访客平均价值',
                                          competitive_score DOUBLE COMMENT '竞争力评分',
    -- 商品区间分析指标
                                          price_band_100_500_count BIGINT COMMENT '价格带100 - 500商品数',
                                          price_band_501_1000_count BIGINT COMMENT '价格带501 - 1000商品数',
                                          price_band_1000_5000_count BIGINT COMMENT '价格带1000 - 5000商品数',
                                          price_band_over_5000_count BIGINT COMMENT '价格带>5000商品数'
)
    COMMENT '整合商品各项指标的大表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/integrated_metrics_table/'
tblproperties ("parquet.compression"="snappy");



-- 插入数据到商品指标汇总表（按日统计）
INSERT INTO TABLE integrated_metrics_table PARTITION (dt = '2025-04-02')
SELECT
    uv.item_id,
    'day' AS time_period,
    -- 访问指标
    COUNT(DISTINCT uv.user_id) AS item_visitor_count,
    COUNT(uv.visit_id) AS item_view_count,
    COUNT(DISTINCT uv.item_id) AS item_count_with_visit,
    AVG(uv.stay_duration) AS avg_stay_duration,
    AVG(CASE WHEN uv.is_bounce THEN 1.0 ELSE 0.0 END) AS item_detail_page_bounce_rate,
    0 AS micro_detail_visitor_count, -- 若无相关数据来源，先设为0，后续完善
    -- 收藏指标
    COUNT(DISTINCT uc.user_id) AS item_collection_count,
    -- 加购指标
    SUM(uatc.item_count) AS item_add_to_cart_count,
    COUNT(DISTINCT uatc.user_id) AS item_add_to_cart_person_count,
    -- 销售指标
    COUNT(DISTINCT uo.user_id) AS order_buyer_count,
    SUM(uo.item_count) AS order_item_count,
    SUM(uo.order_amount) AS order_amount,
    -- 防止分母为0，做条件判断
    CASE WHEN COUNT(DISTINCT uv.user_id) = 0 THEN NULL ELSE COUNT(DISTINCT uo.user_id)/COUNT(DISTINCT uv.user_id) END AS order_conversion_rate,
    COUNT(DISTINCT up.user_id) AS payment_buyer_count,
    COUNT(up.item_id) AS payment_item_count,
    SUM(up.payment_amount) AS payment_amount,
    COUNT(DISTINCT up.item_id) AS item_count_with_payment,
    CASE WHEN COUNT(DISTINCT uv.user_id) = 0 THEN NULL ELSE COUNT(DISTINCT up.user_id)/COUNT(DISTINCT uv.user_id) END AS payment_conversion_rate,
    -- 假设通过订单表的is_new_buyer字段判断新老买家
    COUNT(CASE WHEN uo.is_new_buyer THEN up.user_id END) AS payment_new_buyer_count,
    COUNT(CASE WHEN NOT uo.is_new_buyer THEN up.user_id END) AS payment_old_buyer_count,
    SUM(CASE WHEN NOT uo.is_new_buyer THEN up.payment_amount END) AS old_buyer_payment_amount,
    -- 防止分母为0，做条件判断
    CASE WHEN COUNT(DISTINCT up.user_id) = 0 THEN NULL ELSE SUM(up.payment_amount)/COUNT(DISTINCT up.user_id) END AS unit_price,
    SUM(CASE WHEN up.is_refunded THEN up.refund_amount END) AS successful_refund_amount,
    -- 这里假设没有年累计逻辑，先按当日支付金额统计，后续完善
    SUM(up.payment_amount) AS annual_accumulated_payment_amount,
    -- 防止分母为0，做条件判断
    CASE WHEN COUNT(DISTINCT uv.user_id) = 0 THEN NULL ELSE SUM(up.payment_amount)/COUNT(DISTINCT uv.user_id) END AS visitor_average_value,
    0 AS competitive_score, -- 若无相关数据来源，先设为0，后续完善
    -- 商品区间分析指标
    SUM(CASE WHEN di.item_price BETWEEN 100 AND 500 THEN 1 ELSE 0 END) AS price_band_100_500_count,
    SUM(CASE WHEN di.item_price BETWEEN 501 AND 1000 THEN 1 ELSE 0 END) AS price_band_501_1000_count,
    SUM(CASE WHEN di.item_price BETWEEN 1000 AND 5000 THEN 1 ELSE 0 END) AS price_band_1000_5000_count,
    SUM(CASE WHEN di.item_price > 5000 THEN 1 ELSE 0 END) AS price_band_over_5000_count
FROM
    ods_user_visits uv
-- 左连接用户收藏表
        LEFT JOIN
    ods_user_collections uc ON uv.item_id = uc.item_id AND uv.dt = uc.dt
-- 左连接用户加购表
        LEFT JOIN
    ods_user_add_to_cart uatc ON uv.item_id = uatc.item_id AND uv.dt = uatc.dt
-- 左连接用户下单表
        LEFT JOIN
    ods_user_orders uo ON uv.item_id = uo.item_id AND uv.dt = uo.dt
-- 左连接用户支付表
        LEFT JOIN
    ods_user_payments up ON uv.item_id = up.item_id AND uv.dt = up.dt
-- 连接商品表
        JOIN
    dim_item_info di ON uv.item_id = di.item_id
WHERE
        uv.dt = '2025-04-02'
GROUP BY
    uv.item_id, di.item_price;