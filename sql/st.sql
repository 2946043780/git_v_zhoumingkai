drop table if exists ods_order_info;
//创建订单表
create table ods_order_info (
                                `id` string COMMENT '订单编号',
                                `total_amount` decimal(10,2) COMMENT '订单金额',
                                `order_status` string COMMENT '订单状态',
                                `user_id` string COMMENT '用户id' ,
                                `payment_way` string COMMENT '支付方式',
                                `out_trade_no` string COMMENT '支付流水号',
                                `create_time` string COMMENT '创建时间',
                                `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_order_info/'
tblproperties ("parquet.compression"="snappy");
load data inpath "/2207A/mingkai_Zhou/origin_data/db/order_info/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_order_info partition (dt = '2025-03-24');


//创建订单详情表
drop table if exists ods_order_detail;
create table ods_order_detail(
                                 `id` string COMMENT '订单编号',
                                 `order_id` string  COMMENT '订单号',
                                 `user_id` string COMMENT '用户id' ,
                                 `sku_id` string COMMENT '商品id',
                                 `sku_name` string COMMENT '商品名称',
                                 `order_price` string COMMENT '下单价格',
                                 `sku_num` string COMMENT '商品数量',
                                 `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_order_detail/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/order_detail/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_order_detail partition (dt = '2025-03-24');

//创建商品表
drop table if exists ods_sku_info;
create table ods_sku_info(
                             `id` string COMMENT 'skuId',
                             `spu_id` string  COMMENT 'spuid',
                             `price` decimal(10,2) COMMENT '价格' ,
                             `sku_name` string COMMENT '商品名称',
                             `sku_desc` string COMMENT '商品描述',
                             `weight` string COMMENT '重量',
                             `tm_id` string COMMENT '品牌id',
                             `category3_id` string COMMENT '品类id',
                             `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_sku_info/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/sku_info/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_sku_info partition (dt = '2025-03-24');

//创建用户表
drop table if exists ods_user_info;
create table ods_user_info(
                              `id` string COMMENT '用户id',
                              `name`  string COMMENT '姓名',
                              `birthday` string COMMENT '生日' ,
                              `gender` string COMMENT '性别',
                              `email` string COMMENT '邮箱',
                              `user_level` string COMMENT '用户等级',
                              `create_time` string COMMENT '创建时间'
) COMMENT '用户信息'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_user_info/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/user_info/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_user_info partition (dt = '2025-03-24');

//创建商品一级分类表
drop table if exists ods_base_category1;
create table ods_base_category1(
                                   `id` string COMMENT 'id',
                                   `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_base_category1/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/base_category1/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_base_category1 partition (dt = '2025-03-24');

//创建商品二级分类表
drop table if exists ods_base_category2;
create external table ods_base_category2(
   `id` string COMMENT ' id',
   `name`  string COMMENT '名称',
  category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_base_category2/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/base_category2/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_base_category2 partition (dt = '2025-03-24');

//创建商品三级分类表
drop table if exists ods_base_category3;
create table ods_base_category3(
                                   `id` string COMMENT ' id',
                                   `name`  string COMMENT '名称',
                                   category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_base_category3/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/base_category3/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_base_category3 partition (dt = '2025-03-24');

//创建支付流水表
drop table if exists `ods_payment_info`;
create table  `ods_payment_info`(
                                    `id`  bigint COMMENT '编号',
                                    `out_trade_no`   string COMMENT '对外业务编号',
                                    `order_id`  string COMMENT '订单编号',
                                    `user_id`  string COMMENT '用户编号',
                                    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
                                    `total_amount`  decimal(16,2) COMMENT '支付金额',
                                    `subject`  string COMMENT '交易内容',
                                    `payment_type` string COMMENT '支付类型',
                                    `payment_time`  string COMMENT '支付时间'
)  COMMENT '支付流水表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_payment_info/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/payment_info/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_payment_info partition (dt = '2025-03-24');

drop table if exists ods_comment_log;
create external table ods_comment_log(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ods/ods_comment_log/'
tblproperties ("parquet.compression"="snappy");

load data inpath "/2207A/mingkai_Zhou/origin_data/db/comment_info/2025-03-24" overwrite into table dev_realtime_v1_mingkai_zhou.ods_comment_log partition (dt = '2025-03-24');
------------------------------------DWD-------------------------------------------------
drop table if exists dwd_order_info;
create external table dwd_order_info (
   `id` string COMMENT '',
   `total_amount` decimal(10,2) COMMENT '',
   `order_status` string COMMENT ' 1 2  3  4  5',
  `user_id` string COMMENT 'id' ,
 `payment_way` string COMMENT '',
 `out_trade_no` string COMMENT '',
 `create_time` string COMMENT '',
 `operate_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy");

set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_order_info partition(dt)
select  * from dev_realtime_v1_mingkai_zhou.ods_order_info
where dt='2025-03-24'  and id is not null;


drop table if exists dwd_order_detail;
create external table dwd_order_detail(
   `id` string COMMENT '',
   `order_id` decimal(10,2) COMMENT '',
  `user_id` string COMMENT 'id' ,
 `sku_id` string COMMENT 'id',
 `sku_name` string COMMENT '',
 `order_price` string COMMENT '',
 `sku_num` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy");

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_order_detail partition(dt)
select  * from dev_realtime_v1_mingkai_zhou.ods_order_detail
where dt='2025-03-24'  and id is not null;


drop table if exists dwd_user_info;
create external table dwd_user_info(
   `id` string COMMENT 'id',
   `name`  string COMMENT '',
  `birthday` string COMMENT '' ,
 `gender` string COMMENT '',
 `email` string COMMENT '',
 `user_level` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy");

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_user_info partition(dt)
select  * from dev_realtime_v1_mingkai_zhou.ods_user_info
where dt='2025-03-24'  and id is not null;



drop table if exists `dwd_payment_info`;
create external  table  `dwd_payment_info`(
`id`  bigint COMMENT '',
`out_trade_no`  string COMMENT '',
`order_id`  string COMMENT '',
`user_id`  string COMMENT '',
`alipay_trade_no` string COMMENT '',
`total_amount`  decimal(16,2) COMMENT '',
`subject`  string COMMENT '',
`payment_type` string COMMENT '',
`payment_time`  string COMMENT ''
 )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy");

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_payment_info partition(dt)
select  * from dev_realtime_v1_mingkai_zhou.ods_payment_info
where dt='2025-03-24'  and id is not null;



drop table if exists `dwd_comment_log`;
create external  table  `dwd_comment_log`(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
 )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_comment_log/'
tblproperties ("parquet.compression"="snappy");

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_comment_log partition(dt)
select  * from dev_realtime_v1_mingkai_zhou.ods_comment_log
where dt='2025-03-24'  and id is not null;



drop table if exists dwd_sku_info;
create external table dwd_sku_info(
   `id` string COMMENT 'skuId',
   `spu_id` string COMMENT 'spuid',
  `price` decimal(10,2) COMMENT '' ,
 `sku_name` string COMMENT '',
 `sku_desc` string COMMENT '',
 `weight` string COMMENT '',
 `tm_id` string COMMENT 'id',
 `category3_id` string COMMENT '1id',
 `category2_id` string COMMENT '2id',
 `category1_id` string COMMENT '3id',
 `category3_name` string COMMENT '3',
 `category2_name` string COMMENT '2',
 `category1_name` string COMMENT '1',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy");

insert  overwrite table   dev_realtime_v1_mingkai_zhou.dwd_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id as category2_id ,
    c1.id as category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    dev_realtime_v1_mingkai_zhou.ods_sku_info sku
        join dev_realtime_v1_mingkai_zhou.ods_base_category3 c3 on sku.category3_id=c3.id
        join dev_realtime_v1_mingkai_zhou.ods_base_category2 c2 on c3.category2_id=c2.id
        join dev_realtime_v1_mingkai_zhou.ods_base_category1 c1 on c2.category1_id=c1.id
where sku.dt='2025-03-24'  and c2.dt='2025-03-24'
  and  c3.dt='2025-03-24' and  c1.dt='2025-03-24'
  and sku.id is not null;

----------------------------------DWS-----------------------------------

drop table if exists dws_user_action;
create  external table dws_user_action
(
    user_id         string      comment '用户 id',
    order_count     bigint      comment '下单次数 ',
    order_amount    decimal(16,2)  comment '下单金额 ',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额 ',
    comment_count   bigint      comment '评论次数'
) COMMENT '每日用户行为宽表'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/mingkai_Zhou/warehouse/gmall/dws/dws_user_action/'
tblproperties ("parquet.compression"="snappy");

with
    tmp_order as
        (
            select
                user_id,
                sum(oc.total_amount) order_amount,
                count(*)  order_count
            from dwd_order_info  oc
            where date_format(oc.create_time,'yyyy-MM-dd')='2025-03-24'
            group by user_id
        )  ,
    tmp_payment as
        (
            select
                user_id,
                sum(pi.total_amount) payment_amount,
                count(*) payment_count
            from dwd_payment_info pi
            where date_format(pi.payment_time,'yyyy-MM-dd')='2025-03-24'
            group by user_id
        ),
    tmp_comment as
        (
            select
                user_id,
                count(*) comment_count
            from dwd_comment_log c
            where date_format(c.dt,'yyyy-MM-dd')='2025-03-24'
            group by user_id
        )
insert overwrite table dws_user_action partition(dt='2025-03-24')
select
    user_actions.user_id,
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count)
from
    (
        select
            user_id,
            order_count,
            order_amount ,
            0 payment_count ,
            0 payment_amount,
            0 comment_count
        from tmp_order

        union all
        select
            user_id,
            0,
            0,
            payment_count,
            payment_amount,
            0
        from tmp_payment

        union all
        select
            user_id,
            0,
            0,
            0,
            0,
            comment_count
        from tmp_comment
    ) user_actions
group by user_id;


-----------------------------ADS----------------------------
drop table if exists ads_gmv_sum_day;
create table ads_gmv_sum_day(
                                `dt` string COMMENT '统计日期',
                                `gmv_count`  bigint COMMENT '当日gmv订单个数',
                                `gmv_amount`  decimal(16,2) COMMENT '当日gmv订单总金额',
                                `gmv_payment`  decimal(16,2) COMMENT '当日支付金额'
) COMMENT '每日活跃用户数量'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_gmv_sum_day/';

insert into table ads_gmv_sum_day
select
    '2025-03-24' as dt ,
    sum(order_count)  gmv_count ,
    sum(order_amount) gmv_amount ,
    sum(payment_amount) as payment_amount
from dws_user_action
where dt ='2025-03-24'
group by dt;

select * from ads_gmv_sum_day;


drop table if exists ads_user_convert_day;
create   table ads_user_convert_day(
                                       `dt` string COMMENT '统计日期',
                                       `uv_m_count`  bigint COMMENT '当日活跃设备',
                                       `new_m_count`  bigint COMMENT '当日新增设备',
                                       `new_m_ratio`   decimal(10,2) COMMENT '当日新增占日活的比率'
) COMMENT '每日活跃用户数量'
row format delimited  fields terminated by '\t'
location '/2207A/mingkai_Zhou/warehouse/gmall/ads/ads_user_convert_day/';

insert into table ads_user_convert_day
select
    '2025-03-24',
    sum( uc.dc) sum_dc,
    sum( uc.nmc) sum_nmc,
    cast(sum( uc.nmc)/sum( uc.dc)*100 as decimal(10,2))  new_m_ratio
from
    (
        select
            day_count dc,
            0 nmc
        from ads_uv_count
        where  dt='2025-03-24'

        union all
        select
            0 dc,
            new_mid_count nmc
        from ads_new_mid_count
        where create_date='2025-03-24'
    )uc;

select * from ads_user_convert_day;
