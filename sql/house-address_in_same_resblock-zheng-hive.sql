add jar hdfs://nn-cluster/user/bigdata/lib/dataDev-1.0.jar;
create temporary functiON decodepublic AS 'com.lianjia.datadev.udf.AES128DecodePublicUDF';

-- 这个sql是要导出 分散式整租 同一个商家同一个品牌，在同一个小区下的房源楼栋号
SELECT 
	--用来 groupby 的字段如下：
	business_code, -- 商家code
	brand, -- 品牌id
	hdic_resblock_id, --楼盘字典小区ID
	rent_type,  -- 出租类型
	-- 需要对每一个groupby统计的字段：
	-- 每个groupby的分组下，统计的房源ids信息，以及重复的房源数量。
	concat_ws('#',collect_set(concat(building_name,',',house_unit_name,',',house_no,',',room_name))) as g_ids,
	-- 链接：building_name, -- 楼栋名称;house_unit_name, -- 单元号;house_no, -- 房屋门牌号;room_name --房间名
	-- 关于collect_set（）函数的作用：对于非group by字段，可以用Hive的collect_set函数收集这些字段，返回一个数组；
	count(1) AS cou 
FROM
(
  select
	business_code, -- 商家code
	brand, -- 品牌id
	hdic_resblock_id, --楼盘字典小区ID
	a.rent_type  as  rent_type,-- 出租类型
	building_name,
	house_unit_name,
	house_no,
	room_name,
	b.house_code as house_code
  from
	(
	SELECT 
		house_code, -- 委托编号
		rent_unit_code, -- 出租单元编号
		state, -- 状态： 枚举值：2:待出租,-1:删除
		name AS room_name, --房间名
		rent_type -- 出租类型
	FROM 
		ods.ods_rpms_rent_unit_da 
	WHERE 
		pt='${hiveconf:pt_date}' AND 
		state='2' AND 
		rent_type='111' 
	)a 
	
	LEFT OUTER JOIN 
	
	(
	SELECT 
		house_code, -- 委托编号
		business_code, -- 商家code
		brand,  -- 品牌id，
		hdic_resblock_id, -- 楼盘字典小区ID
		building_name, -- 楼栋名称
		house_unit_name, -- 单元号
		house_no -- 房屋门牌号
	FROM 
		ods.ods_rpms_rent_house_da 
	WHERE 
		pt='${hiveconf:pt_date}' AND 
		state ='1'
	)b 
	ON a.house_code = b.house_code 
) x
WHERE 
	house_code is not null 
GROUP BY 
	business_code, -- 商家code
	brand, -- 品牌id
	hdic_resblock_id, --楼盘字典小区ID
	rent_type  -- 出租类型
HAVING count(1)>1 