#!/usr/bin/env python
# encoding: utf-8
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

user_consume_acts = 
"insert overwrite table dw.userprofile_action_all
partition(data_date="start_date_str",labelid="${labelid}")
select nvl(t2.labelid, t1.labelid) aslabelid,
nvl(t2.userid, t1.userid) as userid,
nvl(t2.labelweight, t1.labelweight) aslabelweight
from (
select * from dw.userprofile_userlabel_all
where data_date=" "old_date_partition"
and labelid='${labelid}'
) t1 # 前日数据分区􀑝存储的用􁡧􁸷􃆮
full outer join (
# 这里插入的是上一􂇥代码
) t2 # 昨日􀑊务运行􀓗生的新的用􁡧􁸷􃆮
on (t1.userid = t2.userid and t1.labelid = t2.labelid) "

def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format = "%Y%m%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    old_date_partition = strftime(strptime(start_date_str, format) - datetime.timedelta(1), format)
    month_day_ago = strftime(strptime(start_date_str, format) - datetime.timedelta(30), format)
    # 􀵘python􀑝初始化Spark
    spark = SparkSession.builder.AppName("latest_30_acts").enableHiveSupport().getOrCreate()
    spark.sql(user_consume_acts) # user_consume_acts 对应􃅜二􂇥代码􀑝􁢗行的语句
if __name__ == '__main__':
    main()



SELECT 'ACTION_U_01_001' AS labelid, cast(user_id as string) as userid, count(distinct order_id) as labelweight from dw.order_info_fact 
WHERE pay_tatus = 1 # 订单状态已支付
    and  to_date(add_time) >="month_day_ago"  # 付款日期大于等于30日前
    and  to_date(add_time)  <="yesterday_date"# 付款日期小于等于昨天

union all 

SELECT  'ACTION_U_01_002' AS labelid, cast(user_id as string ) as userid, sum(order_total_amount) as labelweight from dw.order_info_fact 
where pay_tatus =1 
      and  to_date(add_time) >="month_day_ago"  # 付款日期大于等于30日前
      and  to_date(add_time)  <="yesterday_date"# 付款日期小于等于昨天

union all   
         select 'ACTION_U_01_003' as labelid, # 加入购物􄖖事件次数􁸷􃆮id
        cast(userid as string) as userid,
        count(distinct eventid) as labelweight # 加入购物􄖖事件次数
        from ods.page_event_log # 􀷻􂛩行为事件表
        where data_date >= "month_day_ago"
        and data_date <= "yesterday_date"
        and eventkey = 'add_to_shoppingbag' # 行为事件名称为“加入购物􄖖”
        and userid is not null # 用户id为非空值

#!/usr/bin/env python
# encoding: utf-8
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime
def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format = "%Y%m%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    old_date_partition = strftime(strptime(start_date_str, format) - datetime.timedelta(1), format)
    month_day_ago = strftime(strptime(start_date_str, format) - datetime.timedelta(30), format)
    # 􀵘python􀑝初始化Spark
    spark = SparkSession.builder.AppName("latest_30_acts").enableHiveSupport().getOrCreate()
    spark.sql(user_consume_acts) # user_consume_acts 对应􃅜二􂇥代码􀑝􁢗行的语句
if __name__ == '__main__':
    main()