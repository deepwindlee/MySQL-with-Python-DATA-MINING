show databases;
use demo;
show tables;
/*
select t3.mmonth
,sku_number
,inven_number
,concat(truncate(sku_number/inven_number*100,2),'%') as rate
from(select month(SDate) as mmonth
,count(distinct GoodsID) as sku_number
from demo.OrderItem t
where t.SDate between '20170801' and '20170831'
group by month(SDate)) t3
join
(select month(t1.SDate) as mmonth
,count(distinct t1.GoodsID) as inven_number
from demo.Inventory t1
where t1.SDate between '20170801' and '20170831'
group by month(t1.SDate)
) t4 on t3.mmonth = t4.mmonth;
*/
---- 库存周转率
---- 如何需要算周转天数，再用 30 天除以周转率
/*select t3.mmonth
,sales_value/avg_inventory_value as zj_rate
from(select month(SDate) as mmonth
,sum(t.CostValue) as sales_value
from demo.OrderItem t
where t.SDate between '20170801' and '20170831'
group by month(SDate)) t3
join
(select month(t1.SDate) as mmonth
,sum(t1.CloseCostV)/2 as avg_inventory_value
from demo.Inventory t1
where t1.SDate in ('20170801','20170831')
group by month(t1.SDate)
) t4 on t3.mmonth = t4.mmonth;
*/

--- 库存周转天数
select t3.mmonth
,inventory_value/sales_value as zj_days
from(select month(SDate) as mmonth
,sum(t.CostValue) as sales_value
from demo.OrderItem t
where t.SDate between '20170801' and '20170831'
group by month(SDate)) t3
join
(select month(t1.SDate) as mmonth
,sum(t1.CloseCostV) as inventory_value
from demo.Inventory t1
where t1.SDate between '20170801' and '20170831'
group by month(t1.SDate)
) t4 on t3.mmonth = t4.mmonth;
