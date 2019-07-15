select t1.ShopID,GoodsID,sales_value,total_sales,max_vlaue,total_sales/max_vlaue
,case when total_sales/max_vlaue <=0.6 then 'A'
when total_sales/max_vlaue <=0.8 and
total_sales/max_vlaue>0.6 then 'B'
when total_sales/max_vlaue > 0.8 then 'C' end
as type_1
from (select ShopID
,a.GoodsID
,a.sales_value
,(@total_sales := @total_sales + a.sales_value) as
total_sales
from
( select t1.GoodsID
,t1.ShopID
,sum(t1.SaleValue) as sales_value
from demo.OrderItem as t1
where t1.SDate between 20160101 and 20160131
and t1.ShopID = 'WDGC'
group by t1.GoodsID
,t1.ShopID
order by sum(t1.SaleValue) desc
) as a
join (select @total_sales := 0) as x
) t1
join (select t1.ShopID
,sum(t1.SaleValue) as max_vlaue
 from demo.OrderItem as t1
where t1.SDate between 20160101 and 20160131
and t1.ShopID = 'WDGC'
group by t1.ShopID
) t2 on t1.ShopID = t2.ShopID
order by sales_value desc;