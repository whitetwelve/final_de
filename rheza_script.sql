--datamart1  gross revenue total/hari
with revenue1 as (
with revenue as (
    select os.order_date, os.order_id as orderid1, od.product_id, od.quantity, od.discount, od.unit_price 
    from orders as os
    left join order_details as od on os.order_id = od.order_id
    )
select order_date, revenue.orderid1, unit_price, quantity, discount, 
(quantity*(unit_price-(discount*unit_price))) as total_price from revenue
) select order_date, sum(total_price) as totalprice from revenue1 group by 1 order by 1;

--datamart2  gross revenue per product per bulan
with revenue2 as (
with revenue1 as (
with revenue as (
    select os.order_date, os.order_id as orderid1, od.product_id, 
    od.quantity, od.discount, od.unit_price 
    from orders as os
    left join order_details as od on os.order_id = od.order_id
    )
select order_date, revenue.product_id, revenue.orderid1, unit_price, quantity, discount, 
(quantity*(unit_price-(discount*unit_price))) as total_price from revenue
) 
select * from revenue1
left join products as pd on revenue1.product_id = pd.product_id
)
select DATE_TRUNC ('month', order_date) as bulan, product_name, sum(total_price) as total_price_product 
from revenue2
group by 1,2 order by 1,2


--datamart3  jumlah total pembelian per product per bulan
with revenue as (
    select os.order_date, os.order_id as orderid1, od.product_id, pd.product_name, 
    od.quantity, od.discount, od.unit_price 
    from orders as os
    left join order_details as od on os.order_id = od.order_id
    left join products as pd on od.product_id = pd.product_id
    )
select DATE_TRUNC ('month', order_date) as bulan, revenue.product_name, sum(quantity) as jumlah_quantity from revenue
group by 1,2 order by 1,2

--datamart4  jumlah total pembelian per kategori per bulan
with revenue1 as (
with revenue as (
    select os.order_date, os.order_id as orderid1, od.product_id, pd.product_name, pd.category_id, 
    od.quantity, od.discount, od.unit_price 
    from orders as os
    left join order_details as od on os.order_id = od.order_id
    left join products as pd on od.product_id = pd.product_id
    )
select * from revenue
left join categories as ca on revenue.category_id = ca.category_id
)
select DATE_TRUNC ('month', order_date) as bulan, revenue1.category_name, sum(quantity) as jumlah_quantity
from revenue1
group by 1,2 order by 1,2

--datamart5  jumlah total pembelian per negara per bulan

with revenue as (
    select os.order_date, os.order_id as orderid1,  os.ship_country, od.product_id,  
    od.quantity 
    from orders as os
    left join order_details as od on os.order_id = od.order_id
    )
select DATE_TRUNC ('month', order_date) as bulan, revenue.ship_country, sum(quantity) as jumlah_quantity
from revenue
group by 1,2 order by 1,2

---CREATE VIEW 
CREATE VIEW GROSS_REVENUE (order_date, total_price) AS
(
    with revenue1 as (
        with revenue as (
        select os.order_date, os.order_id as orderid1, od.product_id, od.quantity, 
        od.discount,od.unit_price 
        from orders as os
        left join order_details as od on os.order_id = od.order_id
        )
    select order_date, revenue.orderid1, unit_price, quantity, discount, 
    (quantity*(unit_price-(discount*unit_price))) as total_price from revenue
) 
select order_date, sum(total_price) as totalprice from revenue1 group by 1 order by 1
);

---create view datamart2
CREATE VIEW GROSS_REVENUE_PER_PRODUCT (bulan,product_name,total_price_product) AS
(
    with revenue2 as (
    with revenue1 as (
    with revenue as (
        select os.order_date, os.order_id as orderid1, od.product_id, 
        od.quantity, od.discount, od.unit_price 
        from orders as os
        left join order_details as od on os.order_id = od.order_id
        )
    select order_date, revenue.product_id, revenue.orderid1, unit_price, quantity, discount, 
    (quantity*(unit_price-(discount*unit_price))) as total_price from revenue
    ) 
    select * from revenue1
    left join products as pd on revenue1.product_id = pd.product_id
    )
    select DATE_TRUNC ('month', order_date) as bulan, product_name, sum(total_price) as total_price_product 
    from revenue2
    group by 1,2 order by 1,2
);

---create view datamart3
CREATE VIEW TOTAL_PEMBELIAN_PERPRODUCT_PERBULAN (bulan, product_name, jumlah_quantity)AS
(
    with revenue as (
        select os.order_date, os.order_id as orderid1, od.product_id, pd.product_name, 
        od.quantity, od.discount, od.unit_price 
        from orders as os
        left join order_details as od on os.order_id = od.order_id
        left join products as pd on od.product_id = pd.product_id
        )
    select DATE_TRUNC ('month', order_date) as bulan, revenue.product_name, 
    sum(quantity) as jumlah_quantity from revenue
    group by 1,2 order by 1,2
);

---create view datamart4
CREATE VIEW TOTAL_PEMBELIAN_PERCATEGORY_PERBULAN (bulan,category_name, jumlah_quantity) AS
(
    with revenue1 as (
    with revenue as (
        select os.order_date, os.order_id as orderid1, od.product_id, pd.product_name, pd.category_id, 
        od.quantity, od.discount, od.unit_price 
        from orders as os
        left join order_details as od on os.order_id = od.order_id
        left join products as pd on od.product_id = pd.product_id
        )
    select * from revenue
    left join categories as ca on revenue.category_id = ca.category_id
    )
    select DATE_TRUNC ('month', order_date) as bulan, revenue1.category_name, sum(quantity) as    
    jumlah_quantity
    from revenue1 group by 1,2 order by 1,2
);

---create view datamart5
CREATE VIEW TOTAL_PEMBELIAN_PERNEGARA_PERBULAN (bulan, ship_country, jumlah_quantity) AS
(
    with revenue as (
        select os.order_date, os.order_id as orderid1,  os.ship_country, od.product_id,  
        od.quantity 
        from orders as os
        left join order_details as od on os.order_id = od.order_id
        )
    select DATE_TRUNC ('month', order_date) as bulan, revenue.ship_country, sum(quantity) as jumlah_quantity
    from revenue
    group by 1,2 order by 1,2
);

