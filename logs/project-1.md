
## Assignment submitted by Sai Teja (*macharlasaiteja@gmail.com*)
------------------------------------------------------
1. How many users do we have?

Answer: 130

```SQL
select count(distinct user_guid) from dev_db.dbt_macharlasaitejagmailcom.stg_postgres__users
```
------------------------------------------------------

2. On average, how many orders do we receive per hour?

Answer: 
7.520833


``` SQL
with
order_counts as (
    select
        date_part('year', created_at_utc) as order_year,
        date_part('month', created_at_utc) as order_month,
        date_part('day', created_at_utc) as order_day,
        date_part('hour', created_at_utc) as order_hour,
        count(order_guid) over
            (partition by order_year, order_month, order_day, order_hour)
            as count_orders_per_hour
    from dev_db.dbt_macharlasaitejagmailcom.stg_postgres__orders
    qualify row_number() over
        (partition by order_year, order_month, order_day, order_hour
        order by created_at_utc asc) = 1
    order by created_at_utc asc
)

select avg(count_orders_per_hour) as avg_orders_per_hour from order_counts
```
------------------------------------------------------

3. On average, how long does an order take from being placed to being delivered?

Answer: 3.891803


```SQL
with
order_delivery_time as (
    select
        order_guid,
        datediff(day, created_at_utc, delivered_at_utc) as time_to_deliver
    from dev_db.dbt_macharlasaitejagmailcom.stg_postgres__orders
    order by created_at_utc asc
)

select avg(time_to_deliver) as average_time_to_deliver from order_delivery_time
```
------------------------------------------------------

4. How many users have only made one purchase? Two purchases? Three+ purchases?

Answer: 
- USERS_WITH_1_ORDER	--> 25
- USERS_WITH_2_ORDER	--> 28
- USERS_WITH_MORE_THAN_3_ORDERS --> 71

```SQL

with
orders as (
    select
        user_guid,
        count(distinct order_guid) over
            (partition by user_guid)
            as count_orders
    from dev_db.dbt_macharlasaitejagmailcom.stg_postgres__orders
    qualify row_number() over
        (partition by user_guid
        order by user_guid) = 1
    order by user_guid asc
)

select
    count_if(count_orders = 1) as users_with_1_order,
    count_if(count_orders = 2) as users_with_2_order,
    count_if(count_orders >= 3) as users_with_more_than_3_orders
from orders
```
------------------------------------------------------

5. On average, how many unique sessions do we have per hour?

Answer: 16.327586


```SQL
with
sessions as (
    select
        date_part('year', created_at_utc) as event_year,
        date_part('month', created_at_utc) as event_month,
        date_part('day', created_at_utc) as event_day,
        date_part('hour', created_at_utc) as event_hour,
        count(distinct session_guid) over
            (partition by event_year, event_month, event_day, event_hour)
            as count_sessions_per_hour
    from dev_db.dbt_macharlasaitejagmailcom.stg_postgres__events
    qualify row_number() over
        (partition by event_year, event_month, event_day, event_hour
        order by created_at_utc asc) = 1
)

select avg(count_sessions_per_hour) from sessions
```