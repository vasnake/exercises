-- creating roles and granting access https://youtu.be/EQ44K5GfgDw?t=2006

-- create a role
-- grant access
-- run GET_DDL

use role accountadmin;
use schema ecommerce_db.ecommerce_liv;

create or replace role view_role;

-- grant access
grant usage on warehouse compute_wh to role view_role;
grant usage on database ecommerce_db to role view_role;
grant usage on schema ecommerce_liv to role view_role;

-- no such view (should be)
grant select on ecommerce_db.ecommerce_liv.secure_vw_aggregated_orders to role view_role;

-- add role to user
grant role view_role to user vlk;

-- switch to a new role
use role view_role;

-- select some data
use schema ECOMMERCE_DB.ECOMMERCE_LIV;
select * from secure_vw_aggregated_roles;
select get_ddl('view', 'urgent_priority_orders'); -- should work

select get_ddl('view', 'secure_priority_orders'); -- should fail

-- Introduction to streams https://youtu.be/EQ44K5GfgDw?t=2198
