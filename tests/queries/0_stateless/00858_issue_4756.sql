set enable_analyzer = 1;
set distributed_product_mode = 'local';
set optimize_skip_unused_shards = 0;

drop table if exists shard1;
drop table if exists shard2;
drop table if exists distr1;
drop table if exists distr2;

create table shard1 (id Int32) engine = MergeTree order by cityHash64(id);
create table shard2 (id Int32) engine = MergeTree order by cityHash64(id);

create table distr1 as shard1 engine Distributed (test_cluster_two_shards_localhost, currentDatabase(), shard1, cityHash64(id));
create table distr2 as shard2 engine Distributed (test_cluster_two_shards_localhost, currentDatabase(), shard2, cityHash64(id));

insert into shard1 (id) values (0), (1);
insert into shard2 (id) values (1), (2);

select distinct(distr1.id) from distr1
where distr1.id in
(
    select distr1.id
    from distr1
    join distr2 on distr1.id = distr2.id
    where distr1.id > 0
);

select distinct(d0.id) from distr1 d0
where d0.id in
(
    select d1.id
    from distr1 as d1
    join distr2 as d2 on d1.id = d2.id
    where d1.id > 0
);

-- The inner tables are aliased here, so by default `distr1.id` and `distr2.id` refer to the enclosing
-- query and the subquery is correlated, which `IN` does not support yet. This query keeps the previous
-- resolution, where the name of an aliased table expression qualifies it even inside a query that
-- selects from the same table.
select distinct(distr1.id) from distr1
where distr1.id in
(
   select distr1.id
   from distr1 as d1
   join distr2 as d2 on distr1.id = distr2.id
   where distr1.id > 0
)
settings analyzer_compatibility_qualify_aliased_table_by_name = 1;

drop table shard1;
drop table shard2;
drop table distr1;
drop table distr2;
