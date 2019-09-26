create table tree
(
    id      bigserial primary key,
    pid     bigint,
    level   bigint,
    meta    jsonb default '{}'::jsonb,
    content jsonb default '{}'::jsonb
);
create index on tree using gin (meta);
create index on tree (pid);

create table data
(
    id         bigint primary key,
    meta       jsonb     default '{}'::jsonb,
    content    jsonb     default '{}'::jsonb,
    created_at timestamp default now()
);
create index on data using gin (meta);

create schema ml;

create table ml.node
(
    id         serial primary key,
    network_id int,
    version    int,
    layer      int,
    idx        int,
    w          float[],
    b          float
);
create index on ml.node (network_id);
create index on ml.node (layer, idx);

create table ml.results
(
    id         serial primary key,
    network_id int,
    group_id   int,
    layer      int,
    idx        int,
    zeta       float,
    alpha      float,
    delta      float,
    pd         ml.node_expression
);
create index on ml.results (network_id);
create index on ml.results (layer, idx);

create table ml.t
(
    id       serial primary key,
    group_id int,
    idx      int,
    value    float
);
create index on ml.t (group_id);

create table ml.data
(
    id       serial primary key,
    group_id int,
    idx      int,
    value    float
);
create index on ml.data (group_id);
create index on ml.data (idx);

create or replace function ml.sigmoid(x float) returns float
as
$$
select 1 / (1 + exp(-x))
$$ language SQL immutable;

create or replace function ml.d_sigmoid(x float) returns float
as
$$
select ml.sigmoid(x) * (1 - ml.sigmoid(x));
$$ language SQL immutable;

create or replace function ml.rand_node(weight_count int)
    returns table
            (
                w float[],
                b float
            )
as
$$
with recursive weights as (
    select random() as w
    union all
    select random() as w
    from weights
),
               agg as (select w from weights limit weight_count)
select array_agg(w) as w, random() as b
from agg;
$$ language SQL immutable;

create type ml.node_expression as (w float[], b float);
create type ml.result_expression as (layer int, idx int, zeta float, alpha float);

create or replace function ml.input_layer(size int)
    returns table
            (
                idx int,
                w   float[],
                b   float
            )
as
$$
with recursive nodes as (select 1 as idx, '{1.0}'::float[] w, 0.0 as b
                         union all
                         select idx + 1 as idx, '{1.0}'::float[] w, 0.0 as b
                         from nodes
                         where idx < size)
select idx, w, b::float
from nodes;
$$ language SQL immutable;


create or replace function ml.rand_layer(size int, wc int)
    returns table
            (
                idx int,
                w   float[],
                b   float
            )
as
$$
with recursive nodes as (
    select 1 as idx, w, b
    from ml.rand_node(wc)
    union all
    select nodes.idx + 1 as idx, n.w, n.b
    from nodes,
         ml.rand_node(wc) as n
    where idx < size)
select *
from nodes;
$$ language SQL immutable;

create or replace function ml.rand_layer(layer int, size int, wc int)
    returns table
            (
                layer int,
                idx   int,
                w     float[],
                b     float
            )
as
$$
with recursive nodes as (
    select layer, 1 as idx, w, b
    from ml.rand_node(wc)
    union all
    select layer, nodes.idx + 1 as idx, n.w, n.b
    from nodes,
         ml.rand_node(wc) as n
    where idx < size)
select *
from nodes;
$$ language SQL immutable;

create or replace function ml.rand_network(layers int[])
    returns table
            (
                layer int,
                idx   int,
                w     float[],
                b     float
            )
as
$$
with recursive n(layer) as (
    select 2
    union
    select n.layer + 1
    from n
    where layer < array_length(layers, 1)
),
               input_layer as (select 1, idx, w, b from ml.input_layer(layers[1])),
               nodes as (
                   select l.*
                   from n
                            join ml.rand_layer(layer, layers[layer], layers[layer - 1]) as l on true
                   where n.layer <= array_length(layers, 1)
               )
select *
from input_layer
union all
select *
from nodes;
$$ language SQL immutable;

create or replace function ml.zeta(w float[], b float, alpha float[]) returns float
as
$$
select sum(a * w[ordinality]) + b
from unnest(alpha) with ordinality as a;
$$ language SQL immutable;

create or replace function ml.resolve_nodes(nodes ml.node_expression[], alpha float[])
    returns table
            (
                zeta  float[],
                alpha float[]
            )
as
$$
with zetas as (select ml.zeta((n::ml.node_expression).w, (n::ml.node_expression).b, alpha) as z from unnest(nodes) as n)
select array_agg(z), array_agg(ml.alpha(z)) as a
from zetas
$$ language SQL immutable;

create or replace function ml.alpha(z float) returns float
as
$$
select ml.sigmoid(z);
$$ language SQL immutable;


create or replace function ml.resolve(gid int)
    returns table
            (
                layer int,
                idx   int,
                zeta  float,
                alpha float
            )
as
$$
with recursive network as (select layer, idx, w, b from ml.node),
               data as (select idx, value from ml.data where group_id = gid order by idx),
               results(layer, zeta, alpha) as (
                   select 1 as layer, array_agg(value) as zeta, array_agg(value) as alpha
                   from data
                   union all
                   select r.layer + 1,
                          (ml.resolve_nodes(array(select (w, b)::ml.node_expression
                                                  from network
                                                  where layer = r.layer + 1
                                                  order by idx),
                                            r.alpha)).*
                   from results as r
                   where r.layer < (select max(layer) from network)),
               dataset(layer, zeta, alpha, idx) as (select layer, o.*
                                                    from results,
                                                         unnest(zeta, alpha) with ordinality as o)
select layer, idx::int, zeta, alpha
from dataset;
$$ language SQL immutable;

create or replace function ml.resolve()
    returns table
            (
                group_id int,
                layer    int,
                idx      int,
                zeta     float,
                alpha    float
            )
as
$$
with recursive groups as (
    select min(group_id) as g
    from ml.data
    union all
    select g + 1 as g
    from groups
    where g < (select max(group_id) from ml.data)),
               results as (select groups.g as group_id, (ml.resolve(groups.g)).*
                           from groups)
select *
from results;
$$ language SQL immutable;

create or replace function ml.output_delta(a float, z float, t float) returns float
as
$$
select (a - t) * ml.d_sigmoid(z)
$$ language SQL immutable;

create or replace function ml.update_output_delta()
    returns table
            (
                group_id int,
                idx      int,
                delta    float
            )
as
$$
update ml.results
set delta = ml.output_delta(alpha, zeta, (select value
                                          from ml.t
                                          where t.group_id = results.group_id
                                            and t.idx = results.idx))
where layer = (select max(layer) from ml.results) returning results.group_id, results.idx, delta;
$$ language SQL;

create or replace function ml.update_hidden_delta(l int)
    returns table
            (
                group_id int,
                layer    int,
                idx      int,
                delta    float
            )
as
$$
with am as (select w, nr.group_id, nr.delta
            from ml.node as nl
                     join ml.results as nr on nl.layer = nr.layer and nl.idx = nr.idx
            where nl.layer = l + 1)
update ml.results
set delta = ml.d_sigmoid(results.zeta) *
            (select sum(w[results.idx] * am.delta) from am where am.group_id = results.group_id)
where layer = l returning group_id
    , l
    , idx
    , delta
$$ language SQL;

create or replace function ml.update_delta()
    returns table
            (
                group_id int,
                layer    int,
                idx      int,
                delta    float
            )
as
$$
with recursive last as (select max(layer) as l from ml.results),
               layers as (select group_id, (select l from last) as layer, idx, delta
                          from ml.update_output_delta()
                          union all
                          select d.group_id, layers.layer - 1 as layer, d.idx, d.delta
                          from layers
                                   join ml.update_hidden_delta(layers.layer - 1) as d
                                        on layers.layer = d.layer and layers.idx = d.idx
                          where layers.layer > 1)
select group_id, layer, idx, delta
from layers;
$$ language SQL;

create or replace function ml.update_partial_differential()
    returns table
            (
                layer int,
                idx   int,
                pd    ml.node_expression
            )
as
$$
with a as (select group_id, layer, idx, alpha, delta from ml.results)
update ml.results
set pd = (array((select a.alpha * ml.results.delta
                 from a
                 where a.layer = ml.results.layer - 1
                   and a.group_id = ml.results.group_id
                 order by idx)),
          delta)::ml.node_expression
where layer > 1 returning layer, idx, pd;
$$ language SQL;

create or replace function ml.train_once(eta float)
    returns table
            (
                layer int,
                idx   int,
                w     float[],
                b     float
            )
as
$$
with partials as (select layer, idx, ordinality, partial
                  from ml.results
                           join lateral unnest((pd::ml.node_expression).w) with ordinality as partial on true),
     partial as (select layer, idx, ordinality, sum(partial) as wpd
                 from partials
                 group by layer, idx, ordinality),
     intercepts as (select layer, idx, sum((pd::ml.node_expression).b) as b
                    from ml.results
                    where layer > 1
                    group by layer, idx),
     intercept_trained as (select n.layer, n.idx, n.b - i.b * eta as b
                           from intercepts as i
                                    join ml.node as n on i.layer = n.layer and i.idx = n.idx),
     weights as (select layer, idx, ordinality, weight
                 from ml.node
                          join lateral unnest(w) with ordinality as weight on true
                 where layer > 1),
     weights_walk as (select w.layer, w.idx, w.ordinality, (w.weight - p.wpd * eta) as weight
                      from partial as p
                               join weights as w on p.layer = w.layer and p.idx = w.idx and p.ordinality = w.ordinality
                      order by 1, 2, 3),
     weight_trained as (select layer, idx, array_agg(weight) as w from weights_walk group by 1, 2),
     train as (select w.layer, w.idx, w.w, i.b
               from weight_trained as w
                        join intercept_trained as i on w.layer = i.layer and w.idx = i.idx)
update ml.node
set w = train.w,
    b = train.b,
    version = version +1
from train
where node.layer = train.layer
  and node.idx = train.idx
  and train.layer > 1 returning node.layer, node.idx, node.w, node.b;
$$
    language SQL;

create or replace function ml.cost() returns float
as
$$
select sum((t.value - r.alpha) ^ 2) / 2
from ml.t
         join ml.results as r on t.group_id = r.group_id and r.idx = t.idx
where r.layer = (select max(layer) from ml.results);
$$ language SQL immutable;

create or replace function ml.train(eta float, cost_range float)
    returns setof float
as
$$
declare
    c float;
begin
    loop
        delete from ml.results where id > 0;
        alter sequence ml.results_id_seq restart;
        insert into ml.results(group_id, layer, idx, zeta, alpha)
        select group_id, layer, idx, zeta, alpha
        from ml.resolve();
        c = ml.cost();
        if c < cost_range then
            return;
            else
            return next c;
        end if;
        perform ml.update_output_delta();
        perform ml.update_hidden_delta(2);
        perform ml.update_partial_differential();
        perform ml.train_once(eta);
    end loop;
end;
$$ language PLPGSQL;

create or replace function ml.binary()
    returns table
            (
                group_id int,
                value    int
            )
as
$$
with data as (select group_id, alpha
              from ml.results
              where layer = 3
              order by idx),
     cluster as (
         select group_id, array_agg(alpha) as pair
         from data
         group by group_id)
select group_id, case when pair[1] > pair[2] then 0 else 1 end as value
from cluster;
$$ language SQL;