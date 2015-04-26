begin;

create table pg_chronos_test_base (range daterange);
alter table pg_chronos_test_base add constraint ex_range exclude using gist (range with &&);

create table pg_chronos_test_set1 (like pg_chronos_test_base including all);
create table pg_chronos_test_set2 (like pg_chronos_test_base including all);
create table pg_chronos_test_difference (like pg_chronos_test_base including all);
create table pg_chronos_test_union (like pg_chronos_test_base including all);
create table pg_chronos_test_intersection (like pg_chronos_test_base including all);

insert into pg_chronos_test_set1 (range) values
    (daterange('2015-01-02', '2015-01-06')),
    (daterange('2015-01-07', '2015-01-16')),
    (daterange('2015-01-18', '2015-01-21')),
    (daterange('2015-01-23', '2015-01-24')),
    (daterange('2015-01-25', '2015-01-29'));

insert into pg_chronos_test_set2 (range) values
    (daterange('2015-01-03', '2015-01-05')),
    (daterange('2015-01-08', '2015-01-11')),
    (daterange('2015-01-14', '2015-01-20')),
    (daterange('2015-01-22', '2015-01-25'));

insert into pg_chronos_test_difference (range) values
    (daterange('2015-01-02', '2015-01-03')),
    (daterange('2015-01-05', '2015-01-06')),
    (daterange('2015-01-07', '2015-01-08')),
    (daterange('2015-01-11', '2015-01-14')),
    (daterange('2015-01-20', '2015-01-21')),
    (daterange('2015-01-25', '2015-01-29'));

insert into pg_chronos_test_union (range) values
    (daterange('2015-01-02', '2015-01-06')),
    (daterange('2015-01-07', '2015-01-21')),
    (daterange('2015-01-22', '2015-01-29'));

insert into pg_chronos_test_intersection (range) values
    (daterange('2015-01-03', '2015-01-05')),
    (daterange('2015-01-08', '2015-01-11')),
    (daterange('2015-01-14', '2015-01-16')),
    (daterange('2015-01-18', '2015-01-20')),
    (daterange('2015-01-23', '2015-01-24'));

\echo The following tests should return two columns with matching values.
\echo difference test
select
    t.range as target,
    result.result
from
    (
        select
            unnest
            (
                (select array_agg(range) from pg_chronos_test_set1) - 
                (select array_agg(range) from pg_chronos_test_set2)
            ) result
    ) result
    full outer join pg_chronos_test_difference t
        on lower(t.range) = lower(result.result)
        and upper(t.range) = upper(result.result)
order by 
    coalesce
    (
        lower(t.range),
        lower(result.result)
    )
;

\echo intersection test
select
    t.range as target,
    result.result
from
    (
        select
            unnest
            (
                (select array_agg(range) from pg_chronos_test_set1) *
                (select array_agg(range) from pg_chronos_test_set2)
            ) result
    ) result
    full outer join pg_chronos_test_intersection t
        on lower(t.range) = lower(result.result)
        and upper(t.range) = upper(result.result)
order by 
    coalesce
    (
        lower(t.range),
        lower(result.result)
    )
;

\echo union test
select
    t.range as target,
    result.result
from
    (
        select
            unnest
            (
                (select array_agg(range) from pg_chronos_test_set1) +
                (select array_agg(range) from pg_chronos_test_set2)
            ) result
    ) result
    full outer join pg_chronos_test_union t
        on lower(t.range) = lower(result.result)
        and upper(t.range) = upper(result.result)
order by 
    coalesce
    (
        lower(t.range),
        lower(result.result)
    )
;

rollback;
