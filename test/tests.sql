\set ON_ERROR_STOP true

begin;

\echo datatype is :datatype

create table pg_chronos_test_base (range :datatype);

create table pg_chronos_test_set1 (like pg_chronos_test_base including all);
create table pg_chronos_test_set2 (like pg_chronos_test_base including all);
create table pg_chronos_test_difference (like pg_chronos_test_base including all);
create table pg_chronos_test_union (like pg_chronos_test_base including all);
create table pg_chronos_test_intersection (like pg_chronos_test_base including all);

insert into pg_chronos_test_set1 (range) values
    (:datatype('2015-01-02', '2015-01-06')),
    (:datatype('2015-01-07', '2015-01-10')),
    (:datatype('2015-01-10', '2015-01-16')),
    (:datatype('2015-01-18', '2015-01-21')),
    (:datatype('2015-01-23', '2015-01-24')),
    (:datatype('2015-01-25', '2015-01-27')),
    (:datatype('2015-01-27', '2015-01-29')),
    (:datatype('2015-01-31', '2015-02-01')),
    (:datatype('2015-01-31', '2015-02-01')),
    (:datatype('2015-01-31', '2015-02-01', '[]')),
    (:datatype('2015-01-31', '2015-02-01', '(]')),
    (:datatype('2015-01-31', '2015-02-01', '()'));

insert into pg_chronos_test_set2 (range) values
    (:datatype('2015-01-03', '2015-01-05')),
    (:datatype('2015-01-08', '2015-01-11')),
    (:datatype('2015-01-14', '2015-01-15')),
    (:datatype('2015-01-15', '2015-01-20')),
    (:datatype('2015-01-22', '2015-01-25')),
    (:datatype('2015-01-27', '2015-01-29')),
    (:datatype('2015-02-03', '2015-02-05'));

insert into pg_chronos_test_difference (range) values
    (:datatype('2015-01-02', '2015-01-03')),
    (:datatype('2015-01-05', '2015-01-06')),
    (:datatype('2015-01-07', '2015-01-08')),
    (:datatype('2015-01-11', '2015-01-14')),
    (:datatype('2015-01-20', '2015-01-21')),
    (:datatype('2015-01-25', '2015-01-27')),
    (:datatype('2015-01-31', '2015-02-01'));

insert into pg_chronos_test_union (range) values
    (:datatype('2015-01-02', '2015-01-06')),
    (:datatype('2015-01-07', '2015-01-21')),
    (:datatype('2015-01-22', '2015-01-29')),
    (:datatype('2015-01-31', '2015-02-01', '[]')),
    (:datatype('2015-02-03', '2015-02-05'));

insert into pg_chronos_test_intersection (range) values
    (:datatype('2015-01-03', '2015-01-05')),
    (:datatype('2015-01-08', '2015-01-11')),
    (:datatype('2015-01-14', '2015-01-16')),
    (:datatype('2015-01-18', '2015-01-20')),
    (:datatype('2015-01-23', '2015-01-24')),
    (:datatype('2015-01-27', '2015-01-29'));

\echo 'The following two tests should return true:'

select tstzrange(now(), now() + interval '1 day') @> now();
select daterange(current_date, (current_date + interval '1 day')::date) @> current_date;

\echo 'The following two tests should return false:'

select tstzrange(now(), now() + interval '1 day') @> now() + interval '1 day';
select daterange(current_date, (current_date + interval '1 day')::date) @> (current_date + interval '1 day')::date;

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
