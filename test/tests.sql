\pset format unaligned
\pset tuples_only true

\set ON_ERROR_ROLLBACK true
\set ON_ERROR_STOP true
\set QUIET true

BEGIN;

\ir test_tables.sql
\ir load_results.sql

create or replace function test_reduce_tstz()
returns setof text
language plpgsql as $func$
declare
    v_id text;
    v_result_id integer;
    v_r1_lower timestamptz;
    v_r1_upper timestamptz;
    v_r2_lower timestamptz;
    v_r2_upper timestamptz;
    v_lower_inc1 boolean;
    v_upper_inc1 boolean;
    v_lower_inc2 boolean;
    v_upper_inc2 boolean;
    v_result tstzrange[];
begin
    return next ok((reduce(null::tstzrange[]) is null), 'reduce(null) returns null');

    for v_result_id, v_id, v_r1_lower, v_r1_upper, v_r2_lower, v_r2_upper, v_lower_inc1, v_upper_inc1, v_lower_inc2, v_upper_inc2, v_result in
    select tr.id, ts.id, r1_lower, r1_upper, r2_lower, r2_upper, lower_inc1, upper_inc1, lower_inc2, upper_inc2, tr.result_tstz
    from
        test_sequence ts
        join test_result tr on ts.id = tr.sequence_id
    where
        tr.operator = '+'
        and tr.result_tstz is not null
    order by tr.id LOOP
        if length(v_id) = 4 then
            return next is((reduce(array[tstzrange(v_r1_lower, v_r1_upper, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1)), tstzrange(v_r2_lower, v_r2_upper, pgchronos_range_inclusiveness_text(v_lower_inc2, v_upper_inc2))]))::text, v_result::text,
                format('reduce tstz %s%s%s %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    case when v_lower_inc2 then '[' else '(' end,
                    substr(v_id, 3),
                    case when v_upper_inc2 then ']' else ')' end,
                    v_result_id
                )
            );
        elsif length(v_id) = 2 then
            return next is((reduce(array[tstzrange(v_r1_lower, v_r1_upper, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1))]))::text, v_result::text,
                format('reduce tstz %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    v_result_id
                )
            );
        elsif length(v_id) = 0 then
            return next is((reduce(array[]::tstzrange[]))::text, v_result::text,
                format('reduce tstz empty array id=%s', v_result_id)
            );
        end if;
    end loop;
end;
$func$;

create or replace function test_reduce_date()
returns setof text
language plpgsql as $func$
declare
    v_id text;
    v_result_id integer;
    v_r1_lower timestamptz;
    v_r1_upper timestamptz;
    v_r2_lower timestamptz;
    v_r2_upper timestamptz;
    v_lower_inc1 boolean;
    v_upper_inc1 boolean;
    v_lower_inc2 boolean;
    v_upper_inc2 boolean;
    v_result daterange[];
begin
    return next ok((reduce(null::daterange[]) is null), 'reduce(null) returns null');

    return next is(reduce(array[daterange('2000-01-01','2000-01-02'),daterange('2000-01-02','2000-01-03')]), array[daterange('2000-01-01','2000-01-03')] ,'reduce adjacent date ranges');

    for v_result_id, v_id, v_r1_lower, v_r1_upper, v_r2_lower, v_r2_upper, v_lower_inc1, v_upper_inc1, v_lower_inc2, v_upper_inc2, v_result in
    select tr.id, ts.id, r1_lower, r1_upper, r2_lower, r2_upper, lower_inc1, upper_inc1, lower_inc2, upper_inc2, tr.result_date
    from
        test_sequence ts
        join test_result tr on ts.id = tr.sequence_id
    where
        tr.operator = '+'
        and tr.result_date is not null
    order by tr.id LOOP
        if length(v_id) = 4 then
            return next is((reduce(array[daterange(v_r1_lower::date, v_r1_upper::date, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1)), daterange(v_r2_lower::date, v_r2_upper::date, pgchronos_range_inclusiveness_text(v_lower_inc2, v_upper_inc2))]))::text, v_result::text,
                format('reduce date %s%s%s %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    case when v_lower_inc2 then '[' else '(' end,
                    substr(v_id, 3),
                    case when v_upper_inc2 then ']' else ')' end,
                    v_result_id
                )
            );
        elsif length(v_id) = 2 then
            return next is((reduce(array[daterange(v_r1_lower::date, v_r1_upper::date, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1))]))::text, v_result::text,
                format('reduce date %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    v_result_id
                )
            );
        elsif length(v_id) = 0 then
            return next is((reduce(array[]::daterange[]))::text, v_result::text,
                format('reduce date empty array id=%s', v_result_id)
            );
        end if;
    end loop;
end;
$func$;

create or replace function test_range_union_tstz()
returns setof text
language plpgsql as $func$
declare
    c_want constant tstzrange[] := '{"[\"1999-12-31 16:00:00-08\",\"2000-01-01 04:00:00-08\")","[\"2000-01-01 16:00:00-08\",\"2000-01-02 04:00:00-08\")","[\"2000-01-03 16:00:00-08\",\"2000-01-04 10:00:00-08\")","[\"2000-01-02 16:00:00-08\",\"2000-01-03 10:00:00-08\")","[\"2000-01-04 16:00:00-08\",\"2000-01-05 04:00:00-08\")"}';
    v_have tstzrange[];
begin
   with twoa as
(
select
    array[tstzrange('2000-01-01 00:00+0', '2000-01-01 00:00+0'::timestamp with time zone + '12 hours')] ||  array_agg(tstzrange(d1.generate_series, d1.generate_series + '12 hours'::interval)) dra1,
    (array_agg(tstzrange(d1.generate_series + '6 hours'::interval, d1.generate_series + '18 hours'::interval)))[3:4] dra2
from
    (select generate_series('2000-01-01 00:00+0', '2000-01-01 00:00+0'::timestamp with time zone + '4 days'::interval, '1 day'::interval)) d1
)
select range_union(dra1,dra2) into v_have from twoa;

    return next cmp_ok(v_have, '@>', c_want, 'tstz range_union result contains expected result');
    return next cmp_ok(c_want, '<@', v_have, 'tstz range_union expected result contains actual result');
end;
$func$;

create or replace function test_intersection_tstz()
returns setof text
language plpgsql as $func$
declare
    v_id text;
    v_result_id integer;
    v_r1_lower timestamptz;
    v_r1_upper timestamptz;
    v_r2_lower timestamptz;
    v_r2_upper timestamptz;
    v_lower_inc1 boolean;
    v_upper_inc1 boolean;
    v_lower_inc2 boolean;
    v_upper_inc2 boolean;
    v_result tstzrange[];
begin
    return next ok((intersection(null::tstzrange[], null::tstzrange[]) is null), 'intersection(null,null) returns null');

    for v_result_id, v_id, v_r1_lower, v_r1_upper, v_r2_lower, v_r2_upper, v_lower_inc1, v_upper_inc1, v_lower_inc2, v_upper_inc2, v_result in
    select tr.id, ts.id, r1_lower, r1_upper, r2_lower, r2_upper, lower_inc1, upper_inc1, lower_inc2, upper_inc2, tr.result_tstz
    from
        test_sequence ts
        join test_result tr on ts.id = tr.sequence_id
    where
        tr.operator = '*'
        and tr.result_tstz is not null
    order by tr.id LOOP
        if length(v_id) = 4 then
            return next is((intersection(array[tstzrange(v_r1_lower, v_r1_upper, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1))], array[tstzrange(v_r2_lower, v_r2_upper, pgchronos_range_inclusiveness_text(v_lower_inc2, v_upper_inc2))]))::text, v_result::text,
                format('intersection tstz %s%s%s %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    case when v_lower_inc2 then '[' else '(' end,
                    substr(v_id, 3),
                    case when v_upper_inc2 then ']' else ')' end,
                    v_result_id
                )
            );
        end if;
    end loop;
end;
$func$;

create or replace function test_intersection_date()
returns setof text
language plpgsql as $func$
declare
    v_id text;
    v_result_id integer;
    v_r1_lower timestamptz;
    v_r1_upper timestamptz;
    v_r2_lower timestamptz;
    v_r2_upper timestamptz;
    v_lower_inc1 boolean;
    v_upper_inc1 boolean;
    v_lower_inc2 boolean;
    v_upper_inc2 boolean;
    v_result daterange[];
begin
    return next ok((intersection(null::daterange[], null::daterange[]) is null), 'intersection(null,null) returns null');

    return next is(intersection(array[daterange('2000-01-01','2000-01-02')], array[daterange('2000-01-02','2000-01-03')]), null::daterange[],'intersection adjacent date ranges');

    for v_result_id, v_id, v_r1_lower, v_r1_upper, v_r2_lower, v_r2_upper, v_lower_inc1, v_upper_inc1, v_lower_inc2, v_upper_inc2, v_result in
    select tr.id, ts.id, r1_lower, r1_upper, r2_lower, r2_upper, lower_inc1, upper_inc1, lower_inc2, upper_inc2, tr.result_date
    from
        test_sequence ts
        join test_result tr on ts.id = tr.sequence_id
    where
        tr.operator = '*'
        and tr.result_date is not null
    order by tr.id LOOP
        if length(v_id) = 4 then
            return next is((intersection(array[daterange(v_r1_lower::date, v_r1_upper::date)], array[daterange(v_r2_lower::date, v_r2_upper::date)]))::text, v_result::text,
                format('intersection date %s%s%s %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    case when v_lower_inc2 then '[' else '(' end,
                    substr(v_id, 3),
                    case when v_upper_inc2 then ']' else ')' end,
                    v_result_id
                )
            );
        end if;
    end loop;
end;
$func$;

select * from runtests();

ROLLBACK;

\pset format
\pset tuples_only

\unset ON_ERROR_ROLLBACK
\unset ON_ERROR_STOP
\unset QUIET
