\pset format unaligned
\pset tuples_only true

\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true
\set QUIET 1

BEGIN;

\ir test_tables.sql

create or replace function test_f1()
returns setof text
language plpgsql as $func$
declare
    v_record record;
    v_row test_result%rowtype;
    v_id text;
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
    for v_id, v_r1_lower, v_r1_upper, v_r2_lower, v_r2_upper, v_lower_inc1, v_upper_inc1, v_lower_inc2, v_upper_inc2, v_result in
    select ts.id, r1_lower, r1_upper, r2_lower, r2_upper, lower_inc1, upper_inc1, lower_inc2, upper_inc2, tr.result
    from
        test_sequence ts
        join test_result tr on ts.id = tr.sequence_id
    order by tr.id LOOP
        return next is((reduce(array[tstzrange(v_r1_lower, v_r1_upper, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1)), tstzrange(v_r2_lower, v_r2_upper, pgchronos_range_inclusiveness_text(v_lower_inc2, v_upper_inc2))]))::text, v_result::text,
            format('reduce %s%s%s %s%s%s',
                case when v_lower_inc1 then '[' else '(' end,
                substr(v_id, 1, 2),
                case when v_upper_inc1 then ']' else ')' end,
                case when v_lower_inc2 then '[' else '(' end,
                substr(v_id, 3),
                case when v_upper_inc2 then ']' else ')' end
            )
        );
    end loop;
end;
$func$;

select * from runtests();

ROLLBACK;
