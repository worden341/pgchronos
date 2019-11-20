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
        --and length(ts.id) = 4
    order by tr.id LOOP
        if length(v_id) = 4 then
            return next is((reduce(array[tstzrange(v_r1_lower, v_r1_upper, pgchronos_range_inclusiveness_text(v_lower_inc1, v_upper_inc1)), tstzrange(v_r2_lower, v_r2_upper, pgchronos_range_inclusiveness_text(v_lower_inc2, v_upper_inc2))]))::text, v_result::text,
                format('reduce %s%s%s %s%s%s id=%s',
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
                format('reduce %s%s%s id=%s',
                    case when v_lower_inc1 then '[' else '(' end,
                    substr(v_id, 1, 2),
                    case when v_upper_inc1 then ']' else ')' end,
                    v_result_id
                )
            );
        elsif length(v_id) = 0 then
            return next is((reduce(array[]::tstzrange[]))::text, v_result::text,
                format('reduce empty array id=%s', v_result_id)
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
