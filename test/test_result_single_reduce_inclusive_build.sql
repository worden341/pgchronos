\set ON_ERROR_STOP true
begin;

/******************
* Reduce tstz 
*******************/
insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result_tstz)
select
    '' as id,
    'tstz' datatype,
    '+' op,
    false,
    false,
    reduce(array[]::tstzrange[]) result
;

    select
        ts.id,
        ts.r1_lower,
        ts.r1_upper,
        inc1.lower_inc1,
        inc1.upper_inc1
    into temporary table tmp_combos_2
    from
        test_sequence ts
        cross join
        (
            values
                (true, true),
                (true, false),
                (false, true),
                (false, false)
        ) inc1 (lower_inc1, upper_inc1)
    where
        length(ts.id) = 2
        and not
        (
            --these are invalid values:
            (r1_lower is null and lower_inc1)
            or (r1_upper is null and upper_inc1)
        )
        and not
        (
            r1_lower = r1_upper
            and (not lower_inc1 or not upper_inc1)
        )
    ;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result_tstz)
select
--     format('%s%s%s',
--         case when lower_inc1 then '[' else '(' end,
--         substr(id, 1, 2),
--         case when upper_inc1 then ']' else ')' end
--     ) id,
    id,
    'tstz' datatype,
    '+' op,
    lower_inc1,
    upper_inc1,
    reduce(array[tstzrange(r1_lower, r1_upper, pgchronos_range_inclusiveness_text(lower_inc1, upper_inc1))]) result
from tmp_combos_2
order by 1,3,4,5
--order by 1
;

    select
        ts.id,
        ts.r1_lower,
        ts.r1_upper,
        ts.r2_lower,
        ts.r2_upper,
        inc1.lower_inc1,
        inc1.upper_inc1,
        inc2.lower_inc2,
        inc2.upper_inc2
    into temporary table tmp_combos_4
    from
        test_sequence ts
        cross join
        (
            values
                (true, true),
                (true, false),
                (false, true),
                (false, false)
        ) inc1 (lower_inc1, upper_inc1)
        cross join
        (
            values
                (true, true),
                (true, false),
                (false, true),
                (false, false)
        ) inc2 (lower_inc2, upper_inc2)
    where true
        and length(ts.id) = 4
        and not
        (
            --these are invalid values:
            (r1_lower is null and lower_inc1)
            or (r1_upper is null and upper_inc1)
            or (r2_lower is null and lower_inc2)
            or (r2_upper is null and upper_inc2)
        )
        and not (ts.id = '1111' and inc1.lower_inc1) --eliminate redundant tests
        and not --invalid
        (
            r1_lower = r1_upper
            and (not lower_inc1 or not upper_inc1)
        )
        and not --invalid
        (
            r2_lower = r2_upper
            and (not lower_inc2 or not upper_inc2)
        )
    ;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result_tstz)
select
--     format('%s%s%s %s%s%s',
--         case when lower_inc1 then '[' else '(' end,
--         substr(id, 1, 2),
--         case when upper_inc1 then ']' else ')' end,
--         case when lower_inc2 then '[' else '(' end,
--         substr(id, 3),
--         case when upper_inc2 then ']' else ')' end
--     ) id,
    id,
    'tstz' datatype,
    '+' op,
    lower_inc1,
    upper_inc1,
    lower_inc2,
    upper_inc2,
    reduce(array[tstzrange(r1_lower, r1_upper, pgchronos_range_inclusiveness_text(lower_inc1, upper_inc1)), tstzrange(r2_lower, r2_upper, pgchronos_range_inclusiveness_text(lower_inc2, upper_inc2))]) result
from tmp_combos_4
order by 1,3,4,5,6
--order by 1
;

/******************
* Reduce date 
*******************/

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result_date)
select
    '' as id,
    'date' datatype,
    '+' op,
    false,
    false,
    reduce(array[]::daterange[]) result
;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result_date)
select
    id,
    'date' datatype,
    '+' op,
    lower_inc1,
    upper_inc1,
    reduce(array[daterange(r1_lower::date, r1_upper::date, pgchronos_range_inclusiveness_text(lower_inc1, upper_inc1))]) result
from tmp_combos_2
order by 1,3,4,5
--order by 1
;
insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result_date)
select
    id,
    'date' datatype,
    '+' op,
    lower_inc1,
    upper_inc1,
    lower_inc2,
    upper_inc2,
    reduce(array[daterange(r1_lower::date, r1_upper::date, pgchronos_range_inclusiveness_text(lower_inc1, upper_inc1)), daterange(r2_lower::date, r2_upper::date, pgchronos_range_inclusiveness_text(lower_inc2, upper_inc2))]) result
from tmp_combos_4
order by 1,3,4,5,6
--order by 1
;
commit;
