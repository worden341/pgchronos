\set ON_ERROR_STOP true
begin;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result)
select
    '' as id,
    'tstz' datatype,
    '+' op,
    false,
    false,
    reduce(array[]::tstzrange[]) result
;

with combos as
(
    select
        ts.id,
        ts.r1_lower,
        ts.r1_upper,
        inc1.lower_inc1,
        inc1.upper_inc1
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
)
insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result)
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
from combos
order by 1,3,4,5
--order by 1
;

with combos as
(
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
)
insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result)
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
from combos
order by 1,3,4,5,6
--order by 1
;

--Insert timestamp[] tests:
insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, result)
select
    '' as id,
    'ts' datatype,
    '+' op,
    false,
    false,
    reduce(array[]::tsrange[]) result
;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result)
select
    sequence_id, 'ts' datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2,
    reduce(array[tsrange(r1_lower, r1_upper, pgchronos_range_inclusiveness_text(lower_inc1, upper_inc1))]) result
from test_result
where
    operator = '+'
    and datatype = 'tstz'
    and length(sequence_id) = 2;

insert into test_result (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result)
select
    sequence_id, 'ts' datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2,
from test_result
where
    operator = '+'
    and datatype = 'tstz'
    and length(sequence_id) = 4;

commit;

--sed -e '/./s/["{}\]//g' -e '/08/s/ 00:00:00-08//g' -e '/01-04/s/2018-01-04/1/g' -e '/01-06/s/2018-01-06/2/g' -e '/01-22/s/2018-01-22/3/g' -e '/01-27/s/2018-01-27/4/g'

