/*
create table test_result
(
    id serial primary key,
    sequence_id text references test_sequence(id),
    datatype text not null constraint ck_datatype check (datatype in ('date','tstz','ts')),
    operator text not null constraint ck_operator check (operator in ('+','-','*')),
    lower_inc1 boolean,
    upper_inc1 boolean,
    lower_inc2 boolean,
    upper_inc2 boolean,
    result_tstz tstzrange[],
    result_ts tsrange[],
    result_date daterange[]
);
*/
\copy test_result (id, sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2, result_tstz) from 'test_result_reduce_tstz.copy'
