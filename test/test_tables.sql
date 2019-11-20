drop table if exists test_result;
drop table if exists test_sequence;
create table test_sequence
(
    id text primary key,
    r1_lower timestamp with time zone,
    r1_upper timestamp with time zone,
    r2_lower timestamp with time zone,
    r2_upper timestamp with time zone
);

insert into test_sequence(id)
values
(''),
('__'),
('_1'),
('1_'),
('11'),
('12'),
('___1'),
('__1_'),
('__11'),
('__12'),
('1111'),
('1112'),
('111_'),
('1122'),
('1123'),
('112_'),
('1211'),
('_211'),
('1212'),
('_212'),
('1213'),
('121_'),
('_21_'),
('1223'),
('_223'),
('122_'),
('_22_'),
('1234'),
('_234'),
('123_'),
('_23_'),
('1312'),
('1314'),
('_314'),
('131_'),
('_31_'),
('1324'),
('132_'),
('1423');

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
alter table test_result add constraint test_result_uniq unique (sequence_id, datatype, operator, lower_inc1, upper_inc1, lower_inc2, upper_inc2);

DO $anon$
declare
    D_ constant timestamp with time zone = null;
    D1 constant timestamp with time zone = '2018-01-04';
    D2 constant timestamp with time zone = '2018-01-06';
    D3 constant timestamp with time zone = '2018-01-22';
    D4 constant timestamp with time zone = '2018-01-27';
    v_index integer;
    v_seq_id text;
    v_date1 timestamp with time zone;
    v_date2 timestamp with time zone;
    v_date3 timestamp with time zone;
    v_date4 timestamp with time zone;
begin
    for v_seq_id in select id from test_sequence order by id LOOP
        v_date1 := null;
        v_date2 := null;
        v_date3 := null;
        v_date4 := null;
        if length(v_seq_id) >= 2 then
            select
                case substr(v_seq_id, 1, 1)
                    when '_' then D_
                    when '1' then D1
                    when '2' then D2
                    when '3' then D3
                    when '4' then D4
                end into v_date1
            ;
            select
                case substr(v_seq_id, 2, 1)
                    when '_' then D_
                    when '1' then D1
                    when '2' then D2
                    when '3' then D3
                    when '4' then D4
                end into v_date2
            ;
            if length(v_seq_id) = 4 then
                select
                    case substr(v_seq_id, 3, 1)
                        when '_' then D_
                        when '1' then D1
                        when '2' then D2
                        when '3' then D3
                        when '4' then D4
                    end into v_date3
                ;
                select
                    case substr(v_seq_id, 4, 1)
                        when '_' then D_
                        when '1' then D1
                        when '2' then D2
                        when '3' then D3
                        when '4' then D4
                    end into v_date4
                ;
            end if;
            update test_sequence
            set
                r1_lower = v_date1,
                r1_upper = v_date2,
                r2_lower = v_date3,
                r2_upper = v_date4
            where id = v_seq_id
            ;
        end if;
    end loop;
end;
$anon$;
