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
('0000'),
('0001'),
('0010'),
('0011'),
('0012'),
('1111'),
('1112'),
('1110'),
('1122'),
('1123'),
('1120'),
('1211'),
('0211'),
('1212'),
('0212'),
('1213'),
('1210'),
('0210'),
('1223'),
('0223'),
('1220'),
('0220'),
('1234'),
('0234'),
('1230'),
('0230'),
('1312'),
('1314'),
('0314'),
('1310'),
('0310'),
('1324'),
('1320'),
('1423');

create table test_result
(
    id serial primary key,
    sequence_id text references test_sequence(id),
    operator text not null,
    lower_inc1 boolean not null,
    upper_inc1 boolean not null,
    lower_inc2 boolean not null,
    upper_inc2 boolean not null,
    result tstzrange[]
);


DO $anon$
declare
    D0 constant timestamp with time zone = null;
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
        select
            case substr(v_seq_id, 1, 1)
                when '0' then D0
                when '1' then D1
                when '2' then D2
                when '3' then D3
                when '4' then D4
            end into v_date1
        ;
        select
            case substr(v_seq_id, 2, 1)
                when '0' then D0
                when '1' then D1
                when '2' then D2
                when '3' then D3
                when '4' then D4
            end into v_date2
        ;
        select
            case substr(v_seq_id, 3, 1)
                when '0' then D0
                when '1' then D1
                when '2' then D2
                when '3' then D3
                when '4' then D4
            end into v_date3
        ;
        select
            case substr(v_seq_id, 4, 1)
                when '0' then D0
                when '1' then D1
                when '2' then D2
                when '3' then D3
                when '4' then D4
            end into v_date4
        ;
        update test_sequence
        set
            r1_lower = v_date1,
            r1_upper = v_date2,
            r2_lower = v_date3,
            r2_upper = v_date4
        where id = v_seq_id
        ;
    end loop;
end;
$anon$;

\copy test_result from 'test_result.copy'
