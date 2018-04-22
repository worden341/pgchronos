-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgchronos" to load this file. \quit

CREATE OR REPLACE FUNCTION pgchronos_range_inclusiveness_text
(
    in_lower boolean,
    in_upper boolean
)
returns char(2) as
$$
    select
        case when in_lower then '[' else '(' end ||
        case when in_upper then ']' else ')' end;
$$ language sql
IMMUTABLE STRICT;
COMMENT on FUNCTION pgchronos_range_inclusiveness_text(boolean, boolean) IS 'Return a literal bracket expression suitable for a range constructor function parameter.';

CREATE OR REPLACE FUNCTION contains(
    tsr tstzrange[],
    ts timestampTz
) RETURNS boolean AS
$$
    select coalesce
    (
        (
            SELECT true
            FROM (select unnest(tsr) tr) tr
            where ts <@ tr.tr
            limit 1
        ),
        false
    );
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION contains(tstzrange[], timestampTz) IS 'True if timestamp is contained in any range in the array';

CREATE OR REPLACE FUNCTION contains(
    ts timestamptz,
    tsr tstzrange[]
) RETURNS boolean AS
$$
    select contains(tsr,ts);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION contains(timestamptz, tstzrange[]) IS 'True if timestamp is contained in any range in the array';

CREATE OR REPLACE FUNCTION contains(
    d daterange[],
    dt date
) RETURNS boolean AS
$$
    select coalesce
    (
        (
            SELECT true
            FROM (select unnest(d) d) d
            where dt <@ d.d
            limit 1
        ),
        false
    );
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION contains(daterange[], date) IS 'True if date is contained in any daterange in the array';

CREATE OR REPLACE FUNCTION contains(
    dt date,
    d daterange[]
) RETURNS boolean AS
$$
    select contains(d,dt);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION contains(date, daterange[]) IS 'True if date is contained in any daterange in the array';

CREATE OR REPLACE FUNCTION exists_adjacent_lower(
    ts tstzrange,
    tsr tstzrange[]
) RETURNS boolean AS
$$
select coalesce
(
    (
        SELECT true
        FROM unnest(tsr) r
        WHERE ts -|- r.r
            and r.r << ts
        LIMIT 1
    ),
    false
);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION exists_adjacent_lower(ts tstzrange, tsr tstzrange[]) is 'True if a range exists in the array that is adjacent to the lower bound of the range operand';

CREATE OR REPLACE FUNCTION exists_adjacent_upper(
    ts tstzrange,
    tsr tstzrange[]
) RETURNS boolean AS
$$
select coalesce
(
    (
        SELECT true
        FROM unnest(tsr) r
        WHERE ts -|- r.r
            and r.r >> ts
        LIMIT 1
    ),
    false
);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION exists_adjacent_upper(ts tstzrange, tsr tstzrange[]) is 'True if a range exists in the array that is adjacent to the upper bound of the range operand.';

CREATE OR REPLACE FUNCTION exists_upper(
    ts timestamptz,
    tsr tstzrange[]
) RETURNS boolean AS
$$
select coalesce
(
    (
        SELECT true
        FROM unnest(tsr) r
        WHERE upper(r) = ts
        LIMIT 1
    ),
    false
);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION exists_upper(ts timestamptz, tsr tstzrange[]) is 'True if a range exists in the array having its upper bound equal to the timestamp';

CREATE OR REPLACE FUNCTION difference(
   ts1  IN tstzrange[],
   ts2  IN tstzrange[]
) RETURNS tstzrange[] AS
$$
--TODO: inclusiveness-aware
    with
        t_ts1 as (select unnest(ts1) d),
        t_ts2 as (select * from unnest(ts2) d where not isempty(d))
    SELECT array_agg(prd)
    FROM (
        SELECT tstzrange((d_in).start_date, MIN((d_out).end_date)) AS prd
        FROM (
            SELECT lower(d) AS start_date
            FROM t_ts1
            WHERE
                not exists
                (
                    --~~ lower(d1.d) contained in ts2:
                    select 1 from t_ts2
                    where
                        d && t_ts1.d
                        and t_ts1.d &> d
                )

            UNION

            SELECT upper(d)
            FROM t_ts2
            WHERE
                exists
                (
                    --~~ t_ts2.d contained in ts1:
                    select 1 from t_ts1
                    where
                        t_ts1.d && t_ts2.d
                        and
                        (
                            not t_ts1.d &> t_ts2.d
                            and not
                            (
                                t_ts2.d &< t_ts1.d
                                and t_ts1.d &< t_ts2.d
                            )
                        )
                )
        ) d_in
        JOIN (
            SELECT upper(d) AS end_date
            FROM t_ts1
            WHERE
                not exists
                (
                    select 1 from t_ts2
                    where
                        t_ts2.d && t_ts1.d
                        and
                        (
                            not t_ts2.d &< t_ts1.d
                            or
                            (
                                t_ts2.d &< t_ts1.d
                                and t_ts1.d &< t_ts2.d
                            )
                        )
                )

            UNION

            SELECT lower(d)
            FROM t_ts2
            WHERE --contains(ts1, lower(d))
                exists
                (
                    --~~ lower(t_ts2.d) contained in ts1:
                    select 1 from t_ts1
                    where
                        t_ts1.d && t_ts2.d
                        and not t_ts1.d &> t_ts2.d
                )
        ) d_out ON d_in.start_date < d_out.end_date
        GROUP BY (d_in).start_date
        ORDER BY (d_in).start_date
    ) sub;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
comment on FUNCTION difference(tstzrange[], tstzrange[]) is 'Subtract from the first operand range segments that are occupied by the second operand.';

CREATE OR REPLACE FUNCTION difference(
   dr1  IN daterange[], 
   dr2  IN daterange[]
) RETURNS daterange[] AS
$$
    SELECT array_agg(prd)
    FROM (
        SELECT daterange((d_in).start_date, MIN((d_out).end_date)) AS prd
        FROM (
            SELECT DISTINCT lower(d) AS start_date
            FROM unnest(dr1) d
            WHERE NOT contains(dr2,lower(d))
            AND NOT contains(dr1, lower(d) - 1)

            UNION

            SELECT DISTINCT upper(d)
            FROM unnest(dr2) d
            WHERE contains(dr1, upper(d))
            AND NOT contains(dr2, upper(d))
        ) d_in
        JOIN (
            SELECT upper(d) AS end_date
            FROM unnest(dr1) d
            WHERE NOT contains(dr1, upper(d))

            UNION ALL

            SELECT lower(d)
            FROM unnest(dr2) d
            WHERE contains(dr1, lower(d))
              AND NOT contains(dr2, lower(d) - 1)
        ) d_out ON d_in.start_date < d_out.end_date
        GROUP BY (d_in).start_date
        ORDER BY (d_in).start_date
    ) sub;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
comment on FUNCTION difference(daterange[], daterange[]) is 'Subtract from the first operand range segments that are occupied by the second operand.';

CREATE OR REPLACE FUNCTION intersection(
    dr1 IN tstzrange[], 
    dr2 IN tstzrange[]
) RETURNS tstzrange[] AS
$$
    SELECT array_agg(t)
    FROM (
        SELECT tstzrange(start_date, MIN(end_date)) AS t
        FROM (
            SELECT DISTINCT lower(d) AS start_date
            FROM unnest(dr1) d
            WHERE NOT exists_adjacent_lower(d,dr1)
              AND contains(dr2, lower(d))

            UNION

            SELECT DISTINCT lower(d) 
            FROM unnest(dr2) d
            WHERE NOT exists_adjacent_lower(d,dr2)
              AND contains(dr1, lower(d))
        ) AS t_in
        JOIN (
            SELECT upper(d) AS end_date
            FROM unnest(dr1) d
            WHERE NOT contains(dr1, upper(d))
              AND (contains(dr2, upper(d)) OR exists_upper(upper(d),dr2))

            UNION ALL

            SELECT upper(d)
            FROM unnest(dr2) d
            WHERE NOT contains(dr2, upper(d))
              AND (contains(dr1, upper(d)) OR exists_upper(upper(d),dr1))
        ) AS t_out ON t_in.start_date < t_out.end_date
        GROUP BY t_in.start_date
        ORDER BY t_in.start_date
    ) sub;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION intersection(tstzrange[], tstzrange[]) IS 'Return all range segments common to both operands';

CREATE OR REPLACE FUNCTION intersection(
    dr1 IN daterange[], 
    dr2 IN daterange[]
) RETURNS daterange[] AS
$$
    SELECT array_agg(t)
    FROM (
        SELECT daterange(start_date, MIN(end_date)) AS t
        FROM (
            SELECT DISTINCT lower(d) AS start_date
            FROM unnest(dr1) d
            WHERE NOT contains(dr1, lower(d) - 1)
              AND contains(dr2, lower(d))

            UNION

            SELECT DISTINCT lower(d) 
            FROM unnest(dr2) d
            WHERE NOT contains(dr2, lower(d) - 1)
              AND contains(dr1, lower(d))
        ) AS t_in
        JOIN (
            SELECT upper(d) AS end_date
            FROM unnest(dr1) d
            WHERE NOT contains(dr1, upper(d))
              AND contains(dr2, upper(d)-1)

            UNION ALL

            SELECT upper(d)
            FROM unnest(dr2) d
            WHERE NOT contains(dr2, upper(d))
              AND contains(dr1, upper(d)-1)
        ) AS t_out ON t_in.start_date < t_out.end_date
        GROUP BY t_in.start_date
        ORDER BY t_in.start_date
    ) sub;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION intersection(daterange[], daterange[]) IS 'Return all range segments common to both operands';

CREATE OR REPLACE FUNCTION reduce(dr tstzrange[])
RETURNS tstzrange[] AS
$$
    with d as (select distinct d from unnest(dr) d)
    select array_agg(r)
    from
    (
        with combos as (
            SELECT
                start_date,
                end_date,
                tstzrange(start_date, end_date, pgchronos_range_inclusiveness_text(t_in.inc, t_out.inc)) r
            FROM (
                SELECT lower(d) AS start_date, max(lower_inc(d)::int)::bool as inc
                FROM d
                WHERE NOT exists_adjacent_lower(d,dr)
                    AND NOT EXISTS (
                        select 1 from d d2
                        where true
                            and lower(d.d) <@ d2.d
                            and lower(d.d) <> lower(d2.d)
                    )
                GROUP BY lower(d)
            ) AS t_in
            JOIN (
                SELECT upper(d) AS end_date, max(upper_inc(d)::int)::bool as inc
                FROM d
                WHERE NOT exists_adjacent_upper(d,dr)
                    AND NOT EXISTS (
                        select 1 from d d2
                        where true
                            and upper(d.d) <@ d2.d
                            and upper(d.d) <> upper(d2.d)
                    )
                GROUP BY upper(d)
            ) AS t_out ON t_in.start_date < t_out.end_date
        )
        select r
        from combos c1
        where
            exists (
                select 1
                from combos
                group by start_date
                having
                    start_date = c1.start_date
                    and min(end_date) = c1.end_date
            )
            and not exists (
                select 1 from combos
                where true
                    and start_date = c1.start_date
                    and end_date = c1.end_date
                    and r <> c1.r
                    and c1.r <@ r
            )
    ) sub
    ;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION reduce(tstzrange[]) IS 'Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges';

CREATE OR REPLACE FUNCTION reduce(dr daterange[])
RETURNS daterange[] AS
$$
    SELECT array_agg(t)
    FROM (
        SELECT daterange(start_date, MIN(end_date)) AS t
        FROM (
            SELECT DISTINCT lower(d) AS start_date
            FROM unnest(dr) d
            WHERE NOT contains(dr, lower(d)-1)
        ) AS t_in
        JOIN (
            SELECT upper(d) AS end_date
            FROM unnest(dr) d
            WHERE NOT contains(dr, upper(d))
        ) AS t_out ON t_in.start_date < t_out.end_date
        GROUP BY t_in.start_date
        ORDER BY t_in.start_date
    ) sub;
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION reduce(daterange[]) IS 'Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges';

CREATE OR REPLACE FUNCTION range_union(
   dr1  IN tstzrange[],
   dr2  IN tstzrange[]
) RETURNS tstzrange[] AS
$$
   SELECT reduce(dr1 || dr2);
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
COMMENT ON FUNCTION range_union(tstzrange[], tstzrange[])
IS 'Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges';

CREATE OR REPLACE FUNCTION range_union(
   dr1  IN daterange[],
   dr2  IN daterange[]
) RETURNS daterange[] AS
$$
   SELECT reduce(dr1 || dr2);
$$ LANGUAGE 'sql' IMMUTABLE;
COMMENT ON FUNCTION range_union(daterange[], daterange[])
IS 'Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges';

CREATE OPERATOR @>(
  PROCEDURE = contains,
  LEFTARG = tstzrange[],
  RIGHTARG = timestamptz,
  commutator = <@
);

CREATE OPERATOR <@(
  PROCEDURE = contains,
  LEFTARG = timestamptz,
  RIGHTARG = tstzrange[],
  commutator = @>
);

CREATE OPERATOR @>(
  PROCEDURE = contains,
  LEFTARG = daterange[],
  RIGHTARG = date,
  commutator = <@
);

CREATE OPERATOR <@(
  PROCEDURE = contains,
  LEFTARG = date,
  RIGHTARG = daterange[],
  commutator = @>
);

CREATE OPERATOR - (
  PROCEDURE = difference,
  LEFTARG = daterange[],
  RIGHTARG = daterange[]
);

CREATE OPERATOR - (
  PROCEDURE = difference,
  LEFTARG = tstzrange[],
  RIGHTARG = tstzrange[]
);

CREATE OPERATOR * (
  PROCEDURE = intersection,
  LEFTARG = daterange[],
  RIGHTARG = daterange[]
);

CREATE OPERATOR * (
  PROCEDURE = intersection,
  LEFTARG = tstzrange[],
  RIGHTARG = tstzrange[]
);

CREATE OPERATOR + (
  PROCEDURE = range_union,
  LEFTARG = daterange[],
  RIGHTARG = daterange[]
);

CREATE OPERATOR + (
  PROCEDURE = range_union,
  LEFTARG = tstzrange[],
  RIGHTARG = tstzrange[]
);
