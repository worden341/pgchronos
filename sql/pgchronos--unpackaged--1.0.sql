-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgchronos FROM unpackaged" to load this file. \quit

ALTER EXTENSION pgchronos ADD FUNCTION contains(tstzrange[], timestamptz);
ALTER EXTENSION pgchronos ADD FUNCTION contains(timestamptz, tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION contains(daterange[], date);
ALTER EXTENSION pgchronos ADD FUNCTION contains(date, daterange[]);
ALTER EXTENSION pgchronos ADD FUNCTION exists_adjacent_lower(tstzrange, tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION exists_adjacent_upper(tstzrange, tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION exists_upper(timestamptz, tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION difference(tstzrange[], tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION difference(daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD FUNCTION intersection(tstzrange[], tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION intersection(daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD FUNCTION reduce(tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION reduce(daterange[]);
ALTER EXTENSION pgchronos ADD FUNCTION range_union(tstzrange[], tstzrange[]);
ALTER EXTENSION pgchronos ADD FUNCTION range_union(daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD OPERATOR @> (tstzrange[], timestamptz);
ALTER EXTENSION pgchronos ADD OPERATOR <@ (timestamptz, tstzrange[]);
ALTER EXTENSION pgchronos ADD OPERATOR @> (daterange[], date);
ALTER EXTENSION pgchronos ADD OPERATOR <@ (date, daterange[]);
ALTER EXTENSION pgchronos ADD OPERATOR - (daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD OPERATOR - (tstzrange[], tstzrange[]);
ALTER EXTENSION pgchronos ADD OPERATOR * (daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD OPERATOR * (tstzrange[], tstzrange[]);
ALTER EXTENSION pgchronos ADD OPERATOR + (daterange[], daterange[]);
ALTER EXTENSION pgchronos ADD OPERATOR + (tstzrange[], tstzrange[]);
