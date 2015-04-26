pgchronos are sql functions and operators for Postgresql for performing union, intersection, and difference operations on sets of tsrange and tstzrange types.

As an example of a difference operation, imagine a hotel where frequent customers can earn special member rates during most of the year, excluding holiday weeks.

Select unnest(
    (select array_agg(range) from special_membership_ranges) -
    (select array_agg(range) from holiday_exclusion_ranges)) special_booking_ranges;

  special_booking_ranges
-------------------------
 [2015-01-02,2015-01-06)
 [2015-01-07,2015-01-21)
 [2015-01-22,2015-01-29)

These functions were originally developed as part of the ChronosDB project.  That code became half-redundant when Postgres introduced range data types in version 9.2. This code is a fork that brings it current with the latest Postgres versions and utilizes the native Postgres range data types.

*****************
NOTE
This code is incomplete and lightly-tested. I hurriedly prepared it for a conference presentation.  It needs:
* Implementation of the "intersection" and "range_union" functions for tsrange and tstzrange types
* Complete tests
* Packaging as a Postgresql extension
*****************
