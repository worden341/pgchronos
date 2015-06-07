OVERVIEW

pgchronos are sql functions and operators for Postgresql for performing union, intersection, and difference operations on sets of daterange and tstzrange types.

As an example of a difference operation, imagine a hotel where frequent customers can earn special member rates during most of the year, excluding holiday weeks.

Select unnest(
    (select array_agg(range) from special_membership_ranges) -
    (select array_agg(range) from holiday_exclusion_ranges)) special_booking_ranges;

  special_booking_ranges
-------------------------
 [2015-01-02,2015-01-06)
 [2015-01-07,2015-01-21)
 [2015-01-22,2015-01-29)

These functions were originally developed as part of the ChronosDB project.  That code became half-redundant when Postgres introduced range data types in version 9.2. This code is a fork that brings it current with the latest Postgres versions, and utilizes the native Postgres range data types.  The current project site is https://github.com/worden341/pgchronos.


REQUIREMENTS

pgchronos utilizes the range datatypes introduced in Postgresql 9.2, therefore that is the minimum version you can use with this extension.


INSTALLATION

Copy all pgchronos* files to Postgresql's extension directory; on Debian, that is /usr/share/postgresql/<version>/extension.  Then execute "CREATE EXTENSION pgchronos;".  After installation, you may choose to run the included test script tests_run.sql.


PROVIDED OPERATORS AND FUNCTIONS

                                                             List of operators
 Name |      Left arg type       |      Right arg type      | Result type |                                          Description
------+--------------------------+--------------------------+-------------+------------------------------------------------------------------------------------------------
 *    | daterange[]              | daterange[]              | daterange[] | Return all range segments common to both operands
 *    | tstzrange[]              | tstzrange[]              | tstzrange[] | Return all range segments common to both operands
 +    | daterange[]              | daterange[]              | daterange[] | Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges
 +    | tstzrange[]              | tstzrange[]              | tstzrange[] | Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges
 -    | daterange[]              | daterange[]              | daterange[] | Subtract from the first operand range segments that are occupied by the second operand.
 -    | tstzrange[]              | tstzrange[]              | tstzrange[] | Subtract from the first operand range segments that are occupied by the second operand.
 <@   | date                     | daterange[]              | boolean     | True if date is contained in any daterange in the array
 <@   | timestamp with time zone | tstzrange[]              | boolean     | True if timestamp is contained in any range in the array
 @>   | daterange[]              | date                     | boolean     | True if date is contained in any daterange in the array
 @>   | tstzrange[]              | timestamp with time zone | boolean     | True if timestamp is contained in any range in the array



                                    List of functions
         Name          | Result data type |             Argument data types              |  Description
-----------------------+------------------+----------------------------------------------+-----------------------------------------------------------------------------------------------
 contains              | boolean          | d daterange[], dt date                       | True if date is contained in any daterange in the array
 contains              | boolean          | dt date, d daterange[]                       | True if date is contained in any daterange in the array
 contains              | boolean          | tsr tstzrange[], ts timestamp with time zone | True if timestamp is contained in any range in the array
 contains              | boolean          | ts timestamp with time zone, tsr tstzrange[] | True if timestamp is contained in any range in the array
 difference            | daterange[]      | dr1 daterange[], dr2 daterange[]             | See "-" operator above
 difference            | tstzrange[]      | ts1 tstzrange[], ts2 tstzrange[]             | See "-" operator above
 exists_adjacent_lower | boolean          | ts tstzrange, tsr tstzrange[]                | True if a range exists in the array that is adjacent to the lower bound of the range operand
 exists_adjacent_upper | boolean          | ts tstzrange, tsr tstzrange[]                | True if a range exists in the array that is adjacent to the upper bound of the range operand
 exists_upper          | boolean          | ts timestamp with time zone, tsr tstzrange[] | True if a range exists in the array having its upper bound equal to the timestamp
 intersection          | daterange[]      | dr1 daterange[], dr2 daterange[]             | See "*" operator above
 intersection          | tstzrange[]      | dr1 tstzrange[], dr2 tstzrange[]             | See "*" operator above
 range_union           | daterange[]      | dr1 daterange[], dr2 daterange[]             | See "+" operator above
 range_union           | tstzrange[]      | dr1 tstzrange[], dr2 tstzrange[]             | See "+" operator above
 reduce                | daterange[]      | dr daterange[]                               | Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges
 reduce                | tstzrange[]      | dr tstzrange[]                               | Union overlapping and adjacent ranges into an array of non-overlapping and non-adjacent ranges

