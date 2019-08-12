-- Verify seattleflu/schema:functions/array_distinct on pg

begin;

set local role id3c;

create temporary table tests (
    input    int[],
    expected int[]
);

copy tests from stdin with (delimiter ' ');
{4,2,1,1,1,7,8,1,10,9,2,3} {4,2,1,7,8,10,9,3}
{} {}
{4} {4}
{4,4,4,4,4} {4}
{5,2,null,2,8,null} {5,2,null,8}
\N \N
\.

do $$
    declare
        test tests;
    begin
        for test in table tests loop
            if test.expected is distinct from array_distinct(test.input) then
                raise exception 'array_distinct(%) returned %, expected %',
                    test.input, array_distinct(test.input), test.expected;
            end if;
        end loop;
    end
$$;

rollback;
