-- Verify seattleflu/schema:functions/date_or_null on pg

begin;

create temporary table tests (
    input text,
    expected date
);

copy tests from stdin with (delimiter ';');
2020-01-17;2020-01-17
2020/01/17;2020-01-17
2020.01.17;2020-01-17
2020-01-17 00:00:00;2020-01-17
2020/01/17 00:00:00;2020-01-17
2020.01.17 00:00:00;2020-01-17
Jan 17, 2020;2020-01-17
January 17, 2020;2020-01-17
Fri 17 Jan 12:24:00 2020 PDT;2020-01-17
2020-01-50;\N
2020-01;\N
2;\N
na;\N
\N;\N
\.

do $$
    declare
        test tests;
    begin
        for test in table tests loop
            if test.expected is distinct from date_or_null(test.input) then
                raise exception 'date_or_null(%) returned %, expected %',
                    test.input, date_or_null(test.input), test.expected;
            end if;
        end loop;
    end
$$;

rollback;
