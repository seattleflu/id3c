-- Verify seattleflu/schema:functions/hamming_distance on pg

begin;

create temporary table tests (
    a text,
    b text,
    hamming_distance integer,
    hamming_distance_ci integer
);

copy tests from stdin;
abc	\N	\N	\N
\N	abc	\N	\N
\N	\N	\N	\N
abc	abc	0	0
abc	Abc	1	0
abc	aBc	1	0
abc	abC	1	0
abc	ABc	2	0
abc	aBC	2	0
abc	ABC	3	0
abc	xyz	3	3
abc	XYZ	3	3
\.

do $$
    declare
        test tests;
    begin
        for test in table tests loop
            if test.hamming_distance is distinct from hamming_distance(test.a, test.b) then
                raise exception 'hamming_distance(%, %) returned %, expected %',
                    test.a, test.b, hamming_distance(test.a, test.b), test.hamming_distance;
            end if;

            if test.hamming_distance_ci is distinct from hamming_distance_ci(test.a, test.b) then
                raise exception 'hamming_distance_ci(%, %) returned %, expected %',
                    test.a, test.b, hamming_distance_ci(test.a, test.b), test.hamming_distance_ci;
            end if;
        end loop;

        begin
            perform hamming_distance('looooooong', 'short');
            raise exception 'hamming_distance() did not raise an error on mismatched string lengths';
        exception
            when data_exception then
                null; -- expected!
        end;

        begin
            perform hamming_distance_ci('looooooong', 'short');
            raise exception 'hamming_distance_ci() did not raise an error on mismatched string lengths';
        exception
            when data_exception then
                null; -- expected!
        end;
    end
$$;

rollback;
