-- Verify seattleflu/schema:functions/hamming_distance on pg

begin;

create temporary table tests (
    a text,
    b text,
    hamming_distance integer,
    threshold integer,
    hamming_distance_lte integer
);

copy tests from stdin;
abc	\N	\N	\N	\N
\N	abc	\N	\N	\N
\N	\N	\N	\N	\N
abc	abc	0	2	0
abc	Abc	1	2	1
abc	aBc	1	2	1
abc	abC	1	2	1
abc	ABc	2	0	1
abc	aBC	2	2	2
abc	ABC	3	1	2
abc	xyz	3	2	3
abc	XYZ	3	4	3
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

            if test.hamming_distance_lte is distinct from hamming_distance_lte(test.a, test.b, test.threshold) then
                raise exception 'hamming_distance_lte(%, %, %) returned %, expected %',
                    test.a, test.b, test.threshold, hamming_distance_lte(test.a, test.b, test.threshold), test.hamming_distance_lte;
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
            perform hamming_distance_lte('looooooong', 'short', 100);
            raise exception 'hamming_distance_lte() did not raise an error on mismatched string lengths';
        exception
            when data_exception then
                null; -- expected!
        end;
    end
$$;

rollback;
