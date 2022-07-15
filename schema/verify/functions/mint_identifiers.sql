-- Verify seattleflu/schema:functions/mint_identifiers on pg

begin;

do $$
	declare
		test record;
	begin
        create temp table if not exists tests
			as select uuid, barcode, identifier_set_id, generated
			from public.mint_identifiers(6, 3);

		assert (select count(*) from tests) = 3;

        for test in table tests loop
            assert test.identifier_set_id = 6;
        end loop;

    end
$$;

rollback;
