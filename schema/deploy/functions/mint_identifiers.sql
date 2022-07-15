-- Deploy seattleflu/schema:functions/mint_identifiers to pg

begin;

create or replace function public.mint_identifiers(set_id integer, number_to_mint integer)
    returns setof warehouse.identifier
    language plpgsql as $$

        declare
            counter integer := 0;
            r record;
            failure_count integer := 0;
            error_detail text;
        begin
            while counter < number_to_mint loop

				begin
					insert into warehouse.identifier (identifier_set_id) values (set_id)
					returning uuid, barcode, identifier_set_id, generated into r;

                    raise notice 'Successfully minted identifier: %; barcode: %', r.uuid, r.barcode;
					counter := counter + 1;
					return next r;

				exception
					when others then
						GET STACKED DIAGNOSTICS error_detail =  PG_EXCEPTION_DETAIL;

						raise notice '%; %', SQLERRM, error_detail;
                        failure_count := failure_count + 1;

				end;

            end loop;
            raise notice 'minted_count: %', counter;
            raise notice 'failure_count: %', failure_count;

        end;


    $$
    volatile
    parallel unsafe;

comment on function public.mint_identifiers(integer, integer) is
    'Generates and inserts the specified number of valid identifiers for a given identifier set.';

commit;
