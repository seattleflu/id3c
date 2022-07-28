-- Deploy seattleflu/schema:functions/mint_identifiers to pg

begin;

create or replace function public.mint_identifiers(set_id integer, number_to_mint integer)
    returns setof warehouse.identifier
    language plpgsql as $$

        declare
            counter integer := 0;
            r record;
            failure_count integer := 0;
            failure_count_arr integer[];
            error_detail text;

            start_ts timestamptz;
            exec_time numeric;

            stats jsonb;
        begin
            start_ts := clock_timestamp();

            while counter < number_to_mint loop
                begin
                    insert into warehouse.identifier (identifier_set_id) values (set_id)
                    returning uuid, barcode, identifier_set_id, generated into r;

                    raise notice 'Successfully minted identifier: %; barcode: %', r.uuid, r.barcode;
                    counter := counter + 1;
                    failure_count_arr[counter] = failure_count;
                    failure_count := 0;
                    return next r;

                exception
                    when others then
                        GET STACKED DIAGNOSTICS error_detail =  PG_EXCEPTION_DETAIL;
                        raise notice '%; %', SQLERRM, error_detail;
                        failure_count := failure_count + 1;
                end;

            end loop;

            exec_time := 1000 * (extract(epoch FROM clock_timestamp() - start_ts));

            stats := jsonb_build_object('count',counter, 'exec_time', exec_time) ||
                     (select jsonb_agg(t) -> 0 from (select sum(x) as failures, max(x), mode() within group (order by x), percentile_disc(0.5) within group (order by x) as median from unnest(failure_count_arr) as x) t) as stats;

            raise notice 'id3c_minting_performance::%', stats;
        end;


    $$
    volatile
    parallel unsafe;

comment on function public.mint_identifiers(integer, integer) is
    'Generates and inserts the specified number of valid identifiers for a given identifier set.';

commit;
