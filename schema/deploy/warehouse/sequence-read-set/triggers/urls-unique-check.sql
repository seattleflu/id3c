-- Deploy seattleflu/schema:warehouse/sequence-read-set/triggers/urls_unique_check to pg
-- requires: warehouse/sequence-read-set

begin;

create or replace function warehouse.sequence_read_set_urls_unique_check() returns trigger as $$
    declare
        duplicated_urls text[];
    begin
        raise debug 'Checking new urls "%" are unique',
            NEW.urls;

        -- Determine if null is contained in the array of urls
        select array_agg(url) into duplicated_urls from (
            select url
              from unnest(NEW.urls) as url
             group by url
             having count(*) > 1
        ) s;

        if array_length(duplicated_urls, 1) > 0 then
            raise unique_violation using
                message = format(
                    '%s of sequence read set urls failed unique constraint',
                        TG_OP),

                detail = format(
                    'Duplicate values found in sequence read set urls: %s',
                        duplicated_urls),

                schema = TG_TABlE_SCHEMA,
                table  = TG_TABLE_NAME,
                column = 'urls',
                constraint = TG_NAME;
        else
            return NEW;
        end if;
    end
$$ language plpgsql
    security definer
    SET search_path = pg_catalog, public, pg_temp;

create trigger sequence_read_set_urls_unique_check_before_insert
    before insert on warehouse.sequence_read_set
    for each row
        execute procedure warehouse.sequence_read_set_urls_unique_check();

create trigger sequence_read_set_urls_unique_check_before_update
    before update on warehouse.sequence_read_set
    for each row
        when (NEW.urls is distinct from OLD.urls)
            execute procedure warehouse.sequence_read_set_urls_unique_check();

comment on function warehouse.sequence_read_set_urls_unique_check() is
    'Trigger function to check for duplicated values within urls array';

comment on trigger sequence_read_set_urls_unique_check_before_insert on warehouse.sequence_read_set is
    'Trigger to prevent insertion of urls with duplicated values';

comment on trigger sequence_read_set_urls_unique_check_before_update on warehouse.sequence_read_set is
    'Trigger to prevent update of urls with duplicated values';

commit;
