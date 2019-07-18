-- Deploy seattleflu/schema:warehouse/sequence-read-set/triggers/urls-not-null-check to pg
-- requires: warehouse/sequence-read-set

begin;

create or replace function warehouse.sequence_read_set_urls_not_null_check() returns trigger as $$
    declare
        contains_null boolean := false;
    begin
        raise debug 'Checking new urls "%" are not null',
            NEW.urls;

        -- Determine if null is contained in the array of urls
        select bool_or(a is null) into contains_null
          from unnest(NEW.urls) s(a);

        if contains_null then
            raise not_null_violation using
                message = format(
                    '%s of sequence read set urls failed not null constraint',
                        TG_OP),

                detail = format(
                    'Null values found in sequence read set urls: %s',
                        NEW.urls),

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
    -- Explicitly restrict which schemas the code inside the function can find
    -- other tables, functions, etc. without qualification
    SET search_path = pg_catalog, public, pg_temp;

create trigger sequence_read_set_urls_not_null_check_before_insert
    before insert on warehouse.sequence_read_set
    for each row
        execute procedure warehouse.sequence_read_set_urls_not_null_check();

create trigger sequence_read_set_urls_not_null_check_before_update
    before update on warehouse.sequence_read_set
    for each row
        when (NEW.urls is distinct from OLD.urls)
            execute procedure warehouse.sequence_read_set_urls_not_null_check();

comment on function warehouse.sequence_read_set_urls_not_null_check() is
    'Trigger function to check for null values within urls array';

comment on trigger sequence_read_set_urls_not_null_check_before_insert on warehouse.sequence_read_set is
    'Trigger to prevent insertion of urls with null values';

comment on trigger sequence_read_set_urls_not_null_check_before_update on warehouse.sequence_read_set is
    'Trigger to prevent update of urls with null values';

commit;
