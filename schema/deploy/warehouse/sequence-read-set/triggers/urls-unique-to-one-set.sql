-- Deploy seattleflu/schema:warehouse/sequence-read-set/triggers/urls-unique-to-one-set to pg
-- requires: warehouse/sequence-read-set

begin;

create or replace function warehouse.sequence_read_set_urls_unique_to_one_set_check() returns trigger as $$
    declare
        overlap_urls json;
        old_urls text[];
    begin
        old_urls := case TG_OP when 'UPDATE' then OLD.urls end;

        raise debug 'Checking new urls "%" do not exist within existing sequence read set',
            NEW.urls;

        -- Lock the table, ensuring that the set of existing sequence read sets is stable.
        lock table warehouse.sequence_read_set in exclusive mode;

        -- If updating, remove old urls from new url to not overlap with itself
        if old_urls is not null then
            NEW.urls := array(select unnest(NEW.urls) except select unnest(old_urls));
        end if;

        overlap_urls := json_object_agg(id, url_overlap) from(
            select sequence_read_set.sequence_read_set_id as id, array(select unnest(NEW.urls) intersect select unnest(urls)) as url_overlap
               from warehouse.sequence_read_set
             where NEW.urls && urls
        ) as item;

        if overlap_urls is not null then
            raise unique_violation using
                message = format(
                    '%s of sequence read set urls failed unique to one set constraint',
                        TG_OP),

                detail = format(
                    'The following urls were found in existing sequence read sets (sequence_read_set_id:[urls]): %s',
                        overlap_urls),

                schema = TG_TABLE_SCHEMA,
                table  = TG_TABLE_NAME,
                column = 'urls',
                constraint = TG_NAME;
        else
            return NEW;
        end if;
    end
$$ language plpgsql
    security definer
    SET search_path = pg_catalog, public, pg_temp; -- tests/search-path: ignore

create trigger sequence_read_set_urls_unique_to_one_set_check_before_insert
    before insert on warehouse.sequence_read_set
    for each row
        execute procedure warehouse.sequence_read_set_urls_unique_to_one_set_check();

create trigger sequence_read_set_urls_unique_to_one_set_check_before_update
    before update on warehouse.sequence_read_set
    for each row
        when (NEW.urls is distinct from OLD.urls)
            execute procedure warehouse.sequence_read_set_urls_unique_to_one_set_check();

commit;
