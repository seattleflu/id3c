-- Deploy seattleflu/schema:functions/update-modified-timestamp to pg

begin;

create or replace function warehouse.update_modified_timestamp() returns trigger as $$

    begin
        NEW.modified = now();
        return new;
    end;
$$ language plpgsql
    stable;

comment on function warehouse.update_modified_timestamp is
    'Updates the modified column timestamp with the current time on every update';

commit;
