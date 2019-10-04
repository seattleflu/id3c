-- Revert seattleflu/schema:functions/update-modified-timestamp from pg

begin;

drop function warehouse.update_modified_timestamp();

commit;
