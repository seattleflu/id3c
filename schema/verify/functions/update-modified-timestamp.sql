-- Verify seattleflu/schema:functions/update-modified-timestamp on pg

begin;

do $$
    declare
        pre_update_modified timestamp;
        post_update_modified timestamp;
    begin
        create temporary table tests (
            a text,
            modified timestamp with time zone
        );

        insert into tests
            values ('test', now() - interval '1 hour')
            returning modified into strict pre_update_modified;

        create trigger test_update_trigger
            before update on tests
            for each row
                execute procedure warehouse.update_modified_timestamp();

        update tests set a = 'test_update' where a = 'test'
            returning modified into strict post_update_modified;

        assert post_update_modified - pre_update_modified = interval '1 hour';

    end
$$;


rollback;
