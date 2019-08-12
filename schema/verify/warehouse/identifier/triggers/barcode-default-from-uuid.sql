-- Verify seattleflu/schema:warehouse/identifier/triggers/barcode-default-from-uuid on pg

begin;

set local role id3c;

do $$
declare
    testing_set integer;
begin
    insert into warehouse.identifier_set (name) values ('testing')
        returning identifier_set_id into strict testing_set;

    -- Table and column constraints on warehouse.identifier should prevent this
    -- from succeeding if the trigger doesn't work or produces a static value or
    -- something weird.
    insert into warehouse.identifier (identifier_set_id) values (testing_set);
    insert into warehouse.identifier (identifier_set_id) values (testing_set);
end
$$;

rollback;
