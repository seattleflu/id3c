-- Verify seattleflu/schema:warehouse/identifier/triggers/barcode-distance-check on pg

begin;

do $$
declare
    testing_set integer;
begin
    insert into warehouse.identifier_set (name) values ('testing')
        returning identifier_set_id into strict testing_set;

    insert into warehouse.identifier (uuid, identifier_set_id)
        values ('655add66-4898-4105-be09-0d1b00000000'::uuid, testing_set);

    begin
        insert into warehouse.identifier (uuid, identifier_set_id)
            values ('655add66-4898-4105-be09-0d1b00000012'::uuid, testing_set);
        assert false, 'too similar barcode allowed';
    exception
        when exclusion_violation then
            null; -- expected
    end;

    insert into warehouse.identifier (uuid, identifier_set_id)
        values ('655add66-4898-4105-be09-0d1b00000123'::uuid, testing_set);

    -- Too close to existing other barcode
    begin
        update warehouse.identifier
           set uuid = '655add66-4898-4105-be09-0d1b00000012'::uuid,
               barcode = '00000012'
         where barcode = '00000123';
        assert false, 'too similar barcode allowed';
    exception
        when exclusion_violation then
            null; -- expected
    end;

    -- Close to itself (ok), but far enough from existing other barcode
    update warehouse.identifier
       set uuid = '655add66-4898-4105-be09-0d1b00000124'::uuid,
           barcode = '00000124'
     where barcode = '00000123';
end
$$;

rollback;
