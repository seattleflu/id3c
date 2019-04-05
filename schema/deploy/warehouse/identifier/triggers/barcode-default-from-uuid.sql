-- Deploy seattleflu/schema:warehouse/identifier/triggers/barcode-default-from-uuid to pg
-- requires: warehouse/identifier

begin;

-- Default barcode from uuid
create function warehouse.identifier_barcode_default_from_uuid() returns trigger as $$
    declare
        uuid text;
        barcode_length integer := 8;
    begin
        uuid := NEW.uuid::text;
        NEW.barcode := substring(uuid from length(uuid) - barcode_length + 1);
        return NEW;
    end
$$ language plpgsql;

create trigger identifier_barcode_default_from_uuid
    before insert on warehouse.identifier
    for each row
        when (NEW.barcode is null)
            execute function warehouse.identifier_barcode_default_from_uuid();

comment on function warehouse.identifier_barcode_default_from_uuid() is
    'Trigger function to default barcode from suffix of uuid (as defined by CualID)';

comment on trigger identifier_barcode_default_from_uuid on warehouse.identifier is
    'Trigger on insert to default barcode from suffix of uuid (as defined by CualID)';

commit;
