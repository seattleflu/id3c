-- Deploy seattleflu/schema:warehouse/identifier/triggers/barcode-distance-check to pg
-- requires: warehouse/identifier
-- requires: functions/hamming_distance

begin;

set local role id3c;

-- This is the core of our CualID implementation, akin to running:
--
--    cual-id create ids --existing-ids ...
--
create or replace function warehouse.identifier_barcode_distance_check() returns trigger as $$
    declare
        minimum_distance integer := 3;
        conflicting_barcodes citext[];
        old_barcode citext;
    begin
        old_barcode := case TG_OP when 'UPDATE' then OLD.barcode end;

        raise debug 'Checking new barcode "%" is at least % substitutions away from existing barcodes (except old barcode "%")',
            NEW.barcode, minimum_distance, old_barcode;

        -- Lock the table, ensuring that the set of existing identifiers is
        -- stable, in order to avoid race conditions that allow distance
        -- violations.  An exclusive lock lets normal SELECT queries read the
        -- identifier table but serializes other invocations of this function.
        lock table warehouse.identifier in exclusive mode;

        select array_agg(barcode) into strict conflicting_barcodes from (
            select barcode
              from warehouse.identifier
             where hamming_distance_ci(barcode, NEW.barcode) < minimum_distance
            except
            select old_barcode
        ) x;

        if array_length(conflicting_barcodes, 1) > 0 then
            -- A detailed exception is much nicer for handling in identifer
            -- minting routines.
            raise exclusion_violation using
                message = format(
                    '%s of (barcode=%L) excluded by minimum substitution distance check',
                        TG_OP, NEW.barcode),

                detail = format(
                    'Barcode %L is closer than %s substitutions away from existing barcodes %s',
                        NEW.barcode, minimum_distance, conflicting_barcodes),

                hint = format(
                    'If the %s relied on default values, retrying it will use a new value and possibly succeed',
                        TG_OP),

                schema = TG_TABLE_SCHEMA,
                table  = TG_TABLE_NAME,
                column = 'barcode',
                constraint = TG_NAME;
        else
            return NEW;
        end if;
    end
$$ language plpgsql;

drop trigger identifier_barcode_distance_check_before_insert on warehouse.identifier;
drop trigger identifier_barcode_distance_check_before_update on warehouse.identifier;

create trigger identifier_barcode_distance_check_before_insert
    before insert on warehouse.identifier
    for each row
        execute procedure warehouse.identifier_barcode_distance_check();

create trigger identifier_barcode_distance_check_before_update
    before update of barcode on warehouse.identifier
    for each row
        when (NEW.barcode is distinct from OLD.barcode)
            execute procedure warehouse.identifier_barcode_distance_check();

comment on function warehouse.identifier_barcode_distance_check() is
    'Trigger function to exclude new identifiers with barcodes too close to existing barcodes';

comment on trigger identifier_barcode_distance_check_before_insert on warehouse.identifier is
    'Trigger to exclude inserted identifiers with barcodes too close to existing barcodes';

comment on trigger identifier_barcode_distance_check_before_update on warehouse.identifier is
    'Trigger to exclude updated identifiers with barcodes too close to existing barcodes';

commit;
