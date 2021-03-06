-- Deploy seattleflu/schema:warehouse/sample/null-identifiers-constraint to pg
-- requires: warehouse/sample
-- requires: warehouse/sample/collection-identifier
-- requires: warehouse/sample/encounter-fk

begin;

alter table warehouse.sample
    drop constraint sample_identifiers_not_null,
    add constraint sample_identifiers_not_null check (
        identifier is not null
        or (identifier is null
            and (collection_identifier is not null
                 and encounter_id is not null)));

commit;
