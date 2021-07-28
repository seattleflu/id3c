-- Revert seattleflu/schema:warehouse/identifier-set-use/data from pg

begin;

delete from warehouse.identifier_set_use where use in (
    'sample',
    'collection',
    'clia',
    'kit',
    'test-strip'
);

commit;
