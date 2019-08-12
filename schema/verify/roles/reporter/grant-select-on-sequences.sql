-- Verify seattleflu/schema:roles/reporter/grant-select-on-sequences on pg

begin;

set local role id3c;

create temporary sequence __new_sequence_for_testing__;

do $$ begin
    -- Check on existing sequences
    assert pg_catalog.has_sequence_privilege('reporter', 'receiving.enrollment_enrollment_id_seq', 'select'),
        'select on receiving.enrollment_enrollment_id_seq';

    assert not pg_catalog.has_sequence_privilege('reporter', 'receiving.enrollment_enrollment_id_seq', 'usage'),
        'usage on receiving.enrollment_enrollment_id_seq';

    assert not pg_catalog.has_sequence_privilege('reporter', 'receiving.enrollment_enrollment_id_seq', 'update'),
        'update on receiving.enrollment_enrollment_id_seq';

    -- Check on new (temporary) sequence
    assert pg_catalog.has_sequence_privilege('reporter', '__new_sequence_for_testing__', 'select'),
        'select on __new_sequence_for_testing__';

    assert not pg_catalog.has_sequence_privilege('reporter', '__new_sequence_for_testing__', 'usage'),
        'usage on __new_sequence_for_testing__';

    assert not pg_catalog.has_sequence_privilege('reporter', '__new_sequence_for_testing__', 'update'),
        'update on __new_sequence_for_testing__';
end $$;

rollback;
