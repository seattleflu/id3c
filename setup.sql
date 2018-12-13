begin;

revoke connect on database template1 from public;
revoke connect on database postgres from public;

commit;
