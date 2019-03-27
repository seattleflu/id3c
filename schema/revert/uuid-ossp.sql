-- Revert seattleflu/schema:uuid-ossp from pg

begin;

drop extension "uuid-ossp";

commit;
