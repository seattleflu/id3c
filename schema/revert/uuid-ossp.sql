-- Revert seattleflu/schema:uuid-ossp from pg

begin;

set local role id3c;

drop extension "uuid-ossp";

commit;
