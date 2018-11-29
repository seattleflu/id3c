-- This file must run as a Pg superuser.

-- The default public schema from template0 is owned by the default Pg
-- superuser and we want to own it so our owner has full control after this.
alter schema public owner to :owner;

-- This works *iff* we're the superuser or the owner of the public schema.
-- Otherwise, it quietly does nothing!
revoke create on schema public from public;
