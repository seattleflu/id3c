-- Drop privileges from superuser early so that all subsequent DDL produces
-- objects with our desired owner.
set role :owner;
