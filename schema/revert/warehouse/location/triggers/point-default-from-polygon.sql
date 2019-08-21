-- Revert seattleflu/schema:warehouse/location/triggers/point-default-from-polygon from pg

begin;

drop trigger location_point_default_from_polygon on warehouse.location;
drop function warehouse.location_point_default_from_polygon();

commit;
