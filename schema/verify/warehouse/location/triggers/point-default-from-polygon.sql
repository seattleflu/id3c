-- Verify seattleflu/schema:warehouse/location/triggers/point-default-from-polygon on pg

begin;

do $$
declare
    computed_point geometry(point, 4326);
begin
    insert into warehouse.location (scale, identifier, polygon)
    values ('nation', 'Null Island', st_setsrid(st_multi(st_geomfromtext('polygon((0 0, 45 45, 90 90, 0 0))')), 4326))
    returning point into strict computed_point;

    assert computed_point is not null, 'point was not computed by trigger';
end
$$;

rollback;
