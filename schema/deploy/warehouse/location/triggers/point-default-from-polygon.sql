-- Deploy seattleflu/schema:warehouse/location/triggers/point-default-from-polygon to pg
-- requires: warehouse/location

begin;

set local search_path to warehouse, public;

-- Default point from the polygon, if any
create or replace function location_point_default_from_polygon() returns trigger as $$
    declare
        centroid geometry(point, 4326);
    begin
        -- Prefer centroid, but if it's not covered by the polygon, then
        -- use an arbitrary point guaranteed to be on the polygon's
        -- surface.
        centroid := st_centroid(coalesce(NEW.polygon, NEW.simplified_polygon));

        if st_covers(coalesce(NEW.polygon, NEW.simplified_polygon), centroid) then
            NEW.point := centroid;
        else
            NEW.point := st_pointonsurface(coalesce(NEW.polygon, NEW.simplified_polygon));
        end if;

        return NEW;
    end
$$ language plpgsql;

create trigger location_point_default_from_polygon
    before insert on location
    for each row
        when (NEW.point is null and coalesce(NEW.polygon, NEW.simplified_polygon) is not null)
            execute procedure location_point_default_from_polygon();

comment on function location_point_default_from_polygon() is
    'Trigger function to default point from polygon or simplified_polygon';

comment on trigger location_point_default_from_polygon on location is
    'Trigger on insert to default point from polygon or simplified_polygon';

commit;
