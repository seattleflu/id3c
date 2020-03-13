-- Deploy seattleflu/schema:warehouse/encounter-location-relation/data to pg
-- requires: warehouse/encounter-location-relation

begin;

/* While the most important encounter location will be different for different
 * analyses, a global default is useful for programatically choosing a single
 * location from many.  Based on the Seattle Flu Study, where a person lives
 * (their residence or temporary lodging) is most requested.  If we don't have
 * that, then a workplace or school, which tend to be mutually exclusive, is
 * better.  Both are places where participants spend lots of their time.
 * Finally, the site of collection is better than nothing.  (It's assumed that
 * collection sites are small in number and act as concentrators of
 * participants.)
 *
 * While a global default is useful, the most important thing is that this
 * table allows customization for each ID3C instance.
 */
insert into warehouse.encounter_location_relation
    values
        ('residence', 1),
        ('lodging', 2),
        ('workplace', 3),
        ('school', 4),
        ('site', 5)
    on conflict (relation) do update set
        priority = excluded.priority
;

commit;
