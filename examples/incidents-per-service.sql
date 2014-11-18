-- Count incidents per service over the last 28 days.
select
  services.name,
  count(incidents.id)
from
  incidents,
  services
where
  incidents.created_at > now() - '28 days'::interval and
  incidents.service_id = services.id
group by
  services.name
order by
  count(incidents.id) desc
;
