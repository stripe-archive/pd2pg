-- Compute escalation rate per service over the last 28 days.

with incident_counts as (
select
  incidents.id as incident_id,
  incidents.service_id,
  (select
     count(*)
   from
     log_entries
   where
     log_entries.incident_id = incidents.id and
     log_entries.type = 'escalate'
  ) as escalations
from
  incidents
where
  incidents.created_at > now()-'28 days'::interval
group by
  incident_id,
  service_id
),

counts as (
select
  incident_counts.service_id,
  count(incident_counts.incident_id) as incidents,
  sum(incident_counts.escalations) as escalations
from
  incident_counts
group by
  incident_counts.service_id
)

select
  services.name as service,
  round(counts.escalations / counts.incidents, 1) as escalation_rate,
  counts.incidents as incidents,
  counts.escalations as escalations
from
  services,
  counts
where
  services.id = counts.service_id
order by
  escalation_rate desc
;
