-- Show details about each incident that either originated form one of
-- the team's services or alerted a member of the team (potentially
-- coming from other services).

with team_notified_incident_ids as (
select
  distinct log_entries.incident_id as incident_id
from
  incidents,
  log_entries,
  users
where
  incidents.created_at > now() - '7 days'::interval and
  incidents.id = log_entries.incident_id and
  log_entries.type = 'notify' and
  log_entries.user_id = users.id and
  substring(users.email from '(.+)@stripe.com') in (
    'amy',
    'fred',
    'neil',
    'susan'
  )
),

team_originating_incident_ids as (
select
  distinct incidents.id as incident_id
from
  incidents,
  services
where
  incidents.created_at > now() - '7 days'::interval and
  incidents.service_id = services.id and
  services.name in (
    'Team API',
    'Team Pingdom',
    'Team 911'
  )
),

team_incident_ids as (
select * from team_notified_incident_ids union
select * from team_originating_incident_ids
),

escalation_counts as (
select
  team_incident_ids.incident_id,
  (select count(*)
   from log_entries
   where log_entries.incident_id = team_incident_ids.incident_id and
         log_entries.type = 'escalate'
  ) as num_escalations
from
  team_incident_ids
)

select
  substring(incidents.html_url from 9) as url,
  substring(coalesce(incidents.trigger_summary_subject, incidents.trigger_summary_description, '') for 100) as trigger_summary,
  incidents.created_at at time zone 'America/Los_Angeles' as created_at,
  substring(users.email from '(.+)@stripe.com') as resolved_by,
  services.name as service,
  escalation_policies.name as escalation_policy,
  escalation_counts.num_escalations as escalations
from 
  incidents
left outer join
  log_entries on incidents.id = log_entries.incident_id and log_entries.type = 'resolve'
left outer join
  users on log_entries.agent_id = users.id
left outer join
  services on incidents.service_id = services.id
left outer join
  escalation_policies on incidents.escalation_policy_id = escalation_policies.id
left outer join
  escalation_counts on incidents.id = escalation_counts.incident_id
where
  incidents.id in (select * from team_incident_ids)
order by
  incidents.created_at asc
;
