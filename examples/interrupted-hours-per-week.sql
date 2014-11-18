-- Show the number of interrupted hours per escalation policy, per-week.
-- In the result set, weeks are rows and there's one column per
-- escalation policy. Note that escalation policies are whitelisted in
-- the query.

select * from crosstab(

-- Data: {week, escalation policy name, interrupted hours count}.
-- Interrupted hours are defined as {clock hour, person} tuples for
-- which the person received >= 1 notification during the clock hour.
-- Counts are per escalation-policy, so if 1 person receives pages for
-- 2 EPs during the same hour that will count as 2 interrupted hours.
$SQL$
with notifications as (
select
  log_entries.created_at,
  log_entries.user_id,
  log_entries.incident_id
from
  log_entries
where
  type = 'notify'
),

interruptions as (
select
  date_trunc('hour', notifications.created_at) as hour,
  incidents.escalation_policy_id as escalation_policy_id,
  count(distinct notifications.user_id) as interrupted_users
from
  notifications,
  incidents
where
  notifications.incident_id = incidents.id
group by
  hour,
  escalation_policy_id
)

select
  to_char(date_trunc('week', interruptions.hour), 'YYYY-MM-DD') as week,
  escalation_policies.name as escalation_policy_name,
  sum(interruptions.interrupted_users) as interrupted_hours
from
  interruptions,
  escalation_policies
where
  interruptions.escalation_policy_id = escalation_policies.id
group by
  week,
  escalation_policy_name
order by
  week desc
$SQL$,

-- Escalation policy names.
$SQL$
select unnest(array[
  'Systems',
  'Databases',
  'API'
])
$SQL$

-- Column definitions: week + escalation policy names.
) as ct(
  week text,
  systems int,
  databases int,
  api int
)
;
