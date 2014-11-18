-- Show details about an individual's notifications over the past week.
-- Note that the user's email is in the query.

select
  incidents.html_url as incident_url,
  log_entries.created_at as notification_time,
  log_entries.notification_type as notification_type,
  incidents.trigger_summary_subject,
  services.name as service_name
from
  users,
  log_entries,
  incidents,
  services
where
  users.email = 'mark@stripe.com' and
  log_entries.user_id = users.id and
  log_entries.type = 'notify' and
  log_entries.created_at > now() - '1 week'::interval and
  incidents.id = log_entries.incident_id and
  incidents.service_id = services.id
order by
  log_entries.created_at desc
;
