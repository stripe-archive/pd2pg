# pd2pg

pd2pg imports data from the PagerDuty API into a Postgres database for
easy querying and analysis.

It helps you:

* Collect summary statistics about on-call activity.
* Calculate per-user, per-service, per-escalation-policy on-call metrics.
* Determine the frequency of on-hours vs. off-hours pages.
* Produce custom on-call reports with incident-level detail.
* Back-test proposed on-call changes.
* Perform one-off queries against historical pager data.

## Importing

pd2pg imports user, service, escalation policy, incident, and log entry
(incident event) data from the PagerDuty API into a specified Postgres
database. The import is incremental and idempotent, so you can run it as
often as you'd like to refresh your database.

You'll need the following config set in environment variables:

* `PAGERDUTY_API_KEY`: a read-only API key from `https://api.pagerduty.com/api_keys`.
* `DATABASE_URL`: URL to a Postgres database, e.g. `postgres://127.0.0.1:5432/pagerduty`

Perform a one-time schema load with:

```
$ psql $DATABASE_URL < schema.sql
```

Then refresh the database as often as desired with:

```
$ bundle exec pd2pg
```

## Querying

pd2pg makes PagerDuty data available as regular Postgres data, so you
can query it in the usual way, e.g. with:

```
$ psql $DATABASE_URL
```

For example, to count the number of incidents per service over the past
28 days:

```sql
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
```

Or show all incidents that notified a specific user over the past week:

```sql
select
  log_entries.created_at as notification_time,
  incidents.html_url as incident_url,
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
  log_entries.created_at > now() - '7 days'::interval and
  incidents.id = log_entries.incident_id and
  incidents.service_id = services.id
order by
  incidents.created_at desc
```

See `schema.sql` for details of the data model and `examples/` for
example SQL queries.
