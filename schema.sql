create table incidents (
  id varchar primary key,
  incident_number int not null,
  created_at timestamptz not null,
  html_url varchar not null,
  incident_key varchar,
  service_id varchar,
  escalation_policy_id varchar,
  trigger_summary_subject varchar,
  trigger_summary_description varchar,
  trigger_type varchar not null
);

create table log_entries (
  id varchar primary key,
  type varchar not null,
  created_at timestamptz not null,
  incident_id varchar not null,
  agent_type varchar,
  agent_id varchar,
  channel_type varchar,
  user_id varchar,
  notification_type varchar,
  assigned_user_id varchar
);

create table services (
  id varchar primary key,
  name varchar not null,
  status varchar not null,
  type varchar not null
);

create table escalation_policies (
  id varchar primary key,
  name varchar not null,
  num_loops int not null
);

create table escalation_rules (
  id varchar primary key,
  escalation_policy_id varchar not null,
  escalation_delay_in_minutes int,
  level_index int
);

create table escalation_rule_users (
  id varchar primary key,
  escalation_rule_id varchar not null,
  user_id varchar
);

create table escalation_rule_schedules (
  id varchar primary key,
  escalation_rule_id varchar not null,
  schedule_id varchar
);

create table schedules (
  id varchar primary key,
  name varchar not null
);

create table users (
  id varchar primary key,
  name varchar not null,
  email varchar not null
);

create table user_schedule (
  id varchar primary key,
  user_id varchar,
  schedule_id varchar
);

-- Extension tablefunc enables crosstabs.
create extension tablefunc;
