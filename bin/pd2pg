#!/usr/bin/env ruby

require "json"
require "excon"
require "sequel"

# Ensure all data processing and storage is in UTC.
ENV["TZ"] = "UTC"

class PG2PD
  # Largest page size allowed.
  PAGINATION_LIMIT = 100

  # Rewind by ~1h when doing incremental updates, to ensure we don't
  # miss anything.
  INCREMENTAL_BUFFER = 60*60

  # Apply incremental updates atomically for ~24 hour windows, instead
  # of trying to fetch all of history and apply it at once.
  INCREMENTAL_WINDOW = 60*60*24

  # Earliest time PagerDuty data could be available.
  PAGERDUTY_EPOCH = Time.parse("2009-01-01T00:00Z")

  # Reads required config from environment variables.
  def env!(k)
    v = ENV[k]
    if !v
      $stderr.puts("Must set #{k} in environment")
      Kernel.exit(1)
    end
    v
  end

  # Logs a key-prefixed, name=value line.
  def log(key, data={})
    data_str = data.map { |(k,v)| "#{k}=#{v}" }.join(" ")
    $stdout.puts("#{key}#{" " + data_str if data_str}")
  end

  # Connections to the Postgres DB and PagerDuty API respectively.
  attr_accessor :db, :api

  # Initialize by reading config and establishing API and DB
  # connections.
  def initialize
    # Read config.
    database_url = env!("DATABASE_URL")
    pagerduty_api_token = env!("PAGERDUTY_API_KEY")

    # Establish API connection.
    self.api = Excon::Connection.new(
      :scheme => "https",
      :host => "api.pagerduty.com",
      :port => 443,
      :headers => {
          "Authorization" => "Token token=#{pagerduty_api_token}",
          "Accept" => "application/vnd.pagerduty+json;version=2"
      })

    # Establish DB connection.
    self.db = Sequel.connect(database_url)
  end

  # Send all service records from Pagerduty to database.
  def services_to_db(items)
    columns = [:id, :name, :status, :type]
    records = items.map do |i|
      [i['id'],
       i['name'],
       i['status'],
       i['type']]
    end
    database_replace(:services, columns, records)
  end

  # Adds users to user_schedule table associated with given schedule.
  def user_schedules_to_db(schedule_items)
    columns = [:id, :user_id, :schedule_id]
    all_records = []
    schedule_items.each do |s|
      users = get_bulk(:users,
                       "schedules/#{s['id']}/users",
                       { since: Time.now.strftime('%Y-%m-%d') },
                       false)
      users.each do |u|
        all_records << ["#{u['id']}_#{s['id']}", u['id'], s['id']]
      end
    end
    database_replace(:user_schedule, columns, all_records)
  end

  # Send all schedule records from Pagerduty to database.
  def schedules_to_db(items)
    user_schedules_to_db(items)

    columns = [:id, :name]
    records = items.map do |i|
      [i['id'],
       i['name']]
    end
    database_replace(:schedules, columns, records)
  end

  # Send all escalation policy records from Pagerduty to database.
  def escalation_policies_to_db(items)
    ep_columns = [:id, :name, :num_loops]
    ep_records = items.map do |i|
      [i['id'],
       i['name'],
       i['num_loops']]
    end

    er_columns = [:id, :escalation_policy_id, :escalation_delay_in_minutes, :level_index]
    eru_columns = [:id, :escalation_rule_id, :user_id]
    ers_columns = [:id, :escalation_rule_id, :schedule_id]
    er_records = []
    eru_records = []
    ers_records = []
    items.each do |ep|
      ep['escalation_rules'].each.with_index do |er, i|

        er_records << [
          er['id'],
          ep['id'],
          er['escalation_delay_in_minutes'],
          i + 1
        ]
        er['targets'].each do |t|
          if t['type'] == 'user'
            eru_records << [
              "#{er['id']}_#{t['id']}",
              er['id'],
              t['id']
            ]
          else
            ers_records << [
              "#{er['id']}_#{t['id']}",
              er['id'],
              t['id']
            ]
          end
        end
      end
    end
    database_replace(:escalation_rules, er_columns, er_records)
    database_replace(:escalation_rule_users, eru_columns, eru_records)
    database_replace(:escalation_rule_schedules, ers_columns, ers_records)
    database_replace(:escalation_policies, ep_columns, ep_records)
  end

  # Send all user records to database.
  def users_to_db(items)
    columns = [:id, :name, :email]
    records = items.map do |i|
      [i['id'],
       i['name'],
       i['email']]
    end
    database_replace(:users, columns, records)
  end

  # Convert log entry API value into a DB record.
  def convert_log_entry(le)
    {
      id: le["id"],
      type: le["type"],
      created_at: Time.parse(le["created_at"]),
      incident_id: le["incident"]["id"],
      agent_type: le["agent"] && le["agent"]["type"],
      agent_id: le["agent"] && le["agent"]["id"],
      channel_type: le["channel"] && le["channel"]["type"],
      user_id: le["user"] && le["user"]["id"],
      notification_type: le["notification"] && le["notification"]["type"],
      assigned_user_id: le["assigned_user"] && le["assigned_user"]["id"]
    }
  end

  # Convert incident API value into a DB structure.
  def convert_incident(i)
    {
      id: i["id"],
      incident_number: i["incident_number"],
      created_at: i["created_at"],
      html_url: i["html_url"],
      incident_key: i["incident_key"],
      service_id: i["service"] && i["service"]["id"],
      escalation_policy_id: i["escalation_policy"] && i["escalation_policy"]["id"],
      trigger_summary_subject: i["summary"],
      trigger_summary_description: i["description"],
      # this column is no longer supported in Pagerduty's v2 API
      # we're leaving it in for backwards compatibility.
      trigger_type: "DEPRECATED"
    }
  end

  # Returns list of raw values from the database for the given collection or endpoint.
  def get_bulk(collection, endpoint=nil, additional_headers={}, should_log=true)
    if endpoint.nil?
      endpoint = collection
    end

    offset = 0
    total = nil
    records = []
    loop {
      if should_log
        log('get_bulk.page', collection: collection, offset: offset, total: total || '?')
      end
      response = api.request(
        method: :get,
        path: "/#{endpoint}",
        query: {
            'total' => true,
            'offset' => offset,
            'limit' => PAGINATION_LIMIT
        }.merge(additional_headers),
        expects: [200]
      )
      data = JSON.parse(response.body)
      total = data["total"] || data[collection.to_s].length
      offset += PAGINATION_LIMIT
      records.concat(data[collection.to_s])
      break if !data["more"]
    }
    if should_log
      log('get_bulk.update', collection: collection, total: records.length)
    end
    return records
  end

  def database_replace(table_name, columns, records)
    # Atomically update the given table. Deletes the contents of the table before inserting the new records.
    db.transaction do
      table = db[table_name]
      table.delete()
      records.each do |record|
        table.insert(record)
      end
    end
  end

  # Update database state for the given table by fetching relevant new
  # values from the API. Determine point from which to resume based
  # on existing records, which is assumed to be complete up to the most
  # recent record. Yields each API value to a block that should convert
  # the API value data to a DB record for subseqent insertion.
  def refresh_incremental(collection, query_params={})
    # Calculate the point from which we should resume incremental
    # updates. Allow a bit of overlap to ensure we don't miss anything.
    last_record = db[collection].reverse_order(:created_at).first
    latest = (last_record && last_record[:created_at]) || PAGERDUTY_EPOCH
    log("refresh_incremental.check", collection: collection, latest: latest.iso8601)

    # Update data in windowed time chunks. This will give us manageable
    # amounts of data request from the API coherently.
    since = latest - INCREMENTAL_BUFFER
    while since < Time.now
      through = since + INCREMENTAL_WINDOW
      log("refresh_incremental.window", collection: collection, since: since.iso8601, through: through.iso8601)

      # Fetch all values from the API and apply the conversion block to
      # each, forming an in-memory array of DB-ready records.
      offset = 0
      total = nil
      records = []
      loop {
        log("refresh_incremental.page", collection: collection, since: since, offset: offset, total: total || "?")
        response = api.request(
          method: :get,
          path: "/#{collection}",
          query: {
            total: true,
            since: since,
            until: through,
            offset: offset,
            limit: PAGINATION_LIMIT
          }.merge(query_params),
          expects: [200]
        )
        data = JSON.parse(response.body)
        total = data["total"]
        offset += PAGINATION_LIMIT
        items = data[collection.to_s]
        records.concat(items.map { |i| yield(i) })
        break if !data["more"]
      }

      # Atomically update the DB by inserting all novel records.
      if !records.empty?
        log("refresh_incremental.update", total: records.length)
        db.transaction do
          table = db[collection]
          records.each do |record|
            if table.where(id: record[:id]).empty?
              table.insert(record)
            end
          end
        end
      end

      since = through
    end
  end

  # Refresh data for all tracked tables.
  def refresh
    log("refresh.start")

    services_to_db(get_bulk(:services))
    escalation_policies_to_db(get_bulk(:escalation_policies))
    schedules_to_db(get_bulk(:schedules))
    users_to_db(get_bulk(:users))

    refresh_incremental(:incidents) { |i| convert_incident(i) }
    refresh_incremental(:log_entries, "include[]" => "incident") { |le| convert_log_entry(le) }

    log("refresh.finish")
  end
end

PG2PD.new.refresh
