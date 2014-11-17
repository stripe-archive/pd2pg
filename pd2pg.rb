require "json"
require "excon"
require "time"
require "pg"
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
    pagerduty_subdomain = env!("PAGERDUTY_SUBDOMAIN")
    pagerduty_api_token = env!("PAGERDUTY_API_TOKEN")

    # Establish API connection.
    self.api = Excon::Connection.new(
      :scheme => "https",
      :host => "#{pagerduty_subdomain}.pagerduty.com",
      :port => 443,
      :headers => {"Authorization" => "Token token=#{pagerduty_api_token}"})

    # Establish DB connection.
    self.db = Sequel.connect(database_url)
  end

  # Convert service API value into a DB record.
  def convert_service(s)
    {
      id: s["id"],
      name: s["name"],
      status: s["status"],
      type: s["type"]
    }
  end

  # Convert escalation policy API value into a DB record.
  def convert_escalation_policy(ep)
    {
      id: ep["id"],
      name: ep["name"]
    }
  end

  # Convert user API value into a DB record.
  def convert_user(u)
    {
      id: u["id"],
      name: u["name"],
      email: u["email"]
    }
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
      created_at: i["created_on"],
      html_url: i["html_url"],
      incident_key: i["incident_key"],
      service_id: i["service"] && i["service"]["id"],
      escalation_policy_id: i["escalation_policy"] && i["escalation_policy"]["id"],
      trigger_summary_subject: i["trigger_summary_data"]["subject"],
      trigger_summary_description: i["trigger_summary_data"]["description"],
      trigger_type: i["trigger_type"]
    }
  end

  # Refresh database state for the given table by fetching all relevant
  # values from the API. Yields each API value to a block that should
  # convert the API value to a DB record for subsequent insertion /
  # update.
  def refresh_bulk(collection)
    # Fetch all values from the API and apply the conversion block to
    # each, forming an in-memory array of DB-ready records.
    offset = 0
    total = nil
    records = []
    while !total || offset <= total
      log("refresh_bulk.page", collection: collection, offset: offset, total: total || "?")
      response = api.request(
        :method => :get,
        :path => "/api/v1/#{collection}",
        :query => {"offset" => offset, "limit" => PAGINATION_LIMIT},
        :expects => [200]
      )
      data = JSON.parse(response.body)
      total = data["total"]
      offset = offset + PAGINATION_LIMIT
      items = data[collection.to_s]
      records.concat(items.map { |i| yield(i) })
    end

    # Atomically update the DB, handling both data changes and
    # insertions.
    log("refresh_bulk.update", total: records.length)
    db.transaction do
      table = db[collection]
      records.each do |record|
        dataset = table.where(id: record[:id])
        if dataset.empty?
          table.insert(record)
        else
          dataset.update(record)
        end
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
      while !total || offset <= total
        log("refresh_incremental.page", collection: collection, offset: offset, total: total || "?")
        response = api.request(
          :method => :get,
          :path => "/api/v1/#{collection}",
          :query => {
            "since" => since,
            "until" => through,
            "offset" => offset,
            "limit" => PAGINATION_LIMIT
          }.merge(query_params),
          :expects => [200]
        )
        data = JSON.parse(response.body)
        total = data["total"]
        offset = offset + PAGINATION_LIMIT
        items = data[collection.to_s]
        records.concat(items.map { |i| yield(i) })
      end

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

    refresh_bulk(:services) { |s| convert_service(s) }
    refresh_bulk(:escalation_policies) { |ep| convert_escalation_policy(ep) }
    refresh_bulk(:users) { |u| convert_user(u) }

    refresh_incremental(:log_entries, {"include[]" => "incident"}) { |le| convert_log_entry(le) }
    refresh_incremental(:incidents) { |i| convert_incident(i) }

    log("refresh.finish")
  end
end

PG2PD.new.refresh
