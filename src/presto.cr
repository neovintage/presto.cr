require "db"
require "http/client"
require "json"
require "uri"
require "./presto/*"

module Presto
  DEFAULT_HEADERS = HTTP::Headers{
    "User-Agent" => "presto-crystal/#{VERSION}",
  }

  class Connection < ::DB::Connection
    protected getter connection

    # todo throw error if username isnt defined. that's required
    def initialize(context)
      super(context)

      context.uri.scheme = set_scheme(context.uri)

      @connection = HTTP::Client.new(context.uri)
      @connection.basic_auth(context.uri.user, context.uri.password)

      @options = HTTP::Headers.new

      # todo need to have defaults that the user can then override
      @connection.before_request do |request|
        request.headers["User-Agent"] = "presto-crystal"
      end
    end

    def http_uri
      @context.uri
    end

    def build_unprepared_statement(query) : Statement
      Statement.new(self, query)
    end

    def build_prepared_statement(query) : Statement
      Statement.new(self, query)
    end

    private def set_scheme(uri)
      use_ssl = uri.query_params["SSL"]?
      if use_ssl == "true"
        return "https"
      end
      return "http"
    end
  end

  class Statement < ::DB::Statement
    PRESTO_HEADERS = {
      "user" => "X-Presto-User",
      "source" => "X-Presto-Source",
      "catalog" => "X-Presto-Catalog",
      "path" => "X-Presto-Path",
      "time_zone" => "X-Presto-Time-Zone",
      "language" => "X-Presto-Language",
      "trace_token" => "X-Presto-Trace-Token",
      "session" => "X-Presto-Session",
      "set_catalog" => "X-Presto-Set-Catalog",
      "set_schema" => "X-Presto-Set-Schema",
      "set_path" => "X-Presto-Set-Path",
      "set_session" => "X-Presto-Set-Session",
      "clear_session" => "X-Presto-Clear-Session",
      "set_role" => "X-Presto-Set-Role",
      "role" => "X-Presto-Role",
      "prepared_statement" => "X-Presto-Prepared-Statement",
      "added_prepare" => "X-Presto-Added-Prepare",
      "deallocated_prepare" => "X-Presto-Deallocated-Prepare",
      "transaction_id" => "X-Presto-Transaction-Id",
      "started_transaction_id" => "X-Presto-Started-Transaction-Id",
      "clear_transaction_id" => "X-Presto-Clear-Transaction-Id",
      "client_info" => "X-Presto-Client-Info",
      "client_tags" => "X-Presto-Client-Tags",
      "client_capabilities" => "X-Presto-Client-Capabilities",
      "resource_estimate" => "X-Presto-Resource-Estimate",
      "extra_credential" => "X-Presto-Extra-Credential",

      "current_state" => "X-Presto-Current-State",
      "max_wait" => "X-Presto-Max-Wait",
      "max_size" => "X-Presto-Max-Size",
      "task_instance_id" => "X-Presto-Task-Instance-Id",
      "page_token" => "X-Presto-Page-Sequence-Id",
      "page_end_sequence_id" => "X-Presto-Page-End-Sequence-Id",
      "buffer_complete" => "X-Presto-Buffer-Complete",
    }

    def initialize(connection, @sql : String)
      super(connection)
    end

    protected def conn
      connection.as(Connection).connection
    end

    # todo the args in enumerable should have the options that can be overridden
    protected def perform_query(args : Enumerable) : ResultSet
      start_time = Time.monotonic
      timeout = statement_timeout
      http_response = conn.post("/v1/statement", headers: nil, body: @sql)
      json = uninitialized JSON::Any

      loop do
        json = JSON.parse(http_response.body)
        break if ((Time.monotonic - start_time) > timeout) || json["nextUri"]?.nil? || json["data"]?

        http_response = conn.get(json["nextUri"].to_s)
      end

      ResultSet.new(self, json, http_response)
    end

    protected def perform_exec(args : Enumerable) : ::DB::ExecResult
    end

    # todo enable this to be overriden by user
    private def statement_timeout
      Time::Span.new(seconds: 10, nanoseconds: 0)
    end

    private def parse_headers(options)

    end
  end

  class ResultSet < ::DB::ResultSet
    getter query_results
    getter data : JSON::Any
    getter columns : JSON::Any
    getter row_count : Int32

    def initialize(statement, @query_results : JSON::Any, response : HTTP::Client::Response)
      super(statement)
      @column_index = -1
      @row_index = -1

      @data = @query_results["data"]? || JSON.parse("[]")
      @columns = @query_results["columns"]? || JSON.parse("[]")
      @row_count = @data.size

      @http_response = response

      # todo parse the columns for the data types into hash to make type conversion easier
    end

    def move_next : Bool
      return false if @end

      if @row_index < @row_count - 1
        @row_index += 1
        @column_index = -1
        true
      else
        @end = true
        false
      end
    end

    def column_count : Int32
      @columns.size
    end

    def column_name(index : Int32) : String
      @columns[index]["name"].to_s
    end

    def read
      @column_index += 1
      return @data[@row_index][@column_index]
    end

    def headers
      @http_response.headers
    end
  end

  class Driver < ::DB::Driver
    def build_connection(context : ::DB::ConnectionContext) : Connection
      Connection.new(context)
    end
  end
end

DB.register_driver "presto", Presto::Driver
