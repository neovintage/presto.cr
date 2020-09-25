require "db"
require "http/client"
require "json"
require "uri"
require "./presto/*"

module Presto
  class Statement < ::DB::Statement
    def initialize(connection, @sql : String)
      super(connection)
    end

    protected def http_client
      connection.as(::Presto::Connection).connection
    end

    # todo the args in enumerable should have the options that can be overridden
    #      the connection should have the default options that were set up for the data base. On
    #      a query basis you should be able to override the options.
    protected def perform_query(args : Enumerable) : ResultSet
      start_time = Time.monotonic
      timeout = statement_timeout

      http_response = http_client.post("/v1/statement", headers: connection.options.http_headers, body: @sql)
      json = uninitialized JSON::Any

      loop do
        json = JSON.parse(http_response.body)
        break if ((Time.monotonic - start_time) > timeout) || json["nextUri"]?.nil? || json["data"]?

        http_response = http_client.get(json["nextUri"].to_s, headers: connection.options.http_headers)
      end

      ResultSet.new(self, json, http_response, connection.options)
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
    getter request_options : Presto::ConnectionOptions

    def initialize(statement, @query_results : JSON::Any, response : HTTP::Client::Response, @request_options)
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

    def response_headers
      @http_response.headers
    end
  end

  class Driver < ::DB::Driver
    def build_connection(context : ::DB::ConnectionContext) : ::Presto::Connection
      ::Presto::Connection.new(context)
    end
  end
end

DB.register_driver "presto", Presto::Driver
