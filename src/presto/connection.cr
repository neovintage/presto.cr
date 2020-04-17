module Presto
  DEFAULT_HEADERS = HTTP::Headers{
    "User-Agent" => "presto-crystal/#{VERSION}",
  }

  PRESTO_HEADERS = {
    "user_agent" => "User-Agent",

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

  struct ConnectionOptions
    getter http_headers : HTTP::Headers

    def initialize
      @http_headers = DEFAULT_HEADERS.clone
    end

    # This is used in situations where we're parsing the params from the
    # database uri.
    #
    def initialize(params : HTTP::Params)
      @http_headers = DEFAULT_HEADERS.clone
      map_keys(params)
    end

    def merge!(other)
      other.each do |key, value|
        k = PRESTO_HEADERS[key]?
        if !k.nil?
          @http_headers[k] = value
        end
      end
    end

    private def map_keys(params)
      params.each do |key, value|
        k = PRESTO_HEADERS[key]?
        if !k.nil?
          @http_headers[k] = value
        end
      end
    end

    macro create_methods
      {% for method_name in ["[]=(key, value : String)", "[]=(key, value : Array(String))", "[](key)", "[]?(key)"] %}
        def {{method_name.id}}
          key = PRESTO_HEADERS[key]
          @http_headers.{{method_name.id}}
        end
      {% end %}
    end

    create_methods
  end

  class Connection < ::DB::Connection
    protected getter connection

    # todo throw error if username isnt defined. that's required
    def initialize(context)
      super(context)

      # todo create a new uri
      context.uri.scheme = set_scheme(context.uri)

      @connection = HTTP::Client.new(context.uri)
      @connection.basic_auth(context.uri.user, context.uri.password)

      @options = ConnectionOptions.new

      # todo need to have defaults that the user can then override. dont need to do this. can use a different way of calling in the http client
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
end
