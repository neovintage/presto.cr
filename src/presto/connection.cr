module Presto
  DEFAULT_HEADERS = HTTP::Headers{
    "user_agent" => "presto-crystal/#{VERSION}",
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

  # ConnectionOptions manages all of the header information for the HTTP::Client.
  # This struct will silently drop any options that arent part of the valid
  # connection options for presto. We've got the mapping the way that it is to make
  # constructing a URI and working with the options a bit easier
  #
  struct ConnectionOptions
    @http_headers : HTTP::Headers

    def initialize
      @http_headers = DEFAULT_HEADERS.clone
    end

    # This is used in situations where we're parsing the params from the
    # database uri.
    #
    def initialize(uri : URI)
      @http_headers = DEFAULT_HEADERS.clone
      if !uri.query.nil?
        params = HTTP::Params.parse(uri.query.not_nil!)
        map_keys(params)
      end
    end

    def []=(key, value)
      k = PRESTO_HEADERS[key]?
      if !k.nil?
        @http_headers[key] = value
      end
    end

    def http_headers
      headers = HTTP::Headers.new
      @http_headers.each do |key, value|
        k = PRESTO_HEADERS[key]?
        if !k.nil?
          headers[k] = value
        end
      end
      headers
    end

    private def map_keys(params)
      params.each do |key, value|
        k = PRESTO_HEADERS[key]?
        if !k.nil?
          @http_headers[key] = value
        end
      end
    end

    delegate "[]", to: @http_headers
    delegate "[]?", to: @http_headers
    delegate "has_key?", to: @http_headers
  end

  class Connection < ::DB::Connection
    protected getter connection
    getter http_uri : URI
    getter options : Presto::ConnectionOptions

    # todo throw error if username isnt defined. that's required
    def initialize(context)
      super(context)

      @http_uri = context.uri.dup
      @http_uri.scheme = set_scheme(@http_uri)

      @options = ConnectionOptions.new(@http_uri)

      @connection = HTTP::Client.new(@http_uri)
      @connection.basic_auth(context.uri.user, context.uri.password)
    end

    def uri
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
