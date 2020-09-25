require "./spec_helper"

describe Presto do
  it "should register Presto::Driver" do
    DB.open(DB_URL).driver.should be_a(Presto::Driver)
  end

  it "should query a cluster" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer limit 1"
      typeof(result).should eq Presto::ResultSet
    end
  end

  it "should return the column size" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer limit 1"
      result.column_count.should eq 8
    end
  end

  it "should return column even if data doesnt exist" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer where name = 'nonsense' limit 1"
      result.column_count.should eq 8
    end
  end

  it "should have an empty data array if nothing is returned" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer where name = 'nonsense' limit 1"
      result.data.size.should eq 0
    end
  end

  it "should be able to return rows" do
    DB.open(DB_URL) do |db|
      db.query "select * from tpch.sf1.customer where name = 'Customer#000000001' limit 1" do |rs|
        rs.row_count.should eq 1
        rs.column_name(0).should eq "custkey"
        rs.each do
          rs.read.as_i.should eq 1
        end
      end
    end
  end

  it "should show the response headers" do
    DB.open(DB_URL) do |db|
      db.query "select * from tpch.sf1.customer where name = 'Customer#000000001' limit 1" do |rs|
        typeof(rs.response_headers).should eq HTTP::Headers
        rs.response_headers.should_not be_nil
      end
    end
  end

  it "should use SSL when specified" do
    database = DB.open(DB_URL + "?SSL=true")
    conn = database.checkout
    conn.http_uri.scheme.should eq "https"
    database.close
  end

  it "should add headers from the URI" do
    DB.open(DB_URL + "?time_zone=US%2FEastern") do |db|
      db.using_connection do |conn|
        conn.options["time_zone"].should eq("US/Eastern")
      end
    end
  end

  it "should reset the param when a new assignment happens" do
    DB.open(DB_URL + "?time_zone=US%2FEastern") do |db|
      db.using_connection do |conn|
        conn.options["time_zone"].should eq("US/Eastern")
        conn.options["time_zone"] = "US/Pacific"
        conn.options["time_zone"].should eq("US/Pacific")
      end
    end
  end

  it "should be able to override headers per query" do
    DB.open(DB_URL + "?user_agent=dude") do |db|
      db.using_connection do |conn|
        conn.options["user_agent"] = "another_user_agent"
        result = conn.query "select * from tpch.sf1.customer limit 1"
        result.request_options["user_agent"].should eq "another_user_agent"

        conn.options["user_agent"] = "blah"
        result = conn.query "select * from tpch.sf1.customer limit 1"
        result.request_options["user_agent"].should eq "blah"
      end
    end
  end

end
