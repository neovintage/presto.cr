require "./spec_helper"

describe Presto::ConnectionOptions do

  it "should be able to have defaults" do
    opts = Presto::ConnectionOptions.new
    typeof(opts.http_headers).should eq HTTP::Headers
    opts.http_headers.should eq HTTP::Headers{"user_agent" => "presto-crystal/#{Presto::VERSION}"}
  end

  it "should be able to set new headers" do
    opts = Presto::ConnectionOptions.new
    opts["transaction_id"] = "else"
    opts.http_headers.has_key?("X-Presto-Transaction-Id").should be_true
  end

  it "should be able to use the easy name to retrieve values" do
    opts = Presto::ConnectionOptions.new
    opts["user_agent"].should eq "presto-crystal/#{Presto::VERSION}"
  end

  it "should silently drop options that arent valid" do
    opts = Presto::ConnectionOptions.new
    opts["whatever"] = "else"
    opts.has_key?("whatever").should be_false
  end

end
