#!/usr/bin/env ruby

$:.unshift File.join(File.dirname(__FILE__), *%w[.. lib])

require 'rubygems'
require 'riak'
require 'erlectricity'
require 'time'

class RiakRubyNode
  def initialize()
    @client = Riak::Client.new(:http_backend => :Excon)
  end

  def walk(bucket, key, lbucket, tag)
    b = @client.bucket(bucket)
    o = b.new(key)
    begin
      if o.walk(:bucket=>lbucket, :tag=>tag, :keep=>true)
        :ok
      else
        :error
      end
    rescue
      :error
    end
  end

  def mapred_walk(bucket, key, lbucket, tag)
    begin
      results = Riak::MapReduce.new(@client).
        add(bucket, key).
        link(:bucket=>lbucket, :tag=>tag).
        map(["riak_kv_mapreduce", "map_object_value"]).run
      if results
        :ok
      else
        :error
      end
    rescue
      :error
    end
  end
end

node = RiakRubyNode.new()

begin
  receive do |f|
    f.when([:link_walk, String, String, String, String]) do |b, k, lb, t|
      reply = node.walk(b, k, lb, t)
      #reply = node.mapred_walk(b, k, lb, t)
      f.send!(reply) # :ok or :error
      f.receive_loop
    end
  end
rescue Errno::EPIPE
  # controlling erlang process exited
end
