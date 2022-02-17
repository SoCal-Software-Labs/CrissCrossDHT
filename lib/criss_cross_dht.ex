defmodule CrissCrossDHT do
  require Logger

  alias CrissCrossDHT.Server.Utils, as: Utils

  @moduledoc ~S"""
  CrissCrossDHT is an Elixir package that provides a Kademlia Distributed Hash Table
  (DHT) implementation according to [BitTorrent Enhancement Proposals (BEP)
  05](http://www.bittorrent.org/beps/bep_0005.html). This specific
  implementation is called "mainline" variant.

  """

  ## Constants

  @node_id Utils.gen_node_id()
  @node_id_enc Utils.encode_human(@node_id)

  ## Types

  @typedoc """
  A binary which contains the cluster
  """
  @type cluster :: binary

  @type ttl :: integer

  @typedoc """
  A binary which contains the infohash of a torrent. An infohash is a SHA1
  encoded hex sum which identifies a torrent.
  """
  @type infohash :: binary

  @typedoc """
  A non negative integer (0--65565) which represents a TCP port number.
  """
  @type tcp_port :: 0..65_565

  @typedoc """
  TODO
  """
  @type node_id :: <<_::34>>

  @typedoc """
  TODO
  """
  @type node_id_enc :: String.t()

  @process_name CrissCrossDHT.Server.Worker

  def store(cluster, value, ttl) do
    CrissCrossDHT.Server.Worker.store(@process_name, cluster, value, ttl)
  end

  def store(cluster, value, ttl, tid) do
    CrissCrossDHT.Server.Worker.store(@process_name, cluster, value, ttl, tid)
  end

  def find_value(cluster, infohash, callback) do
    CrissCrossDHT.Server.Worker.find_value(@process_name, cluster, infohash, callback)
  end

  def find_value_sync(cluster, infohash, timeout \\ 10_000) do
    CrissCrossDHT.Server.Worker.find_value_sync(@process_name, cluster, infohash, timeout)
  end

  def store_name(cluster, rsa_priv_key, value, save_local, save_remote, ttl) do
    CrissCrossDHT.Server.Worker.store_name(
      @process_name,
      cluster,
      rsa_priv_key,
      value,
      save_local,
      save_remote,
      ttl
    )
  end

  def store_name(cluster, rsa_priv_key, value, save_local, save_remote, ttl, tid) do
    CrissCrossDHT.Server.Worker.store_name(
      @process_name,
      cluster,
      rsa_priv_key,
      value,
      save_local,
      save_remote,
      ttl,
      tid
    )
  end

  def find_name(cluster, infohash, generation, callback) do
    CrissCrossDHT.Server.Worker.find_name(@process_name, cluster, infohash, generation, callback)
  end

  def find_name_sync(cluster, infohash, generation, timeout \\ 10_000) do
    CrissCrossDHT.Server.Worker.find_name_sync(
      @process_name,
      cluster,
      infohash,
      generation,
      timeout
    )
  end

  def ref() do
    <<System.monotonic_time()::big-integer-64>>
  end

  @doc ~S"""
  This function returns the generated node_id as a bitstring.
  """
  @spec node_id() :: node_id
  def node_id, do: @node_id

  @doc ~S"""
  This function returns the generated node_id encoded as a String (40
  characters).
  """
  @spec node_id_enc() :: node_id_enc
  def node_id_enc, do: @node_id_enc

  @doc ~S"""
  This function needs an infohash as binary and a callback function as
  parameter. This function uses its own routing table as a starting point to
  start a get_peers search for the given infohash.

  ## Example
      iex> "3F19B149F53A50E14FC0B79926A391896EABAB6F"
            |> Base.decode16!
            |> CrissCrossDHT.search(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end)
  """
  @spec search(cluster, infohash, fun) :: atom
  def search(cluster, infohash, callback) do
    CrissCrossDHT.Server.Worker.search(@process_name, cluster, infohash, callback)
  end

  @doc ~S"""
  This function needs an infohash as binary and callback function as
  parameter. This function does the same thing as the search/2 function, except
  it sends an announce message to the found peers. This function does not need a
  TCP port which means the announce message sets `:implied_port` to true.

  ## Example
      iex> "3F19B149F53A50E14FC0B79926A391896EABAB6F"
           |> Base.decode16!
           |> CrissCrossDHT.search_announce(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end)
  """
  @spec search_announce(cluster, infohash, ttl, fun) :: atom
  def search_announce(cluster, infohash, ttl, callback) do
    CrissCrossDHT.Server.Worker.search_announce(@process_name, cluster, infohash, ttl, callback)
  end

  @doc ~S"""
  This function needs an infohash as binary, a callback function as parameter,
  and a TCP port as integer. This function does the same thing as the search/2
  function, except it sends an announce message to the found peers.

  ## Example
      iex> "3F19B149F53A50E14FC0B79926A391896EABAB6F" ## Ubuntu 15.04
           |> Base.decode16!
           |> CrissCrossDHT.search_announce(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end, 6881)
  """
  @spec search_announce(cluster, infohash, fun, ttl, tcp_port) :: atom
  def search_announce(cluster, infohash, callback, ttl, port) do
    CrissCrossDHT.Server.Worker.search_announce(
      @process_name,
      cluster,
      infohash,
      port,
      ttl,
      callback
    )
  end

  def cluster_announce(cluster, infohash, ttl) do
    CrissCrossDHT.Server.Worker.cluster_announce(@process_name, cluster, infohash, ttl)
  end

  def has_announced(cluster, infohash) do
    CrissCrossDHT.Server.Worker.has_announced_cluster(@process_name, cluster, infohash)
  end
end
