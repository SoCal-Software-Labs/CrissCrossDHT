defmodule MlDHT do
  use Application

  require Logger

  alias MlDHT.Server.Utils, as: Utils

  @moduledoc ~S"""
  MlDHT is an Elixir package that provides a Kademlia Distributed Hash Table
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
  @type node_id :: <<_::20>>

  @typedoc """
  TODO
  """
  @type node_id_enc :: String.t()

  @doc false
  def start(_type, _args) do
    MlDHT.Registry.start()

    ## Generate a new node ID
    Logger.debug("Node-ID: #{@node_id_enc}")

    ## Start the main supervisor
    MlDHT.Supervisor.start_link(
      node_id: @node_id,
      name: MlDHT.Registry.via(@node_id_enc, MlDHT.Supervisor)
    )
  end

  def generate_store_keypair() do
    {:ok, rsa_priv_key} = ExPublicKey.generate_key()
    {:ok, rsa_pub_key} = ExPublicKey.public_key_from_private_key(rsa_priv_key)
    {rsa_priv_key, rsa_pub_key}
  end

  def store(cluster, value, ttl) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    key = Utils.hash(value)
    MlDHT.Server.Worker.store(pid, cluster, key, value, ttl)
  end

  def find_value(cluster, infohash, callback) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.find_value(pid, cluster, infohash, callback)
  end

  def store_name(cluster, rsa_priv_key, value, ttl) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.store_name(pid, cluster, rsa_priv_key, value, ttl)
  end

  def find_name(cluster, infohash, generation, callback) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.find_name(pid, cluster, infohash, generation, callback)
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
            |> MlDHT.search(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end)
  """
  @spec search(cluster, infohash, fun) :: atom
  def search(cluster, infohash, callback) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.search(pid, cluster, infohash, callback)
  end

  @doc ~S"""
  This function needs an infohash as binary and callback function as
  parameter. This function does the same thing as the search/2 function, except
  it sends an announce message to the found peers. This function does not need a
  TCP port which means the announce message sets `:implied_port` to true.

  ## Example
      iex> "3F19B149F53A50E14FC0B79926A391896EABAB6F"
           |> Base.decode16!
           |> MlDHT.search_announce(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end)
  """
  @spec search_announce(cluster, infohash, fun) :: atom
  def search_announce(cluster, infohash, callback) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.search_announce(pid, cluster, infohash, callback)
  end

  @doc ~S"""
  This function needs an infohash as binary, a callback function as parameter,
  and a TCP port as integer. This function does the same thing as the search/2
  function, except it sends an announce message to the found peers.

  ## Example
      iex> "3F19B149F53A50E14FC0B79926A391896EABAB6F" ## Ubuntu 15.04
           |> Base.decode16!
           |> MlDHT.search_announce(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end, 6881)
  """
  @spec search_announce(cluster, infohash, fun, tcp_port) :: atom
  def search_announce(cluster, infohash, callback, port) do
    pid = @node_id_enc |> MlDHT.Registry.get_pid(MlDHT.Server.Worker)
    MlDHT.Server.Worker.search_announce(pid, cluster, infohash, callback, port)
  end
end
