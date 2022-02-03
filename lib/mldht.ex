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

  @cypher "DEoGUJcJCMKVuHXAmjgNyU6cCrPcHFymd6c7o5i9yVzT"
  @public_key "-----BEGIN RSA PUBLIC KEY-----\nMIIBCgKCAQEAzCBvDU/9sz1tPMonJMu9khHk0VaMet8bgJMShHpSYM7ul0yfihz1\nL7CU5ppZF+v2tr+xFt2aSSqFf4irpbOOuBryBPZUDBgQSIz/JpLLMBmioUq0EZfZ\nfmG8XU1lygKsP+MoyQJ5umzt4i9e9mMV+LVwZmY88dhcFvDWkT0MG9yrYjX65L8L\nja4mXZWLAIZoUFaJ617SxeoP0Bv5bmAgH666T8q66l4FTagPHX/komRkQhvYH4FD\ncfTr/Z0mnSCuNreA2vJpXupekfgEPMwXz3zlPPQ7f4E+mqUO+p+XYUE7JBlTggYD\ncpEzPUt7Wk0gbe9K+ez7zDojoGkSxhc3KwIDAQAB\n-----END RSA PUBLIC KEY-----\n\n"
  @private_key "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEAzCBvDU/9sz1tPMonJMu9khHk0VaMet8bgJMShHpSYM7ul0yf\nihz1L7CU5ppZF+v2tr+xFt2aSSqFf4irpbOOuBryBPZUDBgQSIz/JpLLMBmioUq0\nEZfZfmG8XU1lygKsP+MoyQJ5umzt4i9e9mMV+LVwZmY88dhcFvDWkT0MG9yrYjX6\n5L8Lja4mXZWLAIZoUFaJ617SxeoP0Bv5bmAgH666T8q66l4FTagPHX/komRkQhvY\nH4FDcfTr/Z0mnSCuNreA2vJpXupekfgEPMwXz3zlPPQ7f4E+mqUO+p+XYUE7JBlT\nggYDcpEzPUt7Wk0gbe9K+ez7zDojoGkSxhc3KwIDAQABAoIBAHcNZ5edEruKVP7C\nbGgSiCL8WrcZQl+bZj/sBz3K1ebuactGfjogP4Qr+fwxA0tnbQIS9Sb/4i9QJIJI\nZMwE2HVaCdOJE2XmVwDpcxq9PNJ18RsfJbypEsmaGTFVpctXGb09MJlj3zkytN9Z\nf4o2KidfMwoWEO+An90lZA9bSoeodbabuaufHiNd2Llx67U67HzmsJhUYrg38ZC5\nMWexapTo4oS9cY4zQN3LwLNyawnekw8oSOOefTr0hnI33xQPZSrH/VCkXNpCUEer\nYigRJGi4GlThtFRnhNcniaRmUs3/EMI85SDvPbtpOSD87iV/uXZBcWfbpJe4NybC\ncZnhy0ECgYEA90t3t/H+Abyt81qgx/r4Fl1zLYSkCuIkllXBQJvrrGuQyLip4/k5\nYvfjUu0gQyaVoLj2oGKlHPXE3xYLK9yJmewRNV7eBmUpfCCjUROFSzCH4SP6u2C0\naG/9DUelQw3pAnMzgXt7ePB6Iw0hqKejDZrJPau7DQDDE1TeL5paCYsCgYEA00/z\nXUtG3sLqfDhrEOt1RfD2+YaAu6ESptX94TxdP/aBrIOy8A9j9XpaXkEwthcLuawL\nQEVjAZsBNIP5yFxI3t7wUL6mDE4nm5btKDYKQc8Fmv/48ynHRqdqXSXSHbLD62SH\nLM/Tli13jRGrSRnOq00T9R5eG1MEkfI6FESw/OECgYBbSu79Z0bAWWlWR4THjuz7\nRLB6g1cT9XxQS4Q2V9lfI66lixac5Kq80IqJWKTqZVojpWTWvNP7pvdw6/Bf1uCt\nhCquK0GH1tzDyEDCc5Rnt5jSErhDaGXxkDY5KtPlt0Ln9qNzD6T7druAKR7d5lUZ\ndqUIMVeyay+Y+WG07SSEFQKBgQCLCF+nUpAeoUCG2tgXGdTfX9wf8U9iJGiRPNr+\nBymTnC1VxJFHQdkS+p3axim2pRMh5wDAGOc7dzEjzHHcUlvfx+92MPovvnxw8qy3\neFbnVb7qbODvnN1wr1ZcUzYcNDKT/mCyK0ub0+6E8sswHbrNGrm23XQtpkGrhSSR\nkWCiAQKBgQDtGPigKl4j61FPgTITeNoG87mTWx7YE5L6AAVjIzH4YiLMsiSL6Cis\nt+NEeIO8O0JAULZdf9jZM1kaV9oGopBUErlcJKfhAndYeWbN2hA0lpPimfCMFaV8\n+9lG3KCkDZJClZkabPK82euAiDDPBWo3MxAeRxPghbIeBftNDRjhzw==\n-----END RSA PRIVATE KEY-----\n\n"

  @cluster_name Utils.encode_human(Utils.hash(Utils.combine_to_sign([@cypher, @public_key])))

  @process_name MlDHT.Server.Worker

  @doc false
  def start(_type, _args) do
    MlDHT.Registry.start()

    config = %{
      port: System.get_env("PORT", "3000") |> String.to_integer(),
      ipv4: true,
      ipv6: false,
      clusters: %{
        @cluster_name => %{secret: @cypher, public_key: @public_key, private_key: @private_key}
      },
      bootstrap_nodes: [
        %{node_id: "8thbnFn4HZ24vVojR5qV6jsLCoqMaeBAVSxioBLmzGzC", host: "localhost", port: 3000}
      ],
      k_bucket_size: 8,
      storage: {MlDHT.Server.Storage, []}
    }

    Logger.debug("Cluster: #{@cluster_name}")
    ## Start the main supervisor
    MlDHT.Supervisor.start_link(
      node_id: @node_id,
      worker_name: @process_name,
      config: config,
      name: MlDHT.Registry.via(@node_id_enc, MlDHT.Supervisor)
    )
  end

  def generate_store_keypair() do
    {:ok, rsa_priv_key} = ExPublicKey.generate_key()
    {:ok, rsa_pub_key} = ExPublicKey.public_key_from_private_key(rsa_priv_key)
    {rsa_priv_key, rsa_pub_key}
  end

  def store(cluster, value, ttl) do
    MlDHT.Server.Worker.store(@process_name, cluster, value, ttl)
  end

  def store(cluster, value, ttl, tid) do
    MlDHT.Server.Worker.store(@process_name, cluster, value, ttl, tid)
  end

  def find_value(cluster, infohash, callback) do
    MlDHT.Server.Worker.find_value(@process_name, cluster, infohash, callback)
  end

  def find_value_sync(cluster, infohash, timeout \\ 10_000) do
    MlDHT.Server.Worker.find_value_sync(@process_name, cluster, infohash, timeout)
  end

  def store_name(cluster, rsa_priv_key, value, local, remote, ttl) do
    MlDHT.Server.Worker.store_name(
      @process_name,
      cluster,
      rsa_priv_key,
      value,
      local,
      remote,
      ttl
    )
  end

  def store_name(cluster, rsa_priv_key, value, local, remote, ttl, tid) do
    MlDHT.Server.Worker.store_name(
      @process_name,
      cluster,
      rsa_priv_key,
      value,
      local,
      remote,
      ttl,
      tid
    )
  end

  def find_name(cluster, infohash, generation, callback) do
    MlDHT.Server.Worker.find_name(@process_name, cluster, infohash, generation, callback)
  end

  def find_name_sync(cluster, infohash, generation, timeout \\ 10_000) do
    MlDHT.Server.Worker.find_name_sync(@process_name, cluster, infohash, generation, timeout)
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
            |> MlDHT.search(fn(node) ->
             {ip, port} = node
             IO.puts "ip: #{inspect ip} port: #{port}"
           end)
  """
  @spec search(cluster, infohash, fun) :: atom
  def search(cluster, infohash, callback) do
    MlDHT.Server.Worker.search(@process_name, cluster, infohash, callback)
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
    MlDHT.Server.Worker.search_announce(@process_name, cluster, infohash, callback)
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
    MlDHT.Server.Worker.search_announce(@process_name, cluster, infohash, callback, port)
  end
end
