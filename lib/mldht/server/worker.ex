defmodule MlDHT.Server.Worker do
  @moduledoc false

  use GenServer

  require Logger

  alias MlDHT.Server.Utils
  alias MlDHT.Server.Storage
  alias MlDHT.Registry

  alias MlDHT.RoutingTable.Node
  alias MlDHT.Search.Worker, as: Search
  alias MlDHT.SearchValue.Worker, as: SearchValue
  alias MlDHT.SearchName.Worker, as: SearchName

  @type ip_vers :: :ipv4 | :ipv6

  # Time after the secret changes
  @time_cluster_secret 60 * 1000 * 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, [node_id: opts[:node_id], config: opts[:config]],
      name: opts[:name]
    )
  end

  @doc """
  This function takes the bootstrapping nodes from the config and starts a
  find_node search to our own node id. By doing this, we will quickly collect
  nodes that are close to us and save it to our own routing table.

  ## Example
  iex> MlDHT.DHTServer.Worker.bootstrap
  """
  def bootstrap(pid) do
    GenServer.cast(pid, :bootstrap)
  end

  @doc ~S"""
  This function needs an infohash as binary, and a callback function as
  parameter. This function uses its own routing table as a starting point to
  start a get_peers search for the given infohash.

  ## Example
  iex> infohash = "3F19..." |> Base.decode16!
  iex> MlDHT.DHTServer.search(infohash, fn(node) ->
  {ip, port} = node
  IO.puts "ip: #{ip} port: #{port}"
  end)
  """
  def search(pid, cluster, infohash, callback) do
    GenServer.cast(pid, {:search, cluster, infohash, callback})
  end

  def search_announce(pid, cluster, infohash, callback) do
    GenServer.cast(pid, {:search_announce, cluster, infohash, callback})
  end

  def search_announce(pid, cluster, infohash, port, callback) do
    GenServer.cast(pid, {:search_announce, cluster, infohash, port, callback})
  end

  def find_value(pid, cluster, key, callback) do
    GenServer.cast(pid, {:find_value, cluster, key, callback})
  end

  def find_value_sync(pid, cluster, key, timeout \\ 10_000) do
    ref = make_ref()
    outer = self()

    callback = fn
      remote, value ->
        send(outer, {ref, remote, value})
    end

    GenServer.cast(pid, {:find_value, cluster, key, callback})

    receive do
      {^ref, remote, value} ->
        {remote, value}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def find_name(pid, cluster, name, generation, callback) do
    GenServer.cast(pid, {:find_name, cluster, name, generation, callback})
  end

  def find_name_sync(pid, cluster, name, generation, timeout \\ 10_000) do
    ref = make_ref()
    outer = self()

    callback = fn remote, name ->
      send(outer, {ref, remote, name})
    end

    GenServer.cast(
      pid,
      {:find_name, cluster, name, generation, callback}
    )

    receive do
      {^ref, remote, name} -> {remote, name}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def store(pid, cluster, value, ttl) do
    tid = KRPCProtocol.gen_tid()

    store(pid, cluster, value, ttl, tid)
  end

  def store(pid, cluster, value, ttl, tid) do
    key = Utils.hash(value)
    :ok = GenServer.cast(pid, {:store, cluster, key, value, ttl, tid})
    {key, tid}
  end

  def store_name(pid, cluster, private_rsa_key, value, local, remote, ttl) do
    tid = KRPCProtocol.gen_tid()
    store_name(pid, cluster, private_rsa_key, value, local, remote, ttl, tid)
  end

  def store_name(pid, cluster, private_rsa_key, value, local, remote, ttl, tid) do
    {:ok, public_key} = ExPublicKey.public_key_from_private_key(private_rsa_key)
    {:ok, pem_string} = ExPublicKey.pem_encode(public_key)
    public_key_hash = Utils.hash(pem_string)
    name = Utils.hash(public_key_hash)

    :ok =
      GenServer.cast(
        pid,
        {:store_name, cluster, private_rsa_key, public_key_hash, name, value, local, remote, ttl,
         tid}
      )

    {name, tid}
  end

  def get_public_key(cache, pid, storage_pid, cluster, key_hash) do
    # TODO add cache
    case :ets.lookup(cache, key_hash) do
      [{_, key}] ->
        {:ok, key}

      _ ->
        key_file =
          case Storage.get_value(storage_pid, key_hash) do
            nil ->
              server_pid = pid
              task = Task.async(fn -> find_value_sync(server_pid, cluster, key_hash) end)
              Task.await(task)

            key ->
              key
          end

        case key_file do
          s when is_binary(s) ->
            case ExPublicKey.loads(s) do
              {:ok, key} ->
                :ets.insert(cache, {key_hash, key})
                {:ok, key}

              e ->
                e
            end

          _ ->
            {:error, "public key not found"}
        end
    end
  end

  def create_udp_socket(config, port, ip_vers) do
    ip_addr = ip_vers |> to_string() |> Kernel.<>("_addr") |> String.to_atom()
    bind_ip = Utils.config(config, ip_addr, {127, 0, 0, 1})
    options = ip_vers |> inet_option() |> maybe_put(:ip, bind_ip)

    case :gen_udp.open(port, options ++ [{:active, true}]) do
      {:ok, socket} ->
        Logger.debug("Init DHT Node (#{ip_vers})")

        foo = :inet.getopts(socket, [:ipv6_v6only])
        Logger.debug("Options: #{inspect(foo)}")

        {socket, bind_ip}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def init(node_id: node_id, config: config) do
    ## Returns false in case the option is not set in the environment (setting
    ## the option to false or not setting the option at all has the same effect
    ## in this case)
    cfg_ipv6_is_enabled? = Utils.config(config, :ipv6, false)
    cfg_ipv4_is_enabled? = Utils.config(config, :ipv4, false)

    unless cfg_ipv4_is_enabled? or cfg_ipv6_is_enabled? do
      raise "Configuration failure: Either ipv4 or ipv6 has to be set to true."
    end

    cfg_cluster = Utils.config(config, :clusters)
    cfg_port = Utils.config(config, :port)

    {socket, socket_ip} =
      if cfg_ipv4_is_enabled?, do: create_udp_socket(config, cfg_port, :ipv4), else: {nil, nil}

    {socket6, socket6_ip} =
      if cfg_ipv6_is_enabled?, do: create_udp_socket(config, cfg_port, :ipv6), else: {nil, nil}

    ## Change secret of the token every 5 minutes
    Process.send_after(self(), :cluster_secret, @time_cluster_secret)
    cache = :ets.new(:key_cache, [:set, :public])

    state = %{
      node_id: node_id,
      node_id_enc: Utils.encode_human(node_id),
      socket: socket,
      socket6: socket6,
      old_secret: nil,
      secret: Utils.gen_secret(),
      cluster_config: cfg_cluster,
      config: config,
      self_pid: self(),
      cache: cache,
      ip_tuple: {socket_ip, cfg_port}
    }

    # INFO Setup routingtable for IPv4
    if cfg_ipv4_is_enabled? do
      for {cluster_header, cluster_secret} <- state.cluster_config do
        start_rtable(node_id, cluster_header, cluster_secret, :ipv4, {socket_ip, cfg_port})
      end

      if Utils.config(config, :bootstrap_nodes) do
        bootstrap(state, {socket, :inet})
      end
    end

    # INFO Setup routingtable for IPv6
    if cfg_ipv6_is_enabled? do
      for {cluster_header, cluster_secret} <- state.cluster_config do
        start_rtable(node_id, cluster_header, cluster_secret, :ipv6, {socket6_ip, cfg_port})
      end

      if Utils.config(config, :bootstrap_nodes) do
        bootstrap(state, {socket6, :inet6})
      end
    end

    {:ok, state}
  end

  defp start_rtable(node_id, header, cluster_secret, rt_ident, ip_tuple) do
    node_id_enc = node_id |> Utils.encode_human()
    rt_name = Utils.encode_human(header) <> to_string(rt_ident)

    ## Allows giving atoms as rt_name to this function, e.g. :ipv4
    {:ok, _pid} =
      node_id_enc
      |> MlDHT.Registry.get_pid(MlDHT.RoutingTable.Supervisor)
      |> DynamicSupervisor.start_child({
        MlDHT.RoutingTable.Supervisor,
        node_id: node_id,
        ip_tuple: ip_tuple,
        node_id_enc: node_id_enc,
        cluster: header,
        cluster_secret: cluster_secret,
        rt_name: rt_name
      })

    node_id |> get_rtable(header, rt_ident)
  end

  defp get_rtable(node_id, header, rt_ident) do
    rt_name = Utils.encode_human(header) <> to_string(rt_ident)

    node_id
    |> Utils.encode_human()
    |> MlDHT.Registry.get_pid(MlDHT.RoutingTable.Worker, rt_name)
  end

  def handle_cast({:bootstrap, socket_tuple}, state) do
    bootstrap(state, socket_tuple)
    {:noreply, state}
  end

  def handle_cast({:store, cluster_header, key, value, ttl, tid}, state) do
    # TODO What about ipv6?
    Logger.debug("[*] >> store #{Utils.encode_human(tid)}")

    case state.cluster_config do
      %{^cluster_header => %{private_key: rsa_priv_key} = cluster_secret}
      when not is_nil(rsa_priv_key) ->
        nodes =
          state.node_id
          |> get_rtable(cluster_header, :ipv4)
          |> MlDHT.RoutingTable.Worker.closest_nodes(key)

        {:ok, signature} = Utils.sign(value, rsa_priv_key)

        args = [
          tid: tid,
          node_id: state.node_id,
          key: key,
          value: value,
          ttl: ttl,
          signature: signature
        ]

        payload = KRPCProtocol.encode(:store, args)
        payload = Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))

        for node <- MlDHT.Search.Worker.nodes_to_search_nodes(nodes) do
          :gen_udp.send(state.socket, node.ip, node.port, payload)
        end

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_cast(
        {:store_name, cluster_header, rsa_priv_name_key, public_key_hash, name, value, local,
         remote, ttl, tid},
        state
      ) do
    # TODO What about ipv6?
    Logger.debug("[*] >> store_name")

    case state.cluster_config do
      %{^cluster_header => %{private_key: rsa_priv_key} = cluster_secret}
      when not is_nil(rsa_priv_key) ->
        storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

        {:ok, signature} = Utils.sign(value, rsa_priv_key)

        {generation, to_sign} =
          case Storage.get_name(storage_pid, name) do
            nil ->
              {0, Utils.combine_to_sign([0, signature])}

            {value, generation, _signature} ->
              {generation + 1, Utils.combine_to_sign([generation + 1, signature])}
          end

        {:ok, signature_ns} = Utils.sign(to_sign, rsa_priv_name_key)

        args = [
          tid: tid,
          node_id: state.node_id,
          name: name,
          value: value,
          ttl: ttl,
          public_key: public_key_hash,
          generation: generation,
          signature: signature,
          signature_ns: signature_ns
        ]

        if local do
          Storage.put_name(
            storage_pid,
            name,
            value,
            generation,
            signature_ns,
            ttl
          )
        end

        if remote do
          payload = KRPCProtocol.encode(:store_name, args)
          payload = Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))

          nodes =
            state.node_id
            |> get_rtable(cluster_header, :ipv4)
            |> MlDHT.RoutingTable.Worker.closest_nodes(name)

          for node <- MlDHT.Search.Worker.nodes_to_search_nodes(nodes) do
            :gen_udp.send(state.socket, node.ip, node.port, payload)
          end
        end

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_cast({:find_name, cluster, infohash, generation, callback}, state) do
    # TODO What about ipv6?
    nodes =
      state.node_id
      |> get_rtable(cluster, :ipv4)
      |> MlDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> MlDHT.Registry.get_pid(MlDHT.SearchName.Supervisor)
    |> MlDHT.SearchName.Supervisor.start_child(
      :find_name,
      state.socket,
      state.node_id,
      state.cluster_config
    )
    |> MlDHT.SearchName.Worker.find_name(
      cluster,
      target: infohash,
      generation: generation,
      start_nodes: nodes,
      callback: callback
    )

    {:noreply, state}
  end

  def handle_cast({:find_value, cluster, infohash, callback}, state) do
    # TODO What about ipv6?
    nodes =
      state.node_id
      |> get_rtable(cluster, :ipv4)
      |> MlDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> MlDHT.Registry.get_pid(MlDHT.SearchValue.Supervisor)
    |> MlDHT.SearchValue.Supervisor.start_child(
      :find_value,
      state.socket,
      state.node_id,
      state.ip_tuple,
      state.cluster_config
    )
    |> MlDHT.SearchValue.Worker.find_value(
      cluster,
      target: infohash,
      start_nodes: nodes,
      callback: callback
    )

    {:noreply, state}
  end

  def handle_cast({:search_announce, cluster, infohash, callback}, state) do
    # TODO What about ipv6?
    nodes =
      state.node_id
      |> get_rtable(cluster, :ipv4)
      |> MlDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> MlDHT.Registry.get_pid(MlDHT.Search.Supervisor)
    |> MlDHT.Search.Supervisor.start_child(
      :get_peers,
      state.socket,
      state.node_id,
      state.cluster_config
    )
    |> Search.get_peers(
      cluster,
      target: infohash,
      start_nodes: nodes,
      callback: callback,
      port: 0,
      announce: true
    )

    {:noreply, state}
  end

  def handle_cast({:search_announce, cluster, infohash, callback, port}, state) do
    nodes =
      state.node_id
      |> get_rtable(cluster, :ipv4)
      |> MlDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> MlDHT.Registry.get_pid(MlDHT.Search.Supervisor)
    |> MlDHT.Search.Supervisor.start_child(
      :get_peers,
      state.socket,
      state.node_id,
      state.cluster_config
    )
    |> Search.get_peers(
      cluster,
      target: infohash,
      start_nodes: nodes,
      callback: callback,
      port: port,
      announce: true
    )

    {:noreply, state}
  end

  def handle_cast({:search, cluster, infohash, callback}, state) do
    nodes =
      state.node_id
      |> get_rtable(cluster, :ipv4)
      |> MlDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> MlDHT.Registry.get_pid(MlDHT.Search.Supervisor)
    |> MlDHT.Search.Supervisor.start_child(
      :get_peers,
      state.socket,
      state.node_id,
      state.cluster_config
    )
    |> Search.get_peers(
      cluster,
      target: infohash,
      start_nodes: nodes,
      port: 0,
      callback: callback,
      announce: false
    )

    {:noreply, state}
  end

  def handle_info(:cluster_secret, state) do
    Logger.debug("Change Secret")
    Process.send_after(self(), :cluster_secret, @time_cluster_secret)

    {:noreply, %{state | old_secret: state.secret, secret: Utils.gen_secret()}}
  end

  def handle_info({:udp, socket, ip, port, raw_data}, state) do
    Task.start(fn ->
      {cluster_header, body} =
        raw_data
        |> :binary.list_to_bin()
        |> Utils.unwrap()

      case state.cluster_config do
        %{^cluster_header => cluster_secret} ->
          decrypted = Utils.decrypt(body, cluster_secret)

          case decrypted do
            c when is_binary(c) ->
              c
              |> String.trim_trailing("\n")
              |> KRPCProtocol.decode()
              |> handle_message(
                {socket, get_ip_vers(socket)},
                {cluster_header, cluster_secret},
                ip,
                port,
                state
              )

            e ->
              Logger.error("Error decrypting: #{inspect(e)}")
              {:noreply, state}
          end

        _ ->
          {:noreply, state}
      end
    end)

    {:noreply, state}
  end

  #########
  # Error #
  #########

  def handle_message({:error, error}, _socket, {cluster_header, cluster_secret}, ip, port, state) do
    args = [code: error.code, msg: error.msg, tid: error.tid]
    payload = KRPCProtocol.encode(:error, args)

    :gen_udp.send(
      state.socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  def handle_message({:invalid, msg}, _socket, _ip, _port, state) do
    Logger.error("Ignore unknown or corrupted message: #{inspect(msg, limit: 5000)}")
    ## Maybe we should blacklist this filthy peer?

    {:noreply, state}
  end

  ########################
  # Incoming DHT Queries #
  ########################

  def handle_message(
        {:ping, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> ping")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    send_ping_reply(state.node_id, remote.tid, {cluster_header, cluster_secret}, ip, port, socket)

    {:noreply, state}
  end

  def handle_message(
        {:find_node, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_node")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    ## Get closest nodes for the requested target from the routing table
    nodes =
      state.node_id
      |> get_rtable(cluster_header, ip_vers)
      |> MlDHT.RoutingTable.Worker.closest_nodes(remote.target, remote.node_id)
      |> Enum.map(fn pid ->
        try do
          if Process.alive?(pid) do
            Node.to_tuple(pid)
          end
        rescue
          _e in Enum.EmptyError -> nil
        end
      end)

    # if nodes != [] do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_node_reply")

    nodes_args = if ip_vers == :ipv4, do: [nodes: nodes], else: [nodes6: nodes]
    args = [node_id: state.node_id] ++ nodes_args ++ [tid: remote.tid]
    Logger.debug("NODES ARGS: #{inspect(args)}")
    payload = KRPCProtocol.encode(:find_node_reply, args)

    # Logger.debug(PrettyHex.pretty_hex(to_string(payload)))

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    # end

    {:noreply, state}
  end

  ## Get_peers
  def handle_message(
        {:get_peers, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> get_peers")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    ## Get pid of the storage genserver
    storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

    args =
      if Storage.has_nodes_for_infohash?(storage_pid, remote.info_hash) do
        values = Storage.get_nodes(storage_pid, remote.info_hash)
        [node_id: state.node_id, values: values, tid: remote.tid, token: token]
      else
        ## Get the closest nodes for the requested info_hash
        rtable = state.node_id |> get_rtable(cluster_header, ip_vers)

        nodes =
          Enum.map(MlDHT.RoutingTable.Worker.closest_nodes(rtable, remote.info_hash), fn pid ->
            Node.to_tuple(pid)
          end)

        Logger.debug("[#{Utils.encode_human(remote.node_id)}] << get_peers_reply (nodes)")
        [node_id: state.node_id, nodes: nodes, tid: remote.tid, token: token]
      end

    payload = KRPCProtocol.encode(:get_peers_reply, args)

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  ## Announce_peer
  def handle_message(
        {:announce_peer, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> announce_peer")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    if token_match(remote.token, ip, port, state.secret, state.old_secret) do
      Logger.debug("Valid Token")
      Logger.debug("#{inspect(remote)}")

      port =
        if Map.has_key?(remote, :implied_port) do
          port
        else
          remote.port
        end

      ## Get pid of the storage genserver
      storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

      Storage.put(storage_pid, remote.info_hash, ip, port)

      ## Sending a ping_reply back as an acknowledgement
      send_ping_reply(
        state.node_id,
        remote.tid,
        {cluster_header, cluster_secret},
        ip,
        port,
        socket
      )

      {:noreply, state}
    else
      Logger.debug("[#{Utils.encode_human(remote.node_id)}] << error (invalid token})")

      args = [code: 203, msg: "Announce_peer with wrong token", tid: remote.tid]
      payload = KRPCProtocol.encode(:error, args)

      :gen_udp.send(
        socket,
        ip,
        port,
        Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
      )

      {:noreply, state}
    end
  end

  ## Find Value
  def handle_message(
        {:find_value, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_value")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    ## Get pid of the storage genserver
    storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

    payload =
      case Storage.get_value(storage_pid, remote.key) do
        nil ->
          rtable = state.node_id |> get_rtable(cluster_header, ip_vers)

          nodes =
            Enum.map(MlDHT.RoutingTable.Worker.closest_nodes(rtable, remote.key), fn pid ->
              Node.to_tuple(pid)
            end)

          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_value (nodes)")

          args = [
            node_id: state.node_id,
            key: remote.key,
            nodes: nodes,
            tid: remote.tid,
            token: token
          ]

          KRPCProtocol.encode(:find_value_nodes_reply, args)

        value ->
          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_value (value)")

          args = [
            tid: remote.tid,
            node_id: state.node_id,
            key: remote.key,
            value: value,
            token: token
          ]

          KRPCProtocol.encode(:find_value_reply, args)
      end

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  def handle_message(
        {:find_name, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_name")
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    ## Get pid of the storage genserver
    storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

    payload =
      case Storage.get_name(storage_pid, remote.name) do
        nil ->
          rtable = state.node_id |> get_rtable(cluster_header, ip_vers)

          nodes =
            Enum.map(MlDHT.RoutingTable.Worker.closest_nodes(rtable, remote.name), fn pid ->
              Node.to_tuple(pid)
            end)

          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_name (nodes)")

          args = [
            node_id: state.node_id,
            name: remote.name,
            nodes: nodes,
            tid: remote.tid,
            token: token
          ]

          KRPCProtocol.encode(:find_name_nodes_reply, args)

        {value, generation, signature} ->
          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_name (value)")

          args = [
            tid: remote.tid,
            node_id: state.node_id,
            name: remote.name,
            value: value,
            generation: generation,
            signature: signature,
            token: token
          ]

          KRPCProtocol.encode(:find_name_reply, args)
      end

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  ## Store
  def handle_message(
        {:store_name, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    valid_signature = Utils.verify_signature(cluster_secret, remote.value, remote.signature)

    storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

    hash_matches =
      Utils.hash(remote.public_key) == remote.name and
        Utils.check_generation(storage_pid, remote.name, remote.generation) and
        case get_public_key(
               state.cache,
               state.self_pid,
               storage_pid,
               cluster_header,
               remote.public_key
             ) do
          {:ok, public_key} ->
            Utils.verify_signature(
              %{public_key: public_key},
              Utils.combine_to_sign([remote.generation, remote.signature]),
              remote.signature_ns
            )

          _ ->
            Logger.debug(
              "Could not found NameService public key, #{Utils.encode_human(remote.public_key)}"
            )

            false
        end

    payload =
      if hash_matches and valid_signature do
        ## Get pid of the storage genserver

        Storage.put_name(
          storage_pid,
          remote.name,
          remote.value,
          remote.generation,
          remote.signature_ns,
          remote.ttl
        )

        ## Sending a store_reply back as an acknowledgement
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_name true")
        args = [tid: remote.tid, node_id: state.node_id, name: remote.name, wrote: true]
        KRPCProtocol.encode(:store_name_reply, args)
      else
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_name false")
        args = [tid: remote.tid, node_id: state.node_id, name: remote.name, wrote: false]
        KRPCProtocol.encode(:store_name_reply, args)
      end

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  def handle_message(
        {:store, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    query_received(remote.node_id, state.node_id, {ip, port}, cluster_header, {socket, ip_vers})

    valid_signature = Utils.verify_signature(cluster_secret, remote.value, remote.signature)
    hash_matches = Utils.hash(remote.value) == remote.key

    payload =
      if hash_matches and valid_signature do
        ## Get pid of the storage genserver
        storage_pid = state.node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

        Storage.put_value(storage_pid, remote.key, remote.value, remote.ttl)

        ## Sending a store_reply back as an acknowledgement
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store true")
        args = [tid: remote.tid, node_id: state.node_id, key: remote.key, wrote: true]
        KRPCProtocol.encode(:store_reply, args)
      else
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> false")
        args = [tid: remote.tid, node_id: state.node_id, key: remote.key, wrote: false]
        KRPCProtocol.encode(:store_reply, args)
      end

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )

    {:noreply, state}
  end

  ########################
  # Incoming DHT Replies #
  ########################

  def handle_message({:error_reply, error}, _socket, _, ip, port, state) do
    ip_port_str = Utils.tuple_to_ipstr(ip, port)
    Logger.error("[#{ip_port_str}] >> error (#{error.code}: #{error.msg})")

    {:noreply, state}
  end

  def handle_message(
        {:find_node_reply, remote},
        {socket, ip_vers},
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_node_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, Search, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        Search.handle_reply(pid, remote, remote.nodes)
    end

    ## Ping all nodes
    payload = KRPCProtocol.encode(:ping, node_id: state.node_id)
    payload = Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))

    Enum.each(remote.nodes, fn node_tuple ->
      {_id, ip, port} = node_tuple
      :gen_udp.send(socket, ip, port, payload)
    end)

    {:noreply, state}
  end

  def handle_message(
        {:store_name_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_name_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    case MlDHT.Registry.lookup(remote.tid) do
      [] ->
        tid_enc = Utils.encode_human(remote.tid)

        Logger.debug(
          "[#{Utils.encode_human(remote.node_id)}] no pids to send name tid: #{tid_enc} "
        )

      pids ->
        for {pid, _} <- pids do
          send(pid, {:store_name_reply, remote.node_id, remote.tid, remote.name})
        end
    end

    {:noreply, state}
  end

  def handle_message(
        {:store_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    case MlDHT.Registry.lookup(remote.tid) do
      [] ->
        tid_enc = Utils.encode_human(remote.tid)

        Logger.debug(
          "[#{Utils.encode_human(remote.node_id)}] no pids to send value tid: #{tid_enc} "
        )

      pids ->
        for {pid, _} <- pids do
          send(pid, {:store_reply, remote.node_id, remote.tid, remote.key})
        end
    end

    {:noreply, state}
  end

  def handle_message(
        {:get_peer_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> get_peer_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, Search, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        Search.handle_reply(pid, remote, remote.nodes)
    end

    {:noreply, state}
  end

  def handle_message(
        {:find_value_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_value_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, SearchValue, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        SearchValue.handle_reply(pid, remote, remote.key, remote.value)
    end

    {:noreply, state}
  end

  def handle_message(
        {:find_name_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_name_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, SearchName, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        SearchName.handle_reply(pid, remote, remote.name, remote.value, remote.generation)
    end

    {:noreply, state}
  end

  def handle_message(
        {:find_value_nodes_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_value_nodes_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, SearchValue, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        SearchValue.handle_nodes_reply(pid, remote, remote.nodes)
    end

    {:noreply, state}
  end

  def handle_message(
        {:find_name_nodes_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_name_nodes_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case MlDHT.Registry.get_pid(state.node_id_enc, SearchName, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        SearchName.handle_nodes_reply(pid, remote, remote.nodes)
    end

    {:noreply, state}
  end

  def handle_message(
        {:ping_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> ping_reply")

    response_received(
      remote.node_id,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    {:noreply, state}
  end

  #####################
  # Private Functions #
  #####################

  defp inet_option(:ipv4), do: [:inet]
  defp inet_option(:ipv6), do: [:inet6, {:ipv6_v6only, true}]

  defp maybe_put(list, _name, nil), do: list
  defp maybe_put(list, name, value), do: list ++ [{name, value}]

  ## This function starts a search with the bootstrapping nodes.
  defp bootstrap(state, {socket, inet}) do
    ## Get the nodes which are defined as bootstrapping nodes in the config
    nodes =
      Utils.config(state.config, :bootstrap_nodes)
      |> resolve_hostnames(inet)

    Logger.debug("nodes: #{inspect(nodes)}")

    for {cluster_header, _} <- state.cluster_config do
      ## Start a find_node search to collect neighbors for our routing table
      state.node_id_enc
      |> MlDHT.Registry.get_pid(MlDHT.Search.Supervisor)
      |> MlDHT.Search.Supervisor.start_child(
        :find_node,
        socket,
        state.node_id,
        state.cluster_config
      )
      |> Search.find_node(cluster_header, target: state.node_id, start_nodes: nodes)
    end
  end

  ## function iterates over a list of bootstrapping nodes and tries to
  ## resolve the hostname of each node. If a node is not resolvable the function
  ## removes it; if is resolvable it replaces the hostname with the IP address.
  defp resolve_hostnames(list, inet), do: resolve_hostnames(list, inet, [])
  defp resolve_hostnames([], _inet, result), do: result

  defp resolve_hostnames([{id, host, port} | tail], inet, result) when is_tuple(host) do
    resolve_hostnames(tail, inet, result ++ [{id, host, port}])
  end

  defp resolve_hostnames([{id, host, port} | tail], inet, result) when is_binary(host) do
    case :inet.getaddr(String.to_charlist(host), :inet) do
      {:ok, ip_addr} ->
        resolve_hostnames(tail, inet, result ++ [{id, ip_addr, port}])

      {:error, code} ->
        Logger.error("Couldn't resolve the hostname: #{host} (reason: #{code})")
        resolve_hostnames(tail, inet, result)
    end
  end

  ## Gets a socket as an argument and returns to which ip version (:ipv4 or
  ## :ipv6) the socket belongs.
  @spec get_ip_vers(port) :: ip_vers
  defp get_ip_vers(socket) when is_port(socket) do
    case :inet.getopts(socket, [:ipv6_v6only]) do
      {:ok, [ipv6_v6only: true]} -> :ipv6
      {:ok, []} -> :ipv4
    end
  end

  defp send_ping_reply(node_id, tid, {cluster_header, cluster_secret}, ip, port, socket) do
    Logger.debug("[#{Utils.encode_human(node_id)}] << ping_reply")

    payload = KRPCProtocol.encode(:ping_reply, tid: tid, node_id: node_id)

    :gen_udp.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    )
  end

  # TODO query_received and response_received are nearly identical

  defp query_received(remote_node_id, node_id, ip_port, cluster, {socket, ip_vers}) do
    rtable = node_id |> get_rtable(cluster, ip_vers)

    if node_pid = MlDHT.RoutingTable.Worker.get(rtable, remote_node_id) do
      Node.query_received(node_pid)
      index = Node.bucket_index(node_pid)
      MlDHT.RoutingTable.Worker.update_bucket(rtable, index)
    else
      MlDHT.RoutingTable.Worker.add(rtable, remote_node_id, ip_port, socket)
    end
  end

  defp response_received(remote_node_id, node_id, ip_port, cluster, {socket, ip_vers}) do
    rtable = node_id |> get_rtable(cluster, ip_vers)

    if node_pid = MlDHT.RoutingTable.Worker.get(rtable, remote_node_id) do
      Node.response_received(node_pid)
      index = Node.bucket_index(node_pid)
      MlDHT.RoutingTable.Worker.update_bucket(rtable, index)
    else
      MlDHT.RoutingTable.Worker.add(rtable, remote_node_id, ip_port, socket)
    end
  end

  defp token_match(tok, ip, port, secret, nil) do
    new_str = Utils.tuple_to_ipstr(ip, port) <> secret
    new_tok = Utils.hash(new_str)

    tok == new_tok
  end

  defp token_match(tok, ip, port, secret, old_secret) do
    token_match(tok, ip, port, secret, nil) or
      token_match(tok, ip, port, old_secret, nil)
  end
end
