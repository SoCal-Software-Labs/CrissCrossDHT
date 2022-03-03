defmodule CrissCrossDHT.Server.Worker do
  @moduledoc false

  use GenServer

  require Logger

  alias CrissCrossDHT.Server.Utils
  alias CrissCrossDHT.Server.Storage
  alias CrissCrossDHT.Registry

  alias CrissCrossDHT.RoutingTable.Node
  alias CrissCrossDHT.Search.Worker, as: Search
  alias CrissCrossDHT.SearchValue.Worker, as: SearchValue
  alias CrissCrossDHT.SearchName.Worker, as: SearchName
  alias CrissCrossDHT.UDPQuic

  @type ip_vers :: :ipv4 | :ipv6

  # Time after the secret changes
  @time_cluster_secret :timer.minutes(5)

  @reannounce_interval :timer.minutes(30)

  @process_values_interval :timer.seconds(10)

  @bootstrap_cluster_interval :timer.seconds(60)

  @max_stored_bytesize 120

  @search_limit_ttl :timer.seconds(5)

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
  iex> CrissCrossDHT.DHTServer.Worker.bootstrap
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
  iex> CrissCrossDHT.DHTServer.search(infohash, fn(node) ->
  {ip, port} = node
  IO.puts "ip: #{ip} port: #{port}"
  end)
  """
  def search(pid, cluster, infohash, callback) do
    GenServer.cast(pid, {:search, cluster, infohash, callback})
  end

  def search_announce(pid, cluster, infohash, port, meta, ttl, callback) do
    ttl = Utils.adjust_ttl(ttl)
    GenServer.cast(pid, {:search_announce, cluster, infohash, callback, ttl, port, meta})
  end

  def cluster_announce(pid, cluster, infohash, ttl) do
    ttl = Utils.adjust_ttl(ttl)
    GenServer.cast(pid, {:cluster_announce, cluster, infohash, ttl})
  end

  def has_announced_cluster(pid, cluster, infohash) do
    GenServer.call(pid, {:has_announced_cluster, cluster, infohash})
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
    ttl = Utils.adjust_ttl(ttl)
    key = Utils.hash(value)
    :ok = GenServer.cast(pid, {:store, cluster, key, value, ttl, tid})
    {key, tid}
  end

  def store_name(pid, cluster, priv_key, value, local, remote, ttl) do
    tid = KRPCProtocol.gen_tid()
    store_name(pid, cluster, priv_key, value, local, remote, ttl, tid)
  end

  def store_name(pid, cluster, priv_key, value, local, remote, ttl, tid) do
    ttl = Utils.adjust_ttl(ttl)
    {:ok, public_key} = ExSchnorr.public_from_private(priv_key)
    {:ok, key_string} = ExSchnorr.public_to_bytes(public_key)
    public_key_hash = Utils.hash(key_string)
    name = Utils.hash(public_key_hash)

    :ok =
      GenServer.cast(
        pid,
        {:store_name, cluster, priv_key, key_string, public_key_hash, name, value, local, remote,
         ttl, tid}
      )

    {name, tid}
  end

  def create_udp_socket(ip_addr, port, dispatcher) do
    case UDPQuic.open(ip_addr, port, dispatcher) do
      {:ok, socket, bind_addr} ->
        [l, r] = String.split(bind_addr, ":")
        {:ok, addr} = :inet.parse_address(String.to_charlist(l))

        {socket, addr, String.to_integer(r)}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def init(node_id: node_id, config: config) do
    # external_ip = Utils.config(config, :ipv4_addr, {127, 0, 0, 1})
    cfg_ip = Utils.config(config, :bind_ip, {127, 0, 0, 1})
    cfg_port = Utils.config(config, :port)
    dispatcher = Utils.config(config, :dispatcher)
    {storage_mod, _} = Utils.config(config, :storage)
    process_values_callback = Utils.config(config, :process_values_callback)

    {socket, external_ip, socket_port} = create_udp_socket(cfg_ip, cfg_port, dispatcher)

    Process.send_after(self(), :cluster_secret, @time_cluster_secret)
    Process.send_after(self(), :reannounce, @reannounce_interval)
    Process.send_after(self(), :process_values, @process_values_interval)
    Process.send_after(self(), {:bootstrap_clusters, false}, 2_000)
    Process.send_after(self(), {:bootstrap_clusters, false}, 10_000)
    Process.send_after(self(), {:bootstrap_clusters, true}, 45_000)

    cache = :ets.new(:key_cache, [:set, :public])
    storage_pid = node_id |> Utils.encode_human() |> Registry.get_pid(Storage)

    real_node_id = Utils.hash(node_id <> Utils.tuple_to_ipstr(external_ip, socket_port))

    state = %{
      node_id: real_node_id,
      node_id_enc: Utils.encode_human(node_id),
      socket: socket,
      old_secret: nil,
      secret: Utils.gen_secret(),
      cluster_mod: CrissCrossDHT.ClusterWatcher,
      config: config,
      self_pid: self(),
      cache: cache,
      ip_tuple: {external_ip, socket_port},
      storage_mod: storage_mod,
      storage_pid: storage_pid,
      process_values_callback: process_values_callback,
      bootstrap_overlay: config.bootstrap_overlay
    }

    Logger.info(
      "Registering real node name #{Utils.encode_human(real_node_id)} on #{Utils.tuple_to_ipstr(external_ip, socket_port)}"
    )

    send(self(), {:start_rtable, external_ip, cfg_port})
    {:ok, state}
  end

  defp get_rtable(node_id_enc, header, rt_ident) do
    rt_name = Utils.encode_human(header) <> to_string(rt_ident)

    node_id_enc
    |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.RoutingTable.Worker, rt_name)
  end

  def get_local_nodes(state, cluster_header, infohash, callback) do
    if state.storage_mod.has_nodes_for_infohash?(
         state.storage_pid,
         cluster_header,
         infohash
       ) do
      values = state.storage_mod.get_nodes(state.storage_pid, cluster_header, infohash)

      Enum.map(values, callback)
    end
  end

  def handle_call({:has_announced_cluster, cluster, infohash}, _, state) do
    has_announced = state.storage_mod.has_announced_cluster(state.storage_pid, cluster, infohash)
    {:reply, has_announced, state}
  end

  def handle_cast({:bootstrap, socket_tuple}, state) do
    bootstrap(state, socket_tuple)

    {:noreply, state}
  end

  def handle_cast({:store, cluster_header, key, value, ttl, tid}, state) do
    # TODO What about ipv6?
    Logger.debug("[*] >> store #{Utils.encode_human(tid)}")

    case state.cluster_mod.get_cluster(cluster_header) do
      %{private_key: private_key, cypher: cypher}
      when not is_nil(private_key) ->
        nodes =
          state.node_id_enc
          |> get_rtable(cluster_header, :ipv4)
          |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(key)

        {:ok, signature} = Utils.sign(value, private_key)

        args = [
          tid: tid,
          node_id: state.node_id,
          key: key,
          value: value,
          ttl: ttl,
          signature: signature
        ]

        payload = KRPCProtocol.encode(:store, args)
        payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))

        for node <- CrissCrossDHT.Search.Worker.nodes_to_search_nodes(nodes) do
          UDPQuic.send(state.socket, node.ip, node.port, payload)
        end

      _ ->
        Logger.error("RSA key not configured for cluster #{Utils.encode_human(cluster_header)}")
        :ok
    end

    {:noreply, state}
  end

  def handle_cast(
        {:broadcast_name, cluster_header, tid, name, value, ttl, key_string, generation,
         signature_cluster, signature_name},
        state
      ) do
    args = [
      tid: tid,
      node_id: state.node_id,
      name: name,
      value: value,
      ttl: ttl,
      key_string: key_string,
      generation: generation,
      signature: signature_cluster,
      signature_ns: signature_name
    ]

    case state.cluster_mod.get_cluster(cluster_header) do
      %{cypher: cypher} ->
        payload = KRPCProtocol.encode(:store_name, args)
        payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))

        nodes =
          state.node_id_enc
          |> get_rtable(cluster_header, :ipv4)
          |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(name)

        for node <- CrissCrossDHT.Search.Worker.nodes_to_search_nodes(nodes) do
          UDPQuic.send(state.socket, node.ip, node.port, payload)
        end

      _ ->
        Logger.error("Could not find cypher for #{cluster_header}")
    end

    {:noreply, state}
  end

  def handle_cast(
        {:store_name, cluster_header, rsa_priv_name_key, key_string, _public_key_hash, name,
         value, local, remote, ttl, tid},
        state
      ) do
    # TODO What about ipv6?
    Logger.debug("[*] >> store_name")

    case state.cluster_mod.get_cluster(cluster_header) do
      %{private_key: rsa_priv_key}
      when not is_nil(rsa_priv_key) ->
        {:ok, signature} = Utils.sign(value, rsa_priv_key)

        {generation, to_sign} =
          case state.storage_mod.get_name(state.storage_pid, cluster_header, name) do
            nil ->
              {0, Utils.combine_to_sign([ttl, 0, signature])}

            {_value, generation, _ttl, _key, _signature_cluster, _signature} ->
              {generation + 1, Utils.combine_to_sign([ttl, generation + 1, signature])}
          end

        {:ok, signature_ns} = Utils.sign(to_sign, rsa_priv_name_key)

        if local do
          state.storage_mod.put_name(
            state.storage_pid,
            cluster_header,
            name,
            value,
            generation,
            key_string,
            signature,
            signature_ns,
            ttl
          )
        end

        if remote do
          GenServer.cast(
            self(),
            {:broadcast_name, cluster_header, tid, name, value, ttl, key_string, generation,
             signature, signature_ns}
          )
        end

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_cast({:find_name, cluster, infohash, rem_generation, callback}, state) do
    case state.storage_mod.get_name(state.storage_pid, cluster, infohash) do
      {value, generation, _ttl, _key_string, _signature_cluster, _signature_name}
      when generation > rem_generation ->
        callback.(nil, value)
        {:noreply, state}

      _ ->
        nodes =
          state.node_id_enc
          |> get_rtable(cluster, :ipv4)
          |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(infohash)

        state.node_id_enc
        |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.SearchName.Supervisor)
        |> CrissCrossDHT.SearchName.Supervisor.start_child(
          :find_name,
          state.socket,
          state.node_id,
          state.node_id_enc,
          state.ip_tuple,
          state.cluster_mod.get_cluster(cluster)
        )
        |> CrissCrossDHT.SearchName.Worker.find_name(
          cluster,
          target: infohash,
          generation: rem_generation,
          start_nodes: nodes,
          callback: callback
        )

        {:noreply, state}
    end
  end

  def handle_cast({:find_value, cluster, infohash, callback}, state) do
    # TODO What about ipv6?
    nodes =
      state.node_id_enc
      |> get_rtable(cluster, :ipv4)
      |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.SearchValue.Supervisor)
    |> CrissCrossDHT.SearchValue.Supervisor.start_child(
      :find_value,
      state.socket,
      state.node_id,
      state.node_id_enc,
      state.ip_tuple,
      state.cluster_mod.get_cluster(cluster)
    )
    |> CrissCrossDHT.SearchValue.Worker.find_value(
      cluster,
      target: infohash,
      start_nodes: nodes,
      callback: callback
    )

    {:noreply, state}
  end

  def handle_cast({:cluster_announce, cluster, infohash, ttl}, state) do
    :ok = state.storage_mod.cluster_announce(state.storage_pid, cluster, infohash, ttl)
    {:noreply, state}
  end

  def handle_cast({:search_announce, cluster, infohash, callback, ttl, port, meta}, state) do
    Cachex.put!(:search_limit, {cluster, infohash}, true, ttl: @search_limit_ttl)

    nodes =
      state.node_id_enc
      |> get_rtable(cluster, :ipv4)
      |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(infohash)

    state.node_id_enc
    |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.Search.Supervisor)
    |> CrissCrossDHT.Search.Supervisor.start_child(
      :get_peers,
      state.socket,
      state.node_id,
      state.node_id_enc,
      state.ip_tuple,
      state.cluster_mod.get_cluster(cluster)
    )
    |> Search.get_peers(
      cluster,
      target: infohash,
      start_nodes: nodes,
      callback: callback,
      port: port,
      meta: meta,
      announce: true,
      ttl: ttl
    )

    {ip, _port} = state.ip_tuple

    state.storage_mod.put(
      state.storage_pid,
      cluster,
      infohash,
      ip,
      port,
      meta,
      ttl
    )

    state.storage_mod.queue_announce(
      state.storage_pid,
      cluster,
      infohash,
      ip,
      port,
      meta,
      ttl
    )

    get_local_nodes(state, cluster, infohash, callback)

    :ok = state.storage_mod.cluster_announce(state.storage_pid, cluster, infohash, ttl)

    {:noreply, state}
  end

  def handle_cast({:search, cluster, infohash, callback}, state) do
    if Cachex.get!(:search_limit, {cluster, infohash}) == nil do
      Cachex.put!(:search_limit, {cluster, infohash}, true, ttl: @search_limit_ttl)

      nodes =
        state.node_id_enc
        |> get_rtable(cluster, :ipv4)
        |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(infohash)

      state.node_id_enc
      |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.Search.Supervisor)
      |> CrissCrossDHT.Search.Supervisor.start_child(
        :get_peers,
        state.socket,
        state.node_id,
        state.node_id_enc,
        state.ip_tuple,
        state.cluster_mod.get_cluster(cluster)
      )
      |> Search.get_peers(
        cluster,
        target: infohash,
        start_nodes: nodes,
        port: 0,
        meta: nil,
        callback: callback,
        announce: false
      )
    end

    get_local_nodes(state, cluster, infohash, callback)

    {:noreply, state}
  end

  def handle_info(:cluster_secret, state) do
    Logger.debug("Change Secret")
    Process.send_after(self(), :cluster_secret, @time_cluster_secret)

    {:noreply, %{state | old_secret: state.secret, secret: Utils.gen_secret()}}
  end

  def handle_info({:udp, socket, ip, port, raw_data}, state) do
    if {ip, port} != state.ip_tuple do
      Task.start(fn ->
        {cluster_header, body} =
          raw_data
          |> Utils.unwrap()

        case state.cluster_mod.get_cluster(cluster_header) do
          %{cypher: cypher} = cluster_secret ->
            decrypted = Utils.decrypt(body, cypher)

            case decrypted do
              c when is_binary(c) ->
                c
                |> String.trim_trailing("\n")
                |> KRPCProtocol.decode()
                |> handle_message(
                  {socket, :ipv4},
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
            Logger.warning(
              "Could not find cluster configured #{Utils.encode_human(cluster_header)}"
            )

            {:noreply, state}
        end
      end)
    end

    {:noreply, state}
  end

  def handle_info(:reannounce, state) do
    Logger.debug("Reannouncing trees to peers")

    pid = self()

    Task.start(fn ->
      state.storage_mod.reannounce_trees(state.storage_pid, pid)
      state.storage_mod.reannounce_names(state.storage_pid, pid)
      Process.send_after(pid, :reannounce, @reannounce_interval)
    end)

    {:noreply, state}
  end

  def handle_info(:process_values, state) do
    Logger.debug("Checking for queued trees to clone")

    pid = self()

    Task.start(fn ->
      state.storage_mod.process_values(state.storage_pid, state.process_values_callback)
      Process.send_after(pid, :process_values, @process_values_interval)
    end)

    {:noreply, state}
  end

  def handle_info({:bootstrap_clusters, do_again}, %{ip_tuple: {_, port}} = state) do
    Logger.debug("Searching for new nodes for clusters")
    worker_pid = self()

    Task.start(fn ->
      bootstrap(state, {state.socket, :inet})

      for {cluster_header, _} <-
            Map.delete(state.cluster_mod.get_clusters(), state.bootstrap_overlay) do
        ## Start a find_node search to collect neighbors for our routing table
        rtable = state.node_id_enc |> get_rtable(state.bootstrap_overlay, :ipv4)

        nodes =
          rtable
          |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(cluster_header)

        task =
          Task.async(fn ->
            ref = make_ref()
            pid = self()

            callback = fn
              :done ->
                send(pid, {ref, :done})

              {ip, port, _meta, _ttl} = node ->
                if state.ip_tuple != {ip, port} do
                  send(pid, {ref, node})
                end
            end

            ttl = Utils.adjust_ttl(@bootstrap_cluster_interval * 5)

            state.node_id_enc
            |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.Search.Supervisor)
            |> CrissCrossDHT.Search.Supervisor.start_child(
              :get_peers,
              state.socket,
              state.node_id,
              state.node_id_enc,
              state.ip_tuple,
              state.cluster_mod.get_cluster(state.bootstrap_overlay)
            )
            |> Search.get_peers(
              state.bootstrap_overlay,
              target: cluster_header,
              start_nodes: nodes,
              callback: callback,
              port: port,
              meta: nil,
              announce: true,
              ttl: ttl
            )

            {ip, port} = state.ip_tuple

            state.storage_mod.put(
              state.storage_pid,
              state.bootstrap_overlay,
              cluster_header,
              ip,
              port,
              nil,
              ttl
            )

            Enum.reduce_while(1..2, [], fn _, acc ->
              receive do
                {^ref, :done} ->
                  {:halt, acc}

                {^ref, {ip, port, _meta, _ttl}} ->
                  node = CrissCrossDHT.RoutingTable.Worker.get_by_ip(rtable, {ip, port})

                  case node do
                    nil -> {:cont, acc}
                    _ -> {:cont, [node | acc]}
                  end
              after
                1000 -> {:halt, acc}
              end
            end)
          end)

        bootstrap_nodes = Task.await(task)

        Logger.debug("Bootstrapping with #{inspect(bootstrap_nodes)}")

        state.node_id_enc
        |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.Search.Supervisor)
        |> CrissCrossDHT.Search.Supervisor.start_child(
          :find_node,
          state.socket,
          state.node_id,
          state.node_id_enc,
          state.ip_tuple,
          state.cluster_mod.get_cluster(cluster_header)
        )
        |> Search.find_node(cluster_header, target: state.node_id, start_nodes: bootstrap_nodes)
      end

      if do_again do
        Process.send_after(worker_pid, {:bootstrap_clusters, true}, @bootstrap_cluster_interval)
      end
    end)

    {:noreply, state}
  end

  def handle_info({:start_rtable, socket_ip, cfg_port}, state) do
    for {cluster_header, cluster_secret} <- state.cluster_mod.get_clusters() do
      start_rtable(
        state.node_id,
        state.node_id_enc,
        cluster_header,
        cluster_secret,
        :ipv4,
        {socket_ip, cfg_port}
      )
    end

    {:noreply, state}
  end

  defp start_rtable(node_id, node_id_enc, header, cluster_secret, rt_ident, ip_tuple) do
    rt_name = Utils.encode_human(header) <> to_string(rt_ident)

    ## Allows giving atoms as rt_name to this function, e.g. :ipv4
    {:ok, _pid} =
      node_id_enc
      |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.RoutingTable.Supervisor)
      |> DynamicSupervisor.start_child({
        CrissCrossDHT.RoutingTable.Supervisor,
        node_id: node_id,
        ip_tuple: ip_tuple,
        node_id_enc: node_id_enc,
        cluster: header,
        cluster_secret: cluster_secret,
        rt_name: rt_name
      })

    node_id_enc |> get_rtable(header, rt_ident)
  end

  #########
  # Error #
  #########

  def handle_message(
        {:error, error},
        _socket,
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    args = [code: error.code, msg: error.msg, tid: error.tid]
    payload = KRPCProtocol.encode(:error, args)

    UDPQuic.send(
      state.socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    {:noreply, state}
  end

  def handle_message({:invalid, msg}, _socket, _, _ip, _port, state) do
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

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    if remote.node_id != state.node_id do
      send_ping_reply(
        state.node_id,
        remote.tid,
        {cluster_header, cluster_secret},
        ip,
        port,
        socket
      )
    end

    {:noreply, state}
  end

  def handle_message(
        {:find_node, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    Logger.debug(
      "[#{Utils.encode_human(remote.node_id)}@#{Utils.encode_human(cluster_header)}] >> find_node"
    )

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    ## Get closest nodes for the requested target from the routing table
    nodes =
      state.node_id_enc
      |> get_rtable(cluster_header, ip_vers)
      |> CrissCrossDHT.RoutingTable.Worker.closest_nodes(remote.target, remote.node_id)
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

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    # end

    {:noreply, state}
  end

  ## Get_peers
  def handle_message(
        {:get_peers, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> get_peers")

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    ## Get pid of the storage genserver
    args =
      if state.storage_mod.has_nodes_for_infohash?(
           state.storage_pid,
           cluster_header,
           remote.info_hash
         ) do
        values = state.storage_mod.get_nodes(state.storage_pid, cluster_header, remote.info_hash)

        [
          node_id: state.node_id,
          info_hash: remote.info_hash,
          values: values,
          tid: remote.tid,
          token: token
        ]
      else
        ## Get the closest nodes for the requested info_hash
        rtable = state.node_id_enc |> get_rtable(cluster_header, ip_vers)

        nodes =
          Enum.map(
            CrissCrossDHT.RoutingTable.Worker.closest_nodes(rtable, remote.info_hash),
            fn pid ->
              Node.to_tuple(pid)
            end
          )

        Logger.debug("[#{Utils.encode_human(remote.node_id)}] << get_peers_reply (nodes)")

        [
          node_id: state.node_id,
          info_hash: remote.info_hash,
          nodes: nodes,
          tid: remote.tid,
          token: token
        ]
      end

    Logger.debug("PEERS ARGS: #{inspect(args)}")
    payload = KRPCProtocol.encode(:get_peers_reply, args)

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    {:noreply, state}
  end

  ## Announce_peer
  def handle_message(
        {:announce_peer, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher} = cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> announce_peer")

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    fits_in_ttl = Utils.check_ttl(cluster_secret, remote.ttl)

    if fits_in_ttl and token_match(remote.token, ip, port, state.secret, state.old_secret) do
      Logger.debug("Valid Token")
      Logger.debug("#{inspect(remote)}")

      state.storage_mod.put(
        state.storage_pid,
        cluster_header,
        remote.info_hash,
        ip,
        port,
        remote.meta,
        remote.ttl
      )

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

      UDPQuic.send(
        socket,
        ip,
        port,
        Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
      )

      {:noreply, state}
    end
  end

  ## Find Value
  def handle_message(
        {:find_value, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_value")

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    payload =
      case state.storage_mod.get_value(state.storage_pid, cluster_header, remote.key) do
        nil ->
          rtable = state.node_id_enc |> get_rtable(cluster_header, ip_vers)

          nodes =
            Enum.map(
              CrissCrossDHT.RoutingTable.Worker.closest_nodes(rtable, remote.key),
              fn pid ->
                Node.to_tuple(pid)
              end
            )

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

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    {:noreply, state}
  end

  def handle_message(
        {:find_name, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_name")

    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    ## Generate a token for the requesting node
    token = Utils.hash(Utils.tuple_to_ipstr(ip, port) <> state.secret)

    payload =
      case state.storage_mod.get_name(state.storage_pid, cluster_header, remote.name) do
        {value, generation, ttl, key_string, signature_cluster, signature_name}
        when generation > remote.generation ->
          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_name (value)")

          args = [
            tid: remote.tid,
            node_id: state.node_id,
            name: remote.name,
            value: value,
            generation: generation,
            signature_cluster: signature_cluster,
            signature_name: signature_name,
            key_string: key_string,
            ttl: ttl,
            token: token
          ]

          KRPCProtocol.encode(:find_name_reply, args)

        _ ->
          rtable = state.node_id_enc |> get_rtable(cluster_header, ip_vers)

          nodes =
            Enum.map(
              CrissCrossDHT.RoutingTable.Worker.closest_nodes(rtable, remote.name),
              fn pid ->
                Node.to_tuple(pid)
              end
            )

          Logger.debug("[#{Utils.encode_human(remote.node_id)}] << find_name (nodes)")

          args = [
            node_id: state.node_id,
            name: remote.name,
            nodes: nodes,
            tid: remote.tid,
            token: token
          ]

          KRPCProtocol.encode(:find_name_nodes_reply, args)
      end

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    {:noreply, state}
  end

  ## Store
  def handle_message(
        {:store_name, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher} = cluster_secret},
        ip,
        port,
        state
      ) do
    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    valid_signature =
      Utils.verify_signature(cluster_secret.public_key, remote.value, remote.signature)

    hash_matches =
      byte_size(remote.value) < @max_stored_bytesize and
        Utils.hash(Utils.hash(remote.key_string)) == remote.name and
        Utils.check_generation(
          state.storage_mod,
          state.storage_pid,
          cluster_header,
          remote.name,
          remote.generation
        ) and
        case Utils.load_public_key(remote.key_string) do
          {:ok, public_key} ->
            Utils.verify_signature(
              public_key,
              Utils.combine_to_sign([remote.ttl, remote.generation, remote.signature]),
              remote.signature_ns
            )

          _ ->
            false
        end

    fits_in_ttl = Utils.check_ttl(cluster_secret, remote.ttl)

    val = remote.value
    generation = remote.generation

    should_store_name =
      case state.storage_mod.get_name(state.storage_pid, cluster_header, remote.name) do
        nil ->
          true

        {^val, ^generation, _ttl, _key, _signature_cluster, _signature} ->
          false

        {_, gen, _ttl, _key, _signature_cluster, _signature} when remote.generation > gen ->
          true

        _ ->
          :invalid
      end

    payload =
      if should_store_name != :invalid and fits_in_ttl and hash_matches and valid_signature do
        ## Get pid of the storage genserver
        if should_store_name do
          state.storage_mod.put_name(
            state.storage_pid,
            cluster_header,
            remote.name,
            remote.value,
            remote.generation,
            remote.key_string,
            remote.signature,
            remote.signature_ns,
            remote.ttl
          )
        else
          state.storage_mod.refresh_name(state.storage_pid, cluster_header, remote.name)
        end

        ## Sending a store_reply back as an acknowledgement
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_name true")
        args = [tid: remote.tid, node_id: state.node_id, name: remote.name, wrote: true]
        KRPCProtocol.encode(:store_name_reply, args)
      else
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store_name false")
        args = [tid: remote.tid, node_id: state.node_id, name: remote.name, wrote: false]
        KRPCProtocol.encode(:store_name_reply, args)
      end

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )

    {:noreply, state}
  end

  def handle_message(
        {:store, remote},
        {socket, ip_vers},
        {cluster_header, %{cypher: cypher} = cluster_secret},
        ip,
        port,
        state
      ) do
    query_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    fits_in_ttl = Utils.check_ttl(cluster_secret, remote.ttl)

    valid_signature =
      Utils.verify_signature(cluster_secret.public_key, remote.value, remote.signature)

    hash_matches = Utils.hash(remote.value) == remote.key
    size_ok = byte_size(remote.value) < @max_stored_bytesize

    payload =
      if fits_in_ttl and hash_matches and valid_signature and size_ok do
        Logger.info(
          "Putting #{Utils.encode_human(remote.value)} in queue for cluster #{Utils.encode_human(cluster_header)}"
        )

        state.storage_mod.put_value(
          state.storage_pid,
          cluster_header,
          remote.key,
          remote.value,
          remote.ttl
        )

        ## Sending a store_reply back as an acknowledgement
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store true")
        args = [tid: remote.tid, node_id: state.node_id, key: remote.key, wrote: true]
        KRPCProtocol.encode(:store_reply, args)
      else
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> store false")
        args = [tid: remote.tid, node_id: state.node_id, key: remote.key, wrote: false]
        KRPCProtocol.encode(:store_reply, args)
      end

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
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
        {cluster_header, %{cypher: cypher}},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_node_reply")

    response_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    remote =
      if remote.values do
        new_vals =
          Enum.filter(remote.values, fn
            {_ip, port, meta, ttl}
            when is_number(ttl) and is_number(port) and (is_binary(meta) or is_nil(meta)) ->
              true

            e ->
              Logger.warning("Received find_node_reply with invalid node #{inspect(e)}")
              false
          end)

        Map.put(remote, :values, new_vals)
      else
        remote
      end

    tid_enc = Utils.encode_human(remote.tid)

    case CrissCrossDHT.Registry.get_pid(state.node_id_enc, Search, tid_enc) do
      nil ->
        Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

      pid ->
        Search.handle_reply(pid, remote, remote.nodes)
    end

    ## Ping all nodes
    payload = KRPCProtocol.encode(:ping, node_id: state.node_id)
    payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))

    Enum.each(remote.nodes, fn node_tuple ->
      {id, ip, port} = node_tuple

      if id != state.node_id do
        UDPQuic.send(socket, ip, port, payload)
      end
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
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    case CrissCrossDHT.Registry.lookup(remote.tid) do
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
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    case CrissCrossDHT.Registry.lookup(remote.tid) do
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
        {:get_peers_reply, remote},
        {socket, ip_vers},
        {cluster_header, _},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> get_peers_reply")

    response_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    remote =
      if remote.values do
        new_vals =
          Enum.filter(remote.values, fn
            {_ip, _port, _meta, _ttl} ->
              true

            e ->
              Logger.warning("Received get_peers_reply with invalid node #{inspect(e)}")
              false
          end)

        Map.put(remote, :values, new_vals)
      else
        remote
      end

    if remote.values,
      do:
        Enum.each(remote.values, fn {ip, port, meta, ttl} ->
          state.storage_mod.put(
            state.storage_pid,
            cluster_header,
            remote.info_hash,
            ip,
            port,
            meta,
            ttl
          )
        end)

    case CrissCrossDHT.Registry.get_pid(state.node_id_enc, Search, tid_enc) do
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
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case CrissCrossDHT.Registry.get_pid(state.node_id_enc, SearchValue, tid_enc) do
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
        {cluster_header, cluster_secret},
        ip,
        port,
        state
      ) do
    Logger.debug("[#{Utils.encode_human(remote.node_id)}] >> find_name_reply")

    response_received(
      remote.node_id,
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    valid_cluster_signature =
      Utils.verify_signature(cluster_secret.public_key, remote.value, remote.signature_cluster)

    valid_signature =
      byte_size(remote.value) < @max_stored_bytesize and
        valid_cluster_signature and
        Utils.hash(Utils.hash(remote.key_string)) == remote.name and
        case Utils.load_public_key(remote.key_string) do
          {:ok, public_key} ->
            Utils.verify_signature(
              public_key,
              Utils.combine_to_sign([remote.ttl, remote.generation, remote.signature_cluster]),
              remote.signature_name
            )

          _ ->
            false
        end

    if valid_signature do
      tid_enc = Utils.encode_human(remote.tid)

      state.storage_mod.put_name(
        state.storage_pid,
        cluster_header,
        remote.name,
        remote.value,
        remote.generation,
        remote.key_string,
        remote.signature_cluster,
        remote.signature_name,
        remote.ttl
      )

      case CrissCrossDHT.Registry.get_pid(state.node_id_enc, SearchName, tid_enc) do
        nil ->
          Logger.debug("[#{Utils.encode_human(remote.node_id)}] ignore unknown tid: #{tid_enc} ")

        pid ->
          SearchName.handle_reply(pid, remote, remote.name, remote.value, remote.generation)
      end
    else
      Logger.error("Invalid signature for name reply.")
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
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case CrissCrossDHT.Registry.get_pid(state.node_id_enc, SearchValue, tid_enc) do
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
      state.node_id_enc,
      state.node_id,
      {ip, port},
      cluster_header,
      {socket, ip_vers}
    )

    tid_enc = Utils.encode_human(remote.tid)

    case CrissCrossDHT.Registry.get_pid(state.node_id_enc, SearchName, tid_enc) do
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
      state.node_id_enc,
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

  ## This function starts a search with the bootstrapping nodes.
  defp bootstrap(state, {socket, inet}) do
    ## Get the nodes which are defined as bootstrapping nodes in the config
    nodes =
      Utils.config(state.config, :bootstrap_nodes)
      |> Utils.resolve_hostnames(inet)

    Logger.debug("nodes: #{inspect(nodes)}")

    state.node_id_enc
    |> CrissCrossDHT.Registry.get_pid(CrissCrossDHT.Search.Supervisor)
    |> CrissCrossDHT.Search.Supervisor.start_child(
      :find_node,
      socket,
      state.node_id,
      state.node_id_enc,
      state.ip_tuple,
      state.cluster_mod.get_cluster(state.bootstrap_overlay)
    )
    |> Search.find_node(state.bootstrap_overlay, target: state.node_id, start_nodes: nodes)
  end

  defp send_ping_reply(node_id, tid, {cluster_header, %{cypher: cypher}}, ip, port, socket) do
    Logger.debug("[#{Utils.encode_human(node_id)}] << ping_reply")

    payload = KRPCProtocol.encode(:ping_reply, tid: tid, node_id: node_id)

    UDPQuic.send(
      socket,
      ip,
      port,
      Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    )
  end

  # TODO query_received and response_received are nearly identical

  defp query_received(
         remote_node_id,
         node_id_enc,
         node_id,
         {ip, port} = ip_port,
         cluster,
         {socket, ip_vers}
       ) do
    rtable = node_id_enc |> get_rtable(cluster, ip_vers)

    if node_pid = CrissCrossDHT.RoutingTable.Worker.get(rtable, remote_node_id) do
      Node.query_received(node_pid)
      index = Node.bucket_index(node_pid)
      CrissCrossDHT.RoutingTable.Worker.update_bucket(rtable, index)
    else
      if remote_node_id != node_id do
        Logger.info(
          "Hello #{Utils.encode_human(remote_node_id)}@#{Utils.encode_human(cluster)} @ #{Utils.tuple_to_ipstr(ip, port)}"
        )

        CrissCrossDHT.RoutingTable.Worker.add(rtable, remote_node_id, ip_port, socket)
      end
    end
  end

  defp response_received(
         remote_node_id,
         node_id_enc,
         node_id,
         {ip, port} = ip_port,
         cluster,
         {socket, ip_vers}
       ) do
    rtable = node_id_enc |> get_rtable(cluster, ip_vers)

    if node_pid = CrissCrossDHT.RoutingTable.Worker.get(rtable, remote_node_id) do
      Node.response_received(node_pid)
      index = Node.bucket_index(node_pid)
      CrissCrossDHT.RoutingTable.Worker.update_bucket(rtable, index)
    else
      if remote_node_id != node_id do
        Logger.info(
          "Hello #{Utils.encode_human(remote_node_id)}@#{Utils.encode_human(cluster)} @ #{Utils.tuple_to_ipstr(ip, port)}"
        )

        CrissCrossDHT.RoutingTable.Worker.add(rtable, remote_node_id, ip_port, socket)
      end
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
