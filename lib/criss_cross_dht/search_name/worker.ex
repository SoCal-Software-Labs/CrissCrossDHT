defmodule CrissCrossDHT.SearchName.Worker do
  @moduledoc false

  @typedoc """
  A transaction_id (tid) is a two bytes binary.
  """
  @type transaction_id :: <<_::16>>

  @typedoc """
  A DHT search is divided in a :find_name search.
  """
  @type search_type :: :find_name

  use GenServer, restart: :temporary

  require Logger

  alias CrissCrossDHT.RoutingTable.Distance
  alias CrissCrossDHT.Search.Node
  alias CrissCrossDHT.Server.Utils

  import CrissCrossDHT.Search.Worker,
    only: [
      nodes_to_search_nodes: 1,
      search_completed?: 2,
      get_cluster_info: 2,
      compute_new_nodes: 4,
      update_nodes: 4,
      update_responded_node: 2
    ]

  ##############
  # Client API #
  ##############

  # @spec start_link() :: atom
  def start_link(opts) do
    args = [
      opts[:socket],
      opts[:node_id],
      opts[:ip_tuple],
      opts[:type],
      opts[:tid],
      opts[:name],
      opts[:cluster]
    ]

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def find_name(pid, cluster, args), do: GenServer.cast(pid, {:find_name, cluster, args})

  @doc """
  Stops a search process.
  """
  @spec stop(pid) :: :ok
  def stop(pid), do: GenServer.call(pid, :stop)

  @doc """
  Returns the type of the search process.
  """
  @spec type(pid) :: search_type
  def type(pid), do: GenServer.call(pid, :type)

  def tid(pid), do: GenServer.call(pid, :tid)

  #  @spec handle_reply(pid, foo, binary, binary) :: :ok
  def handle_reply(pid, remote, key, value, generation) do
    GenServer.cast(pid, {:handle_reply, remote, key, value, generation})
  end

  def handle_nodes_reply(pid, remote, nodes) do
    GenServer.cast(pid, {:handle_nodes_reply, remote, nodes})
  end

  ####################
  # Server Callbacks #
  ####################

  def init([socket, node_id, ip_tuple, type, tid, name, cluster_config]) do
    ## Extract the id from the via string
    {_, _, {_, id}} = name

    {:ok,
     %{
       :socket => socket,
       :node_id => node_id,
       :ip_tuple => ip_tuple,
       :type => type,
       :tid => tid,
       :name => id,
       :cluster_config => cluster_config,
       :completed => false
     }}
  end

  def wind_down(state) do
    CrissCrossDHT.Registry.unregister(state.name)
    {:stop, :normal, state}
  end

  def handle_info({:search_iterate, cluster_info}, state) do
    if state.completed or search_completed?(state.nodes, state.hashed_target) do
      Logger.debug("SearchName is complete")

      if not state.completed do
        state.callback.(nil, :not_found)
      end

      wind_down(state)
    else
      ## Send queries to the 3 closest nodes
      new_state =
        state.nodes
        |> Distance.closest_nodes(state.hashed_target)
        |> Enum.filter(fn x ->
          x.responded == false and
            x.requested < 3 and
            Node.last_time_requested(x) > 5
        end)
        |> Enum.slice(0..2)
        |> nodesinspector()
        |> send_queries(cluster_info, state)

      Process.send_after(self(), {:search_iterate, cluster_info}, 500)

      {:noreply, new_state}
    end
  end

  def nodesinspector(nodes) do
    # Logger.error "#{inspect nodes}"
    nodes
  end

  def handle_call(:stop, _from, state) do
    CrissCrossDHT.Registry.unregister(state.name)

    {:stop, :normal, :ok, state}
  end

  def handle_call(:type, _from, state) do
    {:reply, state.type, state}
  end

  def handle_call(:tid, _from, state) do
    {:reply, state.tid, state}
  end

  def handle_cast({:find_name, cluster, args}, state) do
    case get_cluster_info(cluster, state) do
      nil ->
        Logger.error("find_name cluster not configured: #{Utils.encode_human(cluster)}")
        {:noreply, state}

      cluster_info ->
        args = Keyword.put(args, :hashed_target, Utils.simple_hash(args[:target]))
        new_state = start_search(:find_name, args, cluster_info, state)
        {:noreply, new_state}
    end
  end

  def handle_cast({:handle_reply, remote, key, value, _generation}, state) do
    old_nodes = update_responded_node(state.nodes, remote)

    # TODO: Verify value with public key
    if key == state.target do
      state.callback.(remote, value)
      wind_down(%{state | completed: true, nodes: old_nodes})
    else
      {:noreply, %{state | nodes: old_nodes}}
    end
  end

  def handle_cast({:handle_nodes_reply, remote, nodes}, state) do
    old_nodes = update_responded_node(state.nodes, remote)
    new_nodes = compute_new_nodes(nodes, old_nodes, state.ip_tuple, state.node_id)

    {:noreply, %{state | nodes: old_nodes ++ new_nodes}}
  end

  #####################
  # Private Functions #
  #####################

  def send_announce_msg(cluster_header, %{cypher: cypher}, state) do
    state.nodes
    |> Distance.closest_nodes(state.hashed_target, 7)
    |> Enum.filter(fn node -> node.responded == true end)
    |> Enum.each(fn node ->
      Logger.debug("[#{Utils.encode_human(node.id)}] << announce_peer")

      args = [node_id: state.node_id, info_hash: state.target, token: node.token, port: node.port]
      args = if state.port == 0, do: args ++ [implied_port: true], else: args

      payload = KRPCProtocol.encode(:announce_peer, args)
      payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
      CrissCrossDHT.UDPQuic.send(state.socket, node.ip, node.port, payload)
    end)
  end

  ## This function merges args (keyword list) with the state map and returns a
  ## function depending on the type (:find_name).
  defp start_search(type, args, cluster_info, state) do
    send(self(), {:search_iterate, cluster_info})

    ## Convert the keyword list to a map and merge it with state.
    args
    |> Enum.into(%{})
    |> Map.merge(state)
    |> Map.put(:type, type)
    |> Map.put(:nodes, nodes_to_search_nodes(args[:start_nodes]))
  end

  defp send_queries([], _, state), do: state

  defp send_queries([node | rest], {cluster_header, %{cypher: cypher}} = cluster_info, state) do
    node_id_enc = node.id |> Utils.encode_human()
    Logger.debug("[#{node_id_enc}] << #{state.type}")

    payload = gen_request_msg(state.type, state)
    payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
    CrissCrossDHT.UDPQuic.send(state.socket, node.ip, node.port, payload)

    new_nodes =
      state.nodes
      |> update_nodes(node.id, :requested, &(&1.requested + 1))
      |> update_nodes(node.id, :request_sent, fn _ -> :os.system_time(:millisecond) end)

    send_queries(rest, cluster_info, %{state | nodes: new_nodes})
  end

  defp gen_request_msg(:find_name, state) do
    args = [
      tid: state.tid,
      node_id: state.node_id,
      name: state.target,
      generation: state.generation
    ]

    KRPCProtocol.encode(:find_name, args)
  end
end
