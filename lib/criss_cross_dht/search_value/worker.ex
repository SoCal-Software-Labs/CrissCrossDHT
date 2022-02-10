defmodule CrissCrossDHT.SearchValue.Worker do
  @moduledoc false

  @typedoc """
  A transaction_id (tid) is a two bytes binary.
  """
  @type transaction_id :: <<_::16>>

  @typedoc """
  A DHT search is divided in a :find_value search.
  """
  @type search_type :: :find_value

  use GenServer, restart: :temporary

  require Logger

  alias CrissCrossDHT.RoutingTable.Distance
  alias CrissCrossDHT.Search.Node
  alias CrissCrossDHT.Server.Worker
  alias CrissCrossDHT.Server.Utils

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
      opts[:clusters]
    ]

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def find_value(pid, cluster, args), do: GenServer.cast(pid, {:find_value, cluster, args})

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
  def handle_reply(pid, remote, key, value) do
    GenServer.cast(pid, {:handle_reply, remote, key, value})
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

  def handle_info({:search_iterate, {cluster_header, cluster_secret} = cluster_info}, state) do
    if state.completed or search_completed?(state.nodes, state.target) do
      Logger.debug("SearchValue is complete")

      if not state.completed do
        state.callback.(nil, :not_found)
      end

      wind_down(state)
    else
      ## Send queries to the 3 closest nodes
      new_state =
        state.nodes
        |> Distance.closest_nodes(state.target)
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

  def handle_cast({:find_value, cluster, args}, state) do
    case get_cluster_info(cluster, state) do
      nil ->
        Logger.error("find_value cluster not configured: #{Utils.encode_human(cluster)}")
        {:noreply, state}

      cluster_info ->
        new_state = start_search(:find_value, args, cluster_info, state)
        {:noreply, new_state}
    end
  end

  def handle_cast({:handle_reply, remote, key, value}, state) do
    old_nodes = update_responded_node(state.nodes, remote)

    if key == state.target and key == Utils.hash(value) do
      state.callback.(remote, value)
      wind_down(%{state | completed: true, nodes: old_nodes})
    else
      {:noreply, %{state | nodes: old_nodes}}
    end
  end

  def handle_cast({:handle_nodes_reply, remote, nodes}, state) do
    old_nodes = update_responded_node(state.nodes, remote)

    new_nodes =
      Enum.map(nodes, fn node ->
        {id, ip, port} = node

        unless Enum.find(state.nodes, fn x -> x.id == id or {ip, port} == state.ip_tuple end) do
          %Node{id: id, ip: ip, port: port}
        end
      end)
      |> Enum.filter(fn x -> x != nil end)

    new_state = %{state | nodes: old_nodes ++ new_nodes}

    if search_completed?(new_state.nodes, new_state.target) do
      state.callback.(nil, :not_found)
      wind_down(new_state)
    else
      {:noreply, new_state}
    end
  end

  #####################
  # Private Functions #
  #####################

  def send_announce_msg(cluster_header, cluster_secret, state) do
    state.nodes
    |> Distance.closest_nodes(state.target, 7)
    |> Enum.filter(fn node -> node.responded == true end)
    |> Enum.each(fn node ->
      Logger.debug("[#{Utils.encode_human(node.id)}] << announce_peer")

      args = [node_id: state.node_id, info_hash: state.target, token: node.token, port: node.port]
      args = if state.port == 0, do: args ++ [implied_port: true], else: args

      payload = KRPCProtocol.encode(:announce_peer, args)
      payload = Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
      :gen_udp.send(state.socket, node.ip, node.port, payload)
    end)
  end

  ## This function merges args (keyword list) with the state map and returns a
  ## function depending on the type (:find_value).
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

  defp send_queries([node | rest], {cluster_header, cluster_secret} = cluster_info, state) do
    node_id_enc = node.id |> Utils.encode_human()
    Logger.debug("[#{node_id_enc}] << #{state.type}")

    payload = gen_request_msg(state.type, state)
    payload = Utils.wrap(cluster_header, Utils.encrypt(cluster_secret, payload))
    :gen_udp.send(state.socket, node.ip, node.port, payload)

    new_nodes =
      state.nodes
      |> update_nodes(node.id, :requested, &(&1.requested + 1))
      |> update_nodes(node.id, :request_sent, fn _ -> :os.system_time(:millisecond) end)

    send_queries(rest, cluster_info, %{state | nodes: new_nodes})
  end

  defp nodes_to_search_nodes(nodes) do
    Enum.map(nodes, fn node ->
      {id, ip, port} = extract_node_infos(node)
      %Node{id: id, ip: ip, port: port}
    end)
  end

  defp gen_request_msg(:find_value, state) do
    args = [tid: state.tid, node_id: state.node_id, key: state.target]
    KRPCProtocol.encode(:find_value, args)
  end

  ## It is necessary that we need to know which node in our node list has
  ## responded. This function goes through the node list and sets :responded of
  ## the responded node to true. If the reply from the remote node also contains
  ## a token this function updates this too.
  defp update_responded_node(nodes, remote) do
    node_list = update_nodes(nodes, remote.node_id, :responded)

    if Map.has_key?(remote, :token) do
      update_nodes(node_list, remote.node_id, :token, fn _ -> remote.token end)
    else
      node_list
    end
  end

  ## This function is a helper function to update the node list easily.
  defp update_nodes(nodes, node_id, key) do
    update_nodes(nodes, node_id, key, fn _ -> true end)
  end

  defp update_nodes(nodes, node_id, key, func) do
    Enum.map(nodes, fn node ->
      if node.id == node_id do
        Map.put(node, key, func.(node))
      else
        node
      end
    end)
  end

  defp extract_node_infos(node) when is_tuple(node), do: node

  defp extract_node_infos(node) when is_pid(node) do
    CrissCrossDHT.RoutingTable.Node.to_tuple(node)
  end

  ## This function contains the condition when a search is completed.
  defp search_completed?(nodes, target) do
    nodes
    |> Distance.closest_nodes(target, 7)
    |> Enum.all?(fn node ->
      node.responded == true or node.requested >= 3
    end)
  end

  defp get_cluster_info(cluster_header, %{cluster_config: cluster_config}) do
    case cluster_config do
      %{^cluster_header => cluster_secret} -> {cluster_header, cluster_secret}
      _ -> nil
    end
  end
end
