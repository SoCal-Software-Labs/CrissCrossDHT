defmodule CrissCrossDHT.Search.Worker do
  @moduledoc false

  @typedoc """
  A transaction_id (tid) is a two bytes binary.
  """
  @type transaction_id :: <<_::16>>

  @typedoc """
  A DHT search is divided in a :get_peers or a :find_node search.
  """
  @type search_type :: :get_peers | :find_node

  use GenServer, restart: :temporary

  require Logger

  alias CrissCrossDHT.RoutingTable.Distance
  alias CrissCrossDHT.Search.Node
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

  def get_peers(pid, cluster, args), do: GenServer.cast(pid, {:get_peers, cluster, args})

  def find_node(pid, cluster, args), do: GenServer.cast(pid, {:find_node, cluster, args})

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

  #  @spec handle_reply(pid, foo, list) :: :ok
  def handle_reply(pid, remote, nodes) do
    GenServer.cast(pid, {:handle_reply, remote, nodes})
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
       :cluster_config => cluster_config
     }}
  end

  def handle_info({:search_iterate, {cluster_header, cluster_secret} = cluster_info}, state) do
    if search_completed?(state.nodes, state.target) do
      Logger.debug("Search is complete")

      case state do
        %{:callback => cb} -> cb.(:done)
        _ -> :ok
      end

      ## If the search is complete and it was get_peers search, then we will
      ## send the clostest peers an announce_peer message.
      if Map.has_key?(state, :announce) and state.announce == true do
        send_announce_msg(cluster_header, cluster_secret, state)
      end

      CrissCrossDHT.Registry.unregister(state.name)

      {:stop, :normal, state}
    else
      ## Send queries to the 3 closest nodes
      new_state =
        state.nodes
        |> nodesinspector()
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
    # Logger.error("#{inspect(nodes)}")
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

  def handle_cast({:get_peers, cluster, args}, state) do
    case get_cluster_info(cluster, state) do
      nil ->
        Logger.error("get_peers cluster not configured: #{Utils.encode_human(cluster)}")
        {:noreply, state}

      cluster_info ->
        new_state = start_search(:get_peers, args, cluster_info, state)
        {:noreply, new_state}
    end
  end

  def handle_cast({:find_node, cluster, args}, state) do
    case get_cluster_info(cluster, state) do
      nil ->
        Logger.error("find_node cluster not configured: #{Utils.encode_human(cluster)}")
        {:noreply, state}

      cluster_info ->
        new_state = start_search(:find_node, args, cluster_info, state)

        {:noreply, new_state}
    end
  end

  def handle_cast({:handle_reply, remote, nil}, state) do
    old_nodes = update_responded_node(state.nodes, remote)

    ## If the reply contains values we need to inform the user of this
    ## information and call the callback function.
    if remote.values, do: Enum.each(remote.values, state.callback)

    {:noreply, %{state | nodes: old_nodes}}
  end

  def handle_cast({:handle_reply, remote, nodes}, state) do
    old_nodes = update_responded_node(state.nodes, remote)

    new_nodes =
      Enum.flat_map(nodes, fn
        {id, ip, port} ->
          if Enum.find(state.nodes, fn x -> x.id == id end) == nil and
               {ip, port} != state.ip_tuple and state.node_id != id do
            [%Node{id: id, ip: ip, port: port}]
          else
            []
          end

        other ->
          Logger.warn("Remote node sent invalid node reply #{inspect(other)}")
          []
      end)

    {:noreply, %{state | nodes: old_nodes ++ new_nodes}}
  end

  #####################
  # Private Functions #
  #####################

  def send_announce_msg(cluster_header, %{cypher: cypher}, state) do
    state.nodes
    |> Distance.closest_nodes(state.target, 7)
    |> Enum.filter(fn node -> node.responded == true end)
    |> Enum.each(fn node ->
      Logger.debug("[#{Utils.encode_human(node.id)}] << announce_peer")

      args = [
        node_id: state.node_id,
        info_hash: state.target,
        token: node.token,
        port: state.port,
        meta: state.meta,
        ttl: state.ttl
      ]

      payload = KRPCProtocol.encode(:announce_peer, args)
      payload = Utils.wrap(cluster_header, Utils.encrypt(cypher, payload))
      CrissCrossDHT.UDPQuic.send(state.socket, node.ip, node.port, payload)
    end)
  end

  ## This function merges args (keyword list) with the state map and returns a
  ## function depending on the type (:get_peers, :find_node).
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

  def nodes_to_search_nodes(nodes) do
    Enum.map(nodes, fn node ->
      {id, ip, port} = extract_node_infos(node)
      %Node{id: id, ip: ip, port: port}
    end)
  end

  defp gen_request_msg(:find_node, state) do
    args = [tid: state.tid, node_id: state.node_id, target: state.target]
    KRPCProtocol.encode(:find_node, args)
  end

  defp gen_request_msg(:get_peers, state) do
    args = [tid: state.tid, node_id: state.node_id, info_hash: state.target]
    KRPCProtocol.encode(:get_peers, args)
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
      nil -> nil
      _ -> {cluster_header, cluster_config}
    end
  end
end
