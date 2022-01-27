defmodule MlDHT.Server.Storage do
  @moduledoc false

  use GenServer

  alias MlDHT.Server.Utils
  require Logger

  #############
  # Constants #
  #############

  ## One minute in milliseconds
  @min_in_ms 60 * 1000

  ## 5 Minutes
  @review_time 5 * @min_in_ms

  ## 30 Minutes
  @node_expired 30 * @min_in_ms

  def start_link(opts) do
    GenServer.start_link(__MODULE__, [], opts)
  end

  def init([]) do
    Process.send_after(self(), :review_storage, @review_time)
    {:ok, %{nodes: %{}, values: %{}, names: %{}}}
  end

  def put(pid, infohash, ip, port) do
    GenServer.cast(pid, {:put, infohash, ip, port})
  end

  def put_value(pid, key, value, ttl) do
    GenServer.cast(pid, {:put_value, key, value, ttl})
  end

  def put_name(pid, name, value, generation, signature, ttl) do
    GenServer.cast(pid, {:put_name, name, value, generation, signature, ttl})
  end

  def print(pid) do
    GenServer.cast(pid, :print)
  end

  def has_nodes_for_infohash?(pid, infohash) do
    GenServer.call(pid, {:has_nodes_for_infohash?, infohash})
  end

  def get_nodes(pid, infohash) do
    GenServer.call(pid, {:get_nodes, infohash})
  end

  def get_value(pid, infohash) do
    GenServer.call(pid, {:get_value, infohash})
  end

  def get_name(pid, infohash) do
    GenServer.call(pid, {:get_name, infohash})
  end

  def handle_info(:review_storage, %{nodes: nodes, values: values} = state) do
    Logger.debug("Review storage")

    ## Restart review timer
    Process.send_after(self(), :review_storage, @review_time)

    {:noreply, %{state | nodes: review(Map.keys(nodes), nodes), values: review_values(values)}}
  end

  def handle_call({:has_nodes_for_infohash?, infohash}, _from, %{nodes: nodes} = state) do
    has_keys = Map.has_key?(nodes, infohash)
    result = if has_keys, do: Map.get(nodes, infohash) != [], else: has_keys

    {:reply, result, state}
  end

  def handle_call({:get_nodes, infohash}, _from, %{nodes: nodes} = state) do
    response =
      nodes
      |> Map.get(infohash)
      |> Enum.map(fn x -> Tuple.delete_at(x, 2) end)
      |> Enum.slice(0..99)

    {:reply, response, state}
  end

  def handle_call({:get_value, infohash}, _from, %{values: values} = state) do
    maybe = Map.get(values, infohash)

    time = :os.system_time(:millisecond)

    response =
      case maybe do
        {v, s} when s == -1 or s > time -> v
        _ -> nil
      end

    {:reply, response, state}
  end

  def handle_call({:get_name, infohash}, _from, %{names: names} = state) do
    maybe = Map.get(names, infohash)

    time = :os.system_time(:millisecond)

    response =
      case maybe do
        {v, gen, sig, s} when s == -1 or s > time -> {v, gen, sig}
        _ -> nil
      end

    {:reply, response, state}
  end

  def handle_cast({:put_name, name, value, generation, signature, ttl}, %{names: names} = state) do
    item =
      if ttl == -1 do
        {value, generation, signature, -1}
      else
        {value, generation, signature, :os.system_time(:millisecond) + ttl}
      end

    new_values = Map.put(names, name, item)

    {:noreply, %{state | names: new_values}}
  end

  def handle_cast({:put_value, key, value, ttl}, %{values: values} = state) do
    item =
      if ttl == -1 do
        {value, -1}
      else
        {value, :os.system_time(:millisecond) + ttl}
      end

    new_values = Map.put(values, key, item)

    {:noreply, %{state | values: new_values}}
  end

  def handle_cast({:put, infohash, ip, port}, %{nodes: nodes} = state) do
    item = {ip, port, :os.system_time(:millisecond)}

    new_nodes =
      if Map.has_key?(nodes, infohash) do
        index =
          nodes
          |> Map.get(infohash)
          |> Enum.find_index(fn node_tuple ->
            Tuple.delete_at(node_tuple, 2) == {ip, port}
          end)

        if index do
          Map.update!(nodes, infohash, fn x ->
            List.replace_at(x, index, item)
          end)
        else
          Map.update!(nodes, infohash, fn x ->
            x ++ [item]
          end)
        end
      else
        Map.put(nodes, infohash, [item])
      end

    {:noreply, %{state | nodes: new_nodes}}
  end

  def handle_cast(:print, %{nodes: nodes} = state) do
    Enum.each(Map.keys(nodes), fn infohash ->
      Logger.debug("#{Utils.encode_human(infohash)}")

      Enum.each(Map.get(nodes, infohash), fn x ->
        Logger.debug("  #{inspect(x)}")
      end)
    end)

    {:noreply, state}
  end

  def review([], result), do: result

  def review([head | tail], result) do
    new = delete_old_nodes(result, head)
    review(tail, new)
  end

  def delete_old_nodes(state, infohash) do
    time = :os.system_time(:millisecond)

    Map.update!(state, infohash, fn list ->
      Enum.filter(list, fn x ->
        time - elem(x, 2) <= @node_expired
      end)
    end)
  end

  def review_values(values) do
    time = :os.system_time(:millisecond)

    values
    |> Enum.filter(fn {k, {v, s}} ->
      s == -1 or s > time
    end)
    |> Enum.into(%{})
  end
end
