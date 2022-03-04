defmodule CrissCrossDHT.Server.DHTSled do
  @moduledoc false

  @node_reannounce :timer.minutes(10)
  @tree_reannounce :timer.minutes(10)
  @max_announce_broadcast_ttl :timer.minutes(30)

  defmodule TTLCleanup do
    use GenServer
    require Logger

    @review_time 5_000

    def start_link(conn) do
      GenServer.start_link(__MODULE__, conn, [])
    end

    def init(conn) do
      Process.send_after(self(), :review_storage, @review_time)
      {:ok, conn}
    end

    def handle_info(:review_storage, conn) do
      Logger.debug("Review storage")

      :ok = scan(conn, "values")
      :ok = scan(conn, "names")
      :ok = scan(conn, "members")
      :ok = scan(conn, "announced")
      :ok = scan(conn, "nodes")
      Process.send_after(self(), :review_storage, @review_time)

      {:noreply, conn}
    end

    def scan(conn, collection) do
      new_agg =
        SortedSetKV.zrembyrangebyscore(conn, collection, 0, :os.system_time(:millisecond), 100)

      case new_agg do
        v when is_number(v) and v <= 99 ->
          :ok

        v when is_number(v) ->
          scan(conn, collection)
      end
    end
  end

  require Logger

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(opts) do
    {sled_conn, opts} = Keyword.pop(opts, :database)
    {:ok, _} = TTLCleanup.start_link(sled_conn)
    Agent.start_link(fn -> sled_conn end, opts)
  end

  def next_time() do
    :os.system_time(:millisecond) + @tree_reannounce + :rand.uniform(@tree_reannounce)
  end

  def queue_announce(conn, cluster, infohash, ip, port, meta, ttl) do
    :ok =
      SortedSetKV.zadd(
        Agent.get(conn, fn l -> l end),
        "treeclock",
        :erlang.term_to_binary({cluster, infohash}),
        :erlang.term_to_binary({ip, port, ttl, meta}),
        next_time(),
        true
      )
  end

  def cluster_announce(conn, cluster, infohash, ttl) do
    ttl =
      if ttl == -1 do
        18_446_744_073_709_551_615
      else
        ttl
      end

    :ok =
      SortedSetKV.zadd(
        Agent.get(conn, fn l -> l end),
        "announced",
        Enum.join([cluster, infohash], "-"),
        nil,
        ttl,
        true
      )
  end

  def has_announced_cluster(conn, cluster, infohash) do
    ret =
      SortedSetKV.zscore(
        Agent.get(conn, fn l -> l end),
        "announced",
        Enum.join([cluster, infohash], "-")
      )

    case ret do
      {false, _} -> false
      {true, _} -> true
      e -> e
    end
  end

  def put(conn, cluster, infohash, ip, port, meta, ttl) do
    bin = :erlang.term_to_binary({ip, port})

    ttl =
      if ttl == -1 do
        18_446_744_073_709_551_615
      else
        ttl
      end

    ttl = min(:os.system_time(:millisecond) + @max_announce_broadcast_ttl, ttl)

    :ok =
      SortedSetKV.zadd(
        Agent.get(conn, fn list -> list end),
        "members",
        Enum.join([cluster, infohash, bin], "-"),
        :erlang.term_to_binary(meta),
        ttl,
        true
      )

    :ok
  end

  def put_value(conn, cluster, key, value, ttl) do
    bin = :erlang.term_to_binary({cluster, key, ttl})
    conn = Agent.get(conn, fn c -> c end)

    {:ok, _} =
      case ttl do
        -1 ->
          :ok =
            SortedSetKV.zadd(
              conn,
              "values",
              Enum.join([cluster, key], "-"),
              bin,
              18_446_744_073_709_551_615,
              true
            )

          :ok = SortedSetKV.rpush(conn, "values:queue", bin)

        _ ->
          :ok =
            SortedSetKV.zadd(
              conn,
              "values",
              Enum.join([cluster, key], "-"),
              value,
              ttl,
              true
            )

          :ok = SortedSetKV.rpush(conn, "values:queue", bin)
      end

    :ok
  end

  def put_name(
        conn,
        cluster,
        name,
        value,
        generation,
        public_key,
        signature_cluster,
        signature,
        ttl
      ) do
    bin =
      :erlang.term_to_binary({value, generation, ttl, public_key, signature_cluster, signature})

    conn = Agent.get(conn, fn c -> c end)
    key = Enum.join([cluster, name], "-")
    next_time = :os.system_time(:millisecond) + @node_reannounce + :rand.uniform(@node_reannounce)

    reannounce =
      if ttl == -1 or ttl > next_time do
        bin = :erlang.term_to_binary({cluster, name})

        fn ->
          SortedSetKV.zadd(conn, "nameclock", bin, nil, next_time, true)
        end
      else
        fn -> nil end
      end

    {:ok, _} =
      case ttl do
        -1 ->
          SortedSetKV.zadd(conn, "names", key, bin, 18_446_744_073_709_551_615, true)

          reannounce.()

        _ ->
          SortedSetKV.zadd(conn, "names", key, bin, ttl, true)

          reannounce.()
      end

    :ok
  end

  def refresh_name(conn, bin) do
    conn = Agent.get(conn, fn c -> c end)
    now = :os.system_time(:millisecond)
    next_time = now + @node_reannounce + :rand.uniform(@node_reannounce)
    SortedSetKV.zscoreupdate(conn, "nameclock", bin, next_time, true)
  end

  def has_nodes_for_infohash?(conn, cluster, infohash) do
    conn = Agent.get(conn, fn c -> c end)

    SortedSetKV.zrangebyprefixscore(
      conn,
      "members",
      Enum.join([cluster, infohash], "-"),
      :os.system_time(:millisecond),
      18_446_744_073_709_551_615,
      0,
      1
    ) != []
  end

  def get_nodes(conn, cluster, infohash) do
    conn = Agent.get(conn, fn c -> c end)
    prefix = Enum.join([cluster, infohash], "-")

    vals =
      SortedSetKV.zrangebyprefixscore(
        conn,
        "members",
        prefix,
        :os.system_time(:millisecond),
        18_446_744_073_709_551_615,
        0,
        100
      )

    size = byte_size(prefix) + 1

    vals
    |> Enum.flat_map(fn v ->
      case v do
        <<_::binary-size(size), right::binary>> ->
          {meta_str, ttl} = SortedSetKV.zgetbykey(conn, "members", v, 0)

          meta =
            case meta_str do
              nil -> nil
              b -> :erlang.binary_to_term(b)
            end

          try do
            {ip, port} = :erlang.binary_to_term(right, [:safe])
            [{ip, port, meta, ttl}]
          rescue
            ArgumentError ->
              Logger.error("Could not decode member from db, #{inspect(right)}")
              []
          end

        _ ->
          Logger.error("Could not decode member from db - not enough bytes")
          []
      end
    end)
  end

  def get_value(conn, cluster, infohash) do
    conn = Agent.get(conn, fn c -> c end)

    case SortedSetKV.zgetbykey(
           conn,
           "values",
           Enum.join([cluster, infohash], "-"),
           0
         ) do
      {value, _} -> value
      _ -> nil
    end
  end

  def get_name(conn, cluster, infohash) do
    conn = Agent.get(conn, fn c -> c end)

    case SortedSetKV.zgetbykey(
           conn,
           "names",
           Enum.join([cluster, infohash], "-"),
           0
         ) do
      {value, _} when is_binary(value) -> :erlang.binary_to_term(value)
      _ -> nil
    end
  end

  def process_values(pid, callback) do
    conn = Agent.get(pid, fn c -> c end)
    next_value = SortedSetKV.lpop(conn, "values:queue")

    case next_value do
      bin when is_binary(bin) ->
        {cluster, hash, ttl} = :erlang.binary_to_term(bin)

        case get_value(pid, cluster, hash) do
          val when is_binary(val) ->
            :ok = callback.(cluster, val, ttl)
            process_values(pid, callback)

          _ ->
            :ok
        end

      nil ->
        :ok
    end
  end

  def reannounce_names(pid, worker_conn) do
    conn = Agent.get(pid, fn c -> c end)

    infohashes =
      SortedSetKV.zrangebyscore(conn, "nameclock", 0, :os.system_time(:millisecond), 0, 100)

    for bin <- infohashes do
      {cluster, name} = :erlang.binary_to_term(bin)
      key = Enum.join([cluster, name], "-")

      case SortedSetKV.zgetbykey(conn, "names", key, 0) do
        {b, _} when is_binary(b) ->
          {value, generation, ttl, public_key, signature_cluster, signature_name} =
            :erlang.binary_to_term(b)

          tid = KRPCProtocol.gen_tid()

          Logger.debug(
            "Reannouncing #{CrissCrossDHT.Server.Utils.encode_human(cluster)} #{CrissCrossDHT.Server.Utils.encode_human(name)}"
          )

          GenServer.cast(
            worker_conn,
            {:broadcast_name, cluster, tid, name, value, ttl, public_key, generation,
             signature_cluster, signature_name}
          )

          next = next_time()

          if ttl == -1 or ttl > next do
            SortedSetKV.zscoreupdate(conn, "nameclock", bin, next, true)
          else
            SortedSetKV.zrem(conn, "nameclock", bin)
          end

        nil ->
          :ok
      end
    end

    if length(infohashes) > 0 do
      reannounce_names(pid, worker_conn)
    else
      :ok
    end
  end

  def reannounce_trees(pid, worker_conn) do
    conn = Agent.get(pid, fn c -> c end)

    infohashes =
      SortedSetKV.zrangebyscore(conn, "treeclock", 0, :os.system_time(:millisecond), 0, 100)

    for bin <- infohashes do
      {cluster, infohash} = :erlang.binary_to_term(bin)

      case SortedSetKV.zgetbykey(conn, "treeclock", bin, 0) do
        {b, _} when is_binary(b) ->
          {_ip, port, ttl, meta} = :erlang.binary_to_term(b)

          Logger.debug(
            "Reannouncing #{CrissCrossDHT.Server.Utils.encode_human(cluster)} #{CrissCrossDHT.Server.Utils.encode_human(infohash)}"
          )

          GenServer.cast(
            worker_conn,
            {:search_announce, cluster, infohash, fn _ -> :ok end, ttl, port, meta, false}
          )

          next = next_time()

          if ttl == -1 or ttl > next do
            SortedSetKV.zscoreupdate(conn, "treeclock", bin, next, true)
          else
            SortedSetKV.zrem(conn, "treeclock", bin)
          end

        nil ->
          :ok
      end
    end

    if length(infohashes) > 0 do
      reannounce_names(pid, worker_conn)
    else
      :ok
    end
  end
end
