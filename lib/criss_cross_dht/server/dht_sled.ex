defmodule CrissCrossDHT.Server.DHTSled do
  @moduledoc false

  @node_reannounce 10_000
  @tree_reannounce 10_000

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

  def queue_announce(conn, cluster, infohash, ip, port, ttl) do
    ttl =
      if ttl == -1 do
        18_446_744_073_709_551_615
      else
        ttl
      end

    next_time = :os.system_time(:millisecond) + @tree_reannounce + :rand.uniform(@tree_reannounce)

    :ok =
      SortedSetKV.zadd(
        Agent.get(conn, fn l -> l end),
        "treeclock",
        :erlang.term_to_binary({cluster, infohash}),
        :erlang.term_to_binary({ip, port, ttl}),
        next_time,
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

  def put(conn, cluster, infohash, ip, port, ttl) do
    bin = :erlang.term_to_binary({ip, port})

    :ok =
      SortedSetKV.zadd(
        Agent.get(conn, fn list -> list end),
        "members",
        Enum.join([cluster, infohash, bin], "-"),
        nil,
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

  {:put,
   <<65, 32, 73, 159, 199, 231, 221, 211, 99, 91, 135, 146, 31, 215, 223, 168, 245, 87, 193, 1,
     186, 90, 81, 250, 203, 170, 64, 21, 203, 246, 210, 144, 157, 227, 45, 65, 32, 49, 135, 50,
     248, 34, 117, 206, 53, 87, 189, 226, 87, 155, 11, 233, 179, 78, 177, 107, 34, 187, 188, 117,
     193, 222, 100, 254, 229, 161, 13, 218, 176>>}

  {:put,
   <<65, 32, 73, 159, 199, 231, 221, 211, 99, 91, 135, 146, 31, 215, 223, 168, 245, 87, 193, 1,
     186, 90, 81, 250, 203, 170, 64, 21, 203, 246, 210, 144, 157, 227, 45, 65, 32, 49, 135, 50,
     248, 34, 117, 206, 53, 87, 189, 226, 87, 155, 11, 233, 179, 78, 177, 107, 34, 187, 188, 117,
     193, 222, 100, 254, 229, 161, 13, 218, 176, 45, 131, 104, 2, 104, 4, 97, 127, 97, 0, 97, 0,
     97, 1, 98, 0, 0, 11, 187>>}

  {:get,
   <<65, 32, 73, 159, 199, 231, 221, 211, 99, 91, 135, 146, 31, 215, 223, 168, 245, 87, 193, 1,
     186, 90, 81, 250, 203, 170, 64, 21, 203, 246, 210, 144, 157, 227, 45, 65, 32, 49, 135, 50,
     248, 34, 117, 206, 53, 87, 189, 226, 87, 155, 11, 233, 179, 78, 177, 107, 34, 187, 188, 117,
     193, 222, 100, 254, 229, 161, 13, 218, 176>>}

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
          try do
            [:erlang.binary_to_term(right, [:safe])]
          rescue
            ArgumentError ->
              Logger.error("Could not decode member from db")
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
           :os.system_time(:millisecond)
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
           :os.system_time(:millisecond)
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
      now = :os.system_time(:millisecond)

      {cluster, name} = :erlang.binary_to_term(bin)
      key = Enum.join([cluster, name], "-")

      case SortedSetKV.zgetbykey(conn, "names", key, :os.system_time(:millisecond)) do
        b when is_binary(b) ->
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

          if ttl > now do
            refresh_name(pid, bin)
          end

        _ ->
          :ok
      end
    end

    ret = SortedSetKV.zrembyrangebyscore(conn, "nameclock", 0, :os.system_time(:millisecond), 100)

    if ret > 0 do
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
      now = :os.system_time(:millisecond)

      {cluster, infohash} = :erlang.binary_to_term(bin)

      case SortedSetKV.zgetbykey(conn, "treeclock", bin, :os.system_time(:millisecond)) do
        b when is_binary(b) ->
          {ip, port, ttl} = :erlang.binary_to_term(b)

          Logger.debug(
            "Reannouncing #{CrissCrossDHT.Server.Utils.encode_human(cluster)} #{CrissCrossDHT.Server.Utils.encode_human(infohash)}"
          )

          GenServer.cast(
            worker_conn,
            {:search_announce, cluster, infohash, fn _ -> :ok end, ttl, port}
          )

        _ ->
          :ok
      end
    end

    ret = SortedSetKV.zrembyrangebyscore(conn, "treeclock", 0, :os.system_time(:millisecond), 100)

    if ret > 0 do
      reannounce_names(pid, worker_conn)
    else
      :ok
    end
  end
end
