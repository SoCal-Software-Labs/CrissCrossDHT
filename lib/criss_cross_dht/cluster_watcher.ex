defmodule CrissCrossDHT.ClusterWatcher do
  use GenServer

  require Logger

  alias CrissCrossDHT.Server.Utils

  @cypher "9YtgMwxnoSagovuViBbJ33drDaPpC6Mc2pVDpMLS8erc"
  @public_key "2bDkyNhW9LBRtCsH9xuRRKmvWJtL7QjJ3mao1FkDypmn8kmViGsarw4"

  @cluster_raw Utils.hash(Utils.combine_to_sign([@cypher, @public_key]))
  # "2UPhq1AXgmhSd6etUcSQRPfm42mSREcjUixSgi9N8nU1YoC"
  @cluster_name Utils.encode_human(@cluster_raw)
  @max_default_overlay_ttl 45 * 60 * 60 * 1000

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(path) do
    {:ok, watcher_pid} = FileSystem.start_link(dirs: [path])
    FileSystem.subscribe(watcher_pid)
    clusters = read_clusters(path)
    {:ok, %{watcher_pid: watcher_pid, clusters: clusters}}
  end

  def default_cluster(), do: @cluster_raw

  def get_cluster(cluster_id),
    do: GenServer.call(__MODULE__, {:get_cluster, cluster_id}, :infinity)

  def get_clusters(),
    do: GenServer.call(__MODULE__, :get_clusters, :infinity)

  def handle_call(
        {:get_cluster, cluster_id},
        _,
        %{clusters: clusters} = state
      ) do
    {:reply, Map.get(clusters, cluster_id), state}
  end

  def handle_call(
        :get_clusters,
        _,
        %{clusters: clusters} = state
      ) do
    {:reply, clusters, state}
  end

  def handle_info(
        {:file_event, watcher_pid, {path, events}},
        %{watcher_pid: watcher_pid, clusters: clusters} = state
      ) do
    if String.ends_with?(path, "yaml") and :modified in events do
      Logger.info("Cluster config change detected: #{path} #{inspect(events)}")
      new_clusters = read_cluster(path) |> Enum.map(&Utils.load_cluster/1) |> Enum.into(clusters)
      {:noreply, %{state | clusters: new_clusters}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:file_event, watcher_pid, :stop}, %{watcher_pid: watcher_pid} = state) do
    {:noreply, state}
  end

  def read_clusters(cluster_dir) do
    Path.wildcard("#{cluster_dir}/*.yaml")
    |> Enum.flat_map(fn cluster ->
      read_cluster(cluster)
    end)
    |> Enum.into(%{
      @cluster_name => %{
        max_ttl: @max_default_overlay_ttl,
        secret: @cypher,
        public_key: @public_key,
        max_transfer: 0
      }
    })
    |> Enum.flat_map(fn c ->
      try do
        [Utils.load_cluster(c)]
      rescue
        e ->
          Logger.error("Error loading cluster #{inspect(e)}")
          []
      end
    end)
    |> Enum.into(%{})
  end

  def read_cluster(cluser_file) do
    case YamlElixir.read_from_string(File.read!(cluser_file)) do
      {:ok,
       %{
         "Name" => name,
         "MaxTTL" => max_ttl,
         "Cypher" => cypher,
         "PublicKey" => pubk
       } = opts} ->
        [
          {name,
           %{
             max_ttl: max_ttl,
             secret: cypher,
             public_key: pubk,
             private_key: Map.get(opts, "PrivateKey"),
             max_transfer: Map.get(opts, "MaxAcceptedSize", 0)
           }}
        ]

      _ ->
        Logger.error("Invalid yaml file #{cluser_file}")
        []
    end
  end
end
