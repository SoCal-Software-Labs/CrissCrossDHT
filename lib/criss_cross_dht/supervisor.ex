defmodule CrissCrossDHT.Supervisor do
  use Supervisor

  @moduledoc ~S"""
  Root Supervisor for CrissCrossDHT

  """

  alias CrissCrossDHT.Server.Utils
  require Logger

  def children(node_id: node_id, config: config, worker_name: worker_name) do
    node_id_enc = node_id |> Utils.encode_human()
    Logger.info("Node-ID: #{node_id_enc}")

    {storage_mod, storage_opts} = Map.get(config, :storage)
    cluster_dir = Map.get(config, :cluster_dir)

    config = Map.put(config, :storage, {storage_mod, storage_opts})

    [
      {CrissCrossDHT.ClusterWatcher, cluster_dir},
      {storage_mod,
       storage_opts ++
         [name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.Server.Storage)]},
      {DynamicSupervisor,
       name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.RoutingTable.Supervisor),
       strategy: :one_for_one},
      {CrissCrossDHT.SearchName.Supervisor,
       name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.SearchName.Supervisor),
       strategy: :one_for_one},
      {CrissCrossDHT.SearchValue.Supervisor,
       name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.SearchValue.Supervisor),
       strategy: :one_for_one},
      {CrissCrossDHT.Search.Supervisor,
       name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.Search.Supervisor),
       strategy: :one_for_one},
      {CrissCrossDHT.Server.Worker, node_id: node_id, config: config, name: worker_name}
    ]
  end

  @doc false
  # TODO: use Keyword.fetch!/2 to enforce the :node_id option
  def start_link(opts) do
    Supervisor.start_link(
      __MODULE__,
      [node_id: opts[:node_id], config: opts[:config], worker_name: opts[:worker_name]],
      opts
    )
  end

  @impl true
  def init(opts) do
    Supervisor.init(children(opts), strategy: :one_for_one)
  end
end
