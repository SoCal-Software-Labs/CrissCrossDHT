defmodule MlDHT.Supervisor do
  use Supervisor

  @moduledoc ~S"""
  Root Supervisor for MlDHT

  """

  alias MlDHT.Server.Utils
  require Logger

  def children(node_id: node_id, config: config, worker_name: worker_name) do
    node_id_enc = node_id |> Utils.encode_human()
    Logger.debug("Node-ID: #{node_id_enc}")

    {storage_mod, storage_opts} =
      case Map.get(config, :storage) do
        nil -> {MlDHT.Server.Storage, []}
        {storage_mod, storage_opts} -> {storage_mod, storage_opts}
      end

    [
      {DynamicSupervisor,
       name: MlDHT.Registry.via(node_id_enc, MlDHT.RoutingTable.Supervisor),
       strategy: :one_for_one},
      {MlDHT.SearchName.Supervisor,
       name: MlDHT.Registry.via(node_id_enc, MlDHT.SearchName.Supervisor), strategy: :one_for_one},
      {MlDHT.SearchValue.Supervisor,
       name: MlDHT.Registry.via(node_id_enc, MlDHT.SearchValue.Supervisor), strategy: :one_for_one},
      {MlDHT.Search.Supervisor,
       name: MlDHT.Registry.via(node_id_enc, MlDHT.Search.Supervisor), strategy: :one_for_one},
      {MlDHT.Server.Worker, node_id: node_id, config: config, name: worker_name},
      {storage_mod, storage_opts ++ [name: MlDHT.Registry.via(node_id_enc, MlDHT.Server.Storage)]}
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
