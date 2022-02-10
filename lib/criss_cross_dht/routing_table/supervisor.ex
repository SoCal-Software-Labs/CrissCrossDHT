defmodule CrissCrossDHT.RoutingTable.Supervisor do
  use Supervisor

  require Logger

  @moduledoc ~S"""
    TODO
  """

  def start_link(opts) do
    name =
      opts[:node_id_enc]
      |> CrissCrossDHT.Registry.via(CrissCrossDHT.RoutingTable.Supervisor, opts[:rt_name])

    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(args) do
    node_id = args[:node_id]
    ip_tuple = args[:ip_tuple]
    node_id_enc = args[:node_id_enc]
    rt_name = args[:rt_name]
    cluster = args[:cluster]
    cluster_secret = args[:cluster_secret]

    children = [
      {CrissCrossDHT.RoutingTable.Worker,
       rt_name: rt_name,
       node_id: node_id,
       cluster: cluster,
       cluster_secret: cluster_secret,
       ip_tuple: ip_tuple,
       name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.RoutingTable.Worker, rt_name)},
      {DynamicSupervisor,
       name:
         CrissCrossDHT.Registry.via(
           node_id_enc,
           CrissCrossDHT.RoutingTable.NodeSupervisor,
           rt_name
         ),
       strategy: :one_for_one}
    ]

    Logger.debug("RoutingTable.Supervisor children #{inspect(children)}")
    Supervisor.init(children, strategy: :one_for_one)
  end
end
