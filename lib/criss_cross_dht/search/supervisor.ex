defmodule CrissCrossDHT.Search.Supervisor do
  use DynamicSupervisor

  alias CrissCrossDHT.Server.Utils
  require Logger

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, {:ok, opts[:strategy]}, name: opts[:name])
  end

  def init({:ok, strategy}) do
    DynamicSupervisor.init(strategy: strategy)
  end

  def start_child(pid, type, socket, node_id, node_id_enc, ip_tuple, cluster_config) do
    tid = KRPCProtocol.gen_tid()
    tid_str = Utils.encode_human(tid)

    ## If a Search already exist with this tid, generate a new TID by starting
    ## the function again
    if CrissCrossDHT.Registry.get_pid(node_id_enc, CrissCrossDHT.Search.Worker, tid_str) do
      start_child(pid, type, socket, node_id, node_id_enc, ip_tuple, cluster_config)
    else
      {:ok, search_pid} =
        DynamicSupervisor.start_child(
          pid,
          {CrissCrossDHT.Search.Worker,
           name: CrissCrossDHT.Registry.via(node_id_enc, CrissCrossDHT.Search.Worker, tid_str),
           type: type,
           socket: socket,
           node_id: node_id,
           ip_tuple: ip_tuple,
           clusters: cluster_config,
           tid: tid}
        )

      search_pid
    end
  end
end
