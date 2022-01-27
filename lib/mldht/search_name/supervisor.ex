defmodule MlDHT.SearchName.Supervisor do
  use DynamicSupervisor

  alias MlDHT.Server.Utils
  require Logger

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, {:ok, opts[:strategy]}, name: opts[:name])
  end

  def init({:ok, strategy}) do
    DynamicSupervisor.init(strategy: strategy)
  end

  def start_child(pid, type, socket, node_id) do
    node_id_enc = Utils.encode_human(node_id)
    tid = KRPCProtocol.gen_tid()
    tid_str = Utils.encode_human(tid)

    ## If a SearchName already exist with this tid, generate a new TID by starting
    ## the function again
    if MlDHT.Registry.get_pid(node_id_enc, MlDHT.SearchName.Worker, tid_str) do
      start_child(pid, type, socket, node_id)
    else
      {:ok, search_pid} =
        DynamicSupervisor.start_child(
          pid,
          {MlDHT.SearchName.Worker,
           name: MlDHT.Registry.via(node_id_enc, MlDHT.SearchName.Worker, tid_str),
           type: type,
           socket: socket,
           node_id: node_id,
           tid: tid}
        )

      search_pid
    end
  end
end
