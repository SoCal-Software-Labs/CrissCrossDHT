defmodule CrissCrossDHT.Server.Storage.Test do
  use ExUnit.Case
  require Logger

  alias CrissCrossDHT.Server.Storage
  alias CrissCrossDHT.Registry

  setup do
    node_id_enc = String.duplicate("A", 20) |> Base.encode16()
    rt_name = "test_rt"

    start_supervised!({
      DynamicSupervisor,
      name: Registry.via(node_id_enc, CrissCrossDHT.RoutingTable.NodeSupervisor, rt_name),
      strategy: :one_for_one
    })

    start_supervised!({Storage, name: Registry.via(node_id_enc, Storage)})

    [pid: CrissCrossDHT.Registry.get_pid(node_id_enc, Storage)]
  end

  test "has_nodes_for_infohash?", test_context do
    pid = test_context.pid
    Storage.put(pid, "aaaa", {127, 0, 0, 1}, 6881)

    assert Storage.has_nodes_for_infohash?(pid, "bbbb") == false
    assert Storage.has_nodes_for_infohash?(pid, "aaaa") == true
  end

  test "get_nodes", test_context do
    pid = test_context.pid
    ip1 = {127, 0, 0, 1}
    ip2 = {127, 0, 0, 2}

    Storage.put(pid, "aaaa", ip1, 6881)
    Storage.put(pid, "aaaa", ip1, 6881)
    Storage.put(pid, "aaaa", ip2, 6882)

    Storage.print(pid)

    assert Storage.get_nodes(pid, "aaaa") == [{ip1, 6881}, {ip2, 6882}]
  end
end
