defmodule CrissCrossDHT.RoutingTable.Worker.Test do
  use ExUnit.Case

  @name :test

  setup do
    rt_name = "test_rt"
    node_id = "AAAAAAAAAAAAAAAAAAAB"
    node_id_enc = Base.encode16(node_id)

    start_supervised!({
      DynamicSupervisor,
      name:
        CrissCrossDHT.Registry.via(
          node_id_enc,
          CrissCrossDHT.RoutingTable.NodeSupervisor,
          rt_name
        ),
      strategy: :one_for_one
    })

    start_supervised!({
      CrissCrossDHT.RoutingTable.Worker,
      name: @name, node_id: node_id, rt_name: rt_name
    })

    [node_id: node_id, node_id_enc: node_id_enc, rt_name: rt_name]
  end

  test "If the size of the table is 0 if we add and delete a node" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 1

    CrissCrossDHT.RoutingTable.Worker.del(@name, "BBBBBBBBBBBBBBBBBBBB")
    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 0
  end

  test "get_node" do
    assert :ok ==
             CrissCrossDHT.RoutingTable.Worker.add(
               @name,
               "BBBBBBBBBBBBBBBBBBBB",
               {{127, 0, 0, 1}, 6881},
               23
             )

    assert CrissCrossDHT.RoutingTable.Worker.get(@name, "BBBBBBBBBBBBBBBBBBBB") |> Kernel.is_pid() ==
             true

    assert CrissCrossDHT.RoutingTable.Worker.get(@name, "CCCCCCCCCCCCCCCCCCCC") == nil

    CrissCrossDHT.RoutingTable.Worker.del(@name, "BBBBBBBBBBBBBBBBBBBB")
  end

  test "Double entries" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 1
    CrissCrossDHT.RoutingTable.Worker.del(@name, "BBBBBBBBBBBBBBBBBBBB")
  end

  test "if del() really deletes the node from the routing table" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    node_pid = CrissCrossDHT.RoutingTable.Worker.get(@name, "BBBBBBBBBBBBBBBBBBBB")

    assert Process.alive?(node_pid) == true
    CrissCrossDHT.RoutingTable.Worker.del(@name, "BBBBBBBBBBBBBBBBBBBB")
    assert Process.alive?(node_pid) == false
  end

  test "if routing table size and cache size are equal with two elements" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "CCCCCCCCCCCCCCCCCCCC",
      {{127, 0, 0, 1}, 6881},
      23
    )

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) ==
             CrissCrossDHT.RoutingTable.Worker.cache_size(@name)
  end

  test "if routing table size and cache size are equal with ten elements" do
    Enum.map(?B..?Z, fn x -> String.duplicate(<<x>>, 20) end)
    |> Enum.each(fn node_id ->
      CrissCrossDHT.RoutingTable.Worker.add(@name, node_id, {{127, 0, 0, 1}, 6881}, 23)
    end)

    CrissCrossDHT.RoutingTable.Worker.del(@name, "BBBBBBBBBBBBBBBBBBBB")
    CrissCrossDHT.RoutingTable.Worker.del(@name, "CCCCCCCCCCCCCCCCCCCC")
    CrissCrossDHT.RoutingTable.Worker.del(@name, "DDDDDDDDDDDDDDDDDDDD")

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) ==
             CrissCrossDHT.RoutingTable.Worker.cache_size(@name)
  end

  test "if closest_node() return only the closest nodes", test_worker_context do
    node_id = test_worker_context.node_id

    ## Generate close node_ids
    close_nodes =
      1..16
      |> Enum.map(fn x -> CrissCrossDHT.RoutingTable.Distance.gen_node_id(160 - x, node_id) end)
      |> Enum.filter(fn x -> x != node_id end)
      |> Enum.uniq()
      |> Enum.slice(0..7)
      |> Enum.sort()

    ## Add the close nodes to the RoutingTable
    Enum.each(close_nodes, fn node ->
      CrissCrossDHT.RoutingTable.Worker.add(@name, node, {{127, 0, 0, 1}, 6881}, nil)
    end)

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 8

    ## Generate and add distant nodes
    Enum.map(?B..?I, fn x -> String.duplicate(<<x>>, 20) end)
    |> Enum.each(fn node_id ->
      CrissCrossDHT.RoutingTable.Worker.add(@name, node_id, {{127, 0, 0, 1}, 6881}, 23)
    end)

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 16

    list =
      CrissCrossDHT.RoutingTable.Worker.closest_nodes(@name, node_id)
      |> Enum.map(fn x -> CrissCrossDHT.RoutingTable.Node.id(x) end)
      |> Enum.sort()

    ## list and close_nodes must be equal
    assert list == close_nodes
  end

  test "if routing table closest_nodes filters the source" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "CCCCCCCCCCCCCCCCCCCC",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "DDDDDDDDDDDDDDDDDDDD",
      {{127, 0, 0, 1}, 6881},
      23
    )

    node_id = "AAAAAAAAAAAAAAAAAAAB"
    source = "CCCCCCCCCCCCCCCCCCCC"

    list = CrissCrossDHT.RoutingTable.Worker.closest_nodes(@name, node_id, source)
    assert length(list) == 2
  end

  test "if routing table closest_nodes does not filters the source" do
    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "BBBBBBBBBBBBBBBBBBBB",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "CCCCCCCCCCCCCCCCCCCC",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "DDDDDDDDDDDDDDDDDDDD",
      {{127, 0, 0, 1}, 6881},
      23
    )

    node_id = "AAAAAAAAAAAAAAAAAAAB"

    list = CrissCrossDHT.RoutingTable.Worker.closest_nodes(@name, node_id)
    assert length(list) == 3
  end

  test "if routing table ignores its own node_id", test_worker_context do
    node_id = test_worker_context.node_id
    CrissCrossDHT.RoutingTable.Worker.add(@name, node_id, {{127, 0, 0, 1}, 6881}, 23)

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "CCCCCCCCCCCCCCCCCCCC",
      {{127, 0, 0, 1}, 6881},
      23
    )

    CrissCrossDHT.RoutingTable.Worker.add(
      @name,
      "DDDDDDDDDDDDDDDDDDDD",
      {{127, 0, 0, 1}, 6881},
      23
    )

    assert CrissCrossDHT.RoutingTable.Worker.size(@name) == 2
  end
end
