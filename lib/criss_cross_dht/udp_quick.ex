defmodule CrissCrossDHT.UDPQuic do
  import CrissCrossDHT.Server.Utils

  def open(_ip, _port, dispatcher) do
    ExP2P.Dispatcher.endpoint(dispatcher)
  end

  def send({:client, server, endpoint}, ip, port, msg) do
    ipstr = tuple_to_ipstr(ip, port)

    ExP2P.send_bidirectional_many(
      endpoint,
      [ipstr],
      IO.iodata_to_binary(["dht-", msg]),
      server,
      10_000
    )
  end

  def send(endpoint, {:stream, stream, _}, nil, msg) do
    try do
      ExP2P.stream_send(endpoint, stream, IO.iodata_to_binary(msg), 10_000)
    rescue
      e -> {:error, e}
    end
  end

  def send(endpoint, ip, port, msg) do
    ipstr = tuple_to_ipstr(ip, port)

    try do
      ExP2P.unidirectional_many(endpoint, [ipstr], IO.iodata_to_binary(msg), 10_000)
    rescue
      e -> {:error, e}
    end
  end
end
