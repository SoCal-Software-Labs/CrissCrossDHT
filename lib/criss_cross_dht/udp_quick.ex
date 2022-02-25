defmodule CrissCrossDHT.UDPQuic do
  import CrissCrossDHT.Server.Utils

  def open(_ip, _port, dispatcher) do
    ExP2P.Dispatcher.endpoint(dispatcher)
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
