defmodule KRPCProtocol.Decoder do
  @moduledoc ~S"""
  KRPCProtocol.Encoder provides functions to decode mainline DHT messages.
  """
  require Logger

  def decode(payload) when is_binary(payload) do
    r =
      try do
        payload |> do_decode()
      rescue
        error in RuntimeError ->
          {:invalid, error.message}

        error in _ ->
          {:invalid, "Invalid becoded payload: #{inspect(payload)}"}
      end

    case r do
      {:invalid, _} = msg -> msg
      p -> p |> decode()
    end
  end

  #########
  # Error #
  #########

  def decode(%{:y => :e, :t => tid, :e => [code, msg]}) do
    {:error_reply, %{code: code, msg: msg, tid: tid}}
  end

  def decode(%{:y => :e, :e => [code, msg]}) do
    {:error_reply, %{code: code, msg: msg, tid: nil}}
  end

  ###########
  # Queries #
  ###########

  ## Get_peers
  def decode(%{
        :y => :q,
        :t => tid,
        :q => :get_peers,
        :a => %{:id => node_id, :hash => info_hash}
      }) do
    {:get_peers, %{tid: tid, node_id: node_id, info_hash: info_hash}}
  end

  def decode(%{:y => :q, :t => tid, :q => :get_peers, :a => %{:id => _}}) do
    {:error, %{code: 203, msg: "Get_peers without infohash", tid: tid}}
  end

  ## Find_node
  def decode(%{
        :y => :q,
        :t => tid,
        :q => :find_node,
        :a => %{:id => node_id, :target => target}
      }) do
    {:find_node, %{node_id: node_id, target: target, tid: tid}}
  end

  ## Store
  def decode(%{
        :q => :store,
        :t => tid,
        :y => :q,
        :a => %{
          :id => node_id,
          :key => key,
          :value => value,
          :ttl => ttl,
          :sig => sig
        }
      }) do
    {:store,
     %{
       node_id: node_id,
       tid: tid,
       key: key,
       value: value,
       ttl: ttl,
       signature: sig
     }}
  end

  def decode(%{
        :q => :store_name,
        :t => tid,
        :y => :q,
        :a => %{
          :id => node_id,
          :name => name,
          :value => value,
          :ttl => ttl,
          :sig => sig,
          :priv => key_string,
          :sig_ns => sig_ns,
          :gen => gen
        }
      }) do
    {:store_name,
     %{
       node_id: node_id,
       tid: tid,
       name: name,
       value: value,
       ttl: ttl,
       key_string: key_string,
       signature_ns: sig_ns,
       signature: sig,
       generation: gen
     }}
  end

  ## Find Value
  def decode(%{
        :q => :find_value,
        :t => tid,
        :y => :q,
        :a => %{:id => node_id, :key => key}
      }) do
    {:find_value, %{node_id: node_id, key: key, tid: tid}}
  end

  def decode(%{:y => :q, :t => tid, :q => :find_node, :a => %{:id => _}}) do
    {:error, %{code: 203, msg: "Find_node without target", tid: tid}}
  end

  def decode(%{
        :q => :find_name,
        :t => tid,
        :y => :q,
        :a => %{:id => node_id, :name => name, :gen => generation}
      }) do
    {:find_name, %{node_id: node_id, name: name, tid: tid, generation: generation}}
  end

  ## Ping
  def decode(%{:q => :ping, :t => tid, :y => :q, :a => %{:id => node_id}}) do
    {:ping, %{node_id: node_id, tid: tid}}
  end

  ## Announce_peer
  def decode(%{
        :q => :announce_peer,
        :t => tid,
        :y => :q,
        :a => %{
          :id => node_id,
          :hash => infohash,
          :port => port,
          :token => token,
          :implied_port => implied_port,
          :ttl => ttl
        }
      }) do
    {:announce_peer,
     %{
       tid: tid,
       node_id: node_id,
       info_hash: infohash,
       port: port,
       ttl: ttl,
       token: token,
       implied_port: implied_port
     }}
  end

  def decode(%{
        :q => :announce_peer,
        :t => tid,
        :y => :q,
        :a => %{
          :id => node_id,
          :hash => infohash,
          :port => port,
          :token => token,
          :ttl => ttl
        }
      }) do
    {:announce_peer,
     %{tid: tid, node_id: node_id, info_hash: infohash, port: port, ttl: ttl, token: token}}
  end

  def decode(%{
        :q => :announce_peer,
        :t => _,
        :y => :q,
        :a => %{:id => _, :port => _, :token => _}
      }) do
    {:error, %{code: 203, msg: "Announce_peer with no info_hash."}}
  end

  def decode(%{
        :q => :announce_peer,
        :t => _,
        :y => :q,
        :a => %{:id => _, :token => _, :hash => _}
      }) do
    {:error, %{code: 203, msg: "Announce_peer with no port."}}
  end

  def decode(%{
        :q => :announce_peer,
        :t => _,
        :y => :q,
        :a => %{:id => _, :port => _, :hash => _}
      }) do
    {:error, %{code: 203, msg: "Announce_peer with no token."}}
  end

  ###########
  # Replies #
  ###########

  ## Find Name Reply
  def decode(%{
        :y => :r,
        :t => tid,
        :r => %{
          :id => node_id,
          :name => name,
          :value => value,
          :token => token,
          :gen => generation,
          :priv => key_string,
          :sig => signature_cluster,
          :sig_n => signature_name,
          :ttl => ttl
        }
      }) do
    {:find_name_reply,
     %{
       node_id: node_id,
       tid: tid,
       name: name,
       value: value,
       token: token,
       generation: generation,
       key_string: key_string,
       signature_cluster: signature_cluster,
       signature_name: signature_name,
       ttl: ttl
     }}
  end

  def decode(%{
        :y => :r,
        :t => tid,
        :r => %{:id => node_id, :name => name, :nodes => nodes}
      }) do
    {:find_name_nodes_reply, %{tid: tid, node_id: node_id, name: name, nodes: nodes}}
  end

  def decode(%{
        :y => :r,
        :t => tid,
        :r => %{:id => node_id, :name => name, :nodes6 => nodes}
      }) do
    {:find_name_nodes_reply, %{tid: tid, node_id: node_id, name: name, nodes: nodes}}
  end

  ## Find Value Reply
  def decode(%{
        :y => :r,
        :t => tid,
        :r => %{:id => node_id, :key => key, :value => value, :token => token}
      }) do
    {:find_value_reply, %{node_id: node_id, tid: tid, key: key, value: value, token: token}}
  end

  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id, :key => key, :nodes => nodes}}) do
    {:find_value_nodes_reply, %{tid: tid, node_id: node_id, key: key, nodes: nodes}}
  end

  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id, :key => key, :nodes6 => nodes}}) do
    {:find_value_nodes_reply, %{tid: tid, node_id: node_id, key: key, nodes: nodes}}
  end

  ## Get_peer Reply
  def decode(%{:y => :r, :t => tid, :r => %{:id => id, :token => t, :values => values}}) do
    {:get_peer_reply, %{tid: tid, node_id: id, token: t, values: values, nodes: nil}}
  end

  def decode(%{:y => :r, :t => tid, :r => %{:id => id, :token => t, :nodes => nodes}}) do
    {:get_peer_reply, %{tid: tid, node_id: id, token: t, values: nil, nodes: nodes}}
  end

  def decode(%{:y => :r, :t => tid, :r => %{:id => id, :token => t, :nodes6 => nodes}}) do
    {:get_peer_reply, %{tid: tid, node_id: id, token: t, values: nil, nodes: nodes}}
  end

  ## Find_node Reply
  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id, :nodes => nodes}}) do
    {:find_node_reply, %{tid: tid, node_id: node_id, values: nil, nodes: nodes}}
  end

  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id, :nodes6 => nodes}}) do
    {:find_node_reply, %{tid: tid, node_id: node_id, values: nil, nodes: nodes}}
  end

  ## Store Reply
  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id, :key => key, :wrote => wrote}}) do
    {:store_reply, %{node_id: node_id, tid: tid, key: key, wrote: wrote}}
  end

  ## Store name Reply
  def decode(%{
        :y => :r,
        :t => tid,
        :r => %{:id => node_id, :name => name, :wrote => wrote}
      }) do
    {:store_name_reply, %{node_id: node_id, tid: tid, name: name, wrote: wrote}}
  end

  ## Ping Reply
  def decode(%{:y => :r, :t => tid, :r => %{:id => node_id}}) do
    {:ping_reply, %{node_id: node_id, tid: tid}}
  end

  ## We ignore unknown messages
  def decode(message) do
    {:invalid, message}
  end

  #####################
  # Private Functions #
  #####################

  ## This function checks for common error.
  defp check_errors(msg) do
    if Map.has_key?(msg, :a) and byte_size(msg[:a][:id]) != 32 do
      raise "Invalid node id size: #{byte_size(msg[:a][:id])}"
    end

    if Map.has_key?(msg, :r) and byte_size(msg[:r][:id]) != 32 do
      raise "Invalid node id size: #{byte_size(msg[:r][:id])}"
    end

    if has_nodes?(msg, :nodes) and size_is_multiple_of?(msg[:r][:nodes], 26) do
      raise "Size of IPv4 nodes is not a multiple of 26: #{byte_size(msg[:r][:nodes])}"
    end

    if has_nodes?(msg, :nodes6) and size_is_multiple_of?(msg[:r][:nodes6], 38) do
      raise "Size of IPv6 nodes is not a multiple of 38: #{byte_size(msg[:r][:nodes6])}"
    end

    msg
  end

  defp has_nodes?(msg, key), do: Map.has_key?(msg, :r) and Map.has_key?(msg[:r], key)
  defp size_is_multiple_of?(map, size), do: map |> byte_size |> rem(size) != 0

  ## This functions gets a binary and extracts the IPv4/IPv6 address and the
  ## port and returns it as a tuple in the following format: {{127, 0, 0, 1}, 80}
  def comp_form(<<v4::binary-size(4), port::size(16)>>), do: {ip_tuple(v4), port}
  def comp_form(<<v6::binary-size(16), port::size(16)>>), do: {ip_tuple(v6), port}

  ## This function gets an IPv4/IPv6 address as a binary and convert is to a
  ## tuple.
  ## Example
  ##  iex> ip_tuple("aaaa")
  ##    {97, 97, 97, 97}
  defp ip_tuple(ip_addr) when byte_size(ip_addr) == 4, do: ipv4_tuple(ip_addr, [])
  defp ip_tuple(ip_addr) when byte_size(ip_addr) == 16, do: ipv6_tuple(ip_addr, [])

  defp ipv4_tuple("", result), do: List.to_tuple(result)

  defp ipv4_tuple(ip_addr, result) do
    <<octet::size(8), rest::binary>> = ip_addr
    ipv4_tuple(rest, result ++ [octet])
  end

  defp ipv6_tuple("", result), do: List.to_tuple(result)

  defp ipv6_tuple(ip_addr, result) do
    <<two_octets::size(16), rest::binary>> = ip_addr
    ipv6_tuple(rest, result ++ [two_octets])
  end

  defp do_decode(obj) do
    # Bencodex.decode
    :erlang.binary_to_term(obj)
  end
end
