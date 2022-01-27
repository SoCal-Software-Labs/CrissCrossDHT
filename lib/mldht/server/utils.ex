defmodule MlDHT.Server.Utils do
  @moduledoc false

  @doc ~S"""
  This function gets a tuple as IP address and a port and returns a
  string which contains the IPv4 or IPv6 address and port in the following
  format: "127.0.0.1:6881".

    ## Example
    iex> MlDHT.Server.Utils.tuple_to_ipstr({127, 0, 0, 1}, 6881)
    "127.0.0.1:6881"
  """
  @aad "AES256GCM"

  alias MlDHT.Server.Storage

  def tuple_to_ipstr({oct1, oct2, oct3, oct4}, port) do
    "#{oct1}.#{oct2}.#{oct3}.#{oct4}:#{port}"
  end

  def tuple_to_ipstr(ipv6_addr, port) when tuple_size(ipv6_addr) == 8 do
    ip_str =
      String.duplicate("~4.16.0B:", 8)
      ## remove last ":" of the string
      |> String.slice(0..-2)
      |> :io_lib.format(Tuple.to_list(ipv6_addr))
      |> List.to_string()

    "[#{ip_str}]:#{port}"
  end

  @doc ~S"""
  This function generates a 256 bit (32 byte) random node id as a
  binary.
  """
  @spec gen_node_id :: Types.node_id()
  def gen_node_id do
    :rand.seed(:exs64, :os.timestamp())

    s =
      Stream.repeatedly(fn -> :rand.uniform(255) end)
      |> Enum.take(40)
      |> :binary.list_to_bin()

    hash(s)
  end

  def hash(s), do: :crypto.hash(:sha3_256, s)

  @doc """
  TODO
  """
  def gen_secret, do: gen_node_id()

  def config(:clusters) do
    Application.get_env(:mldht, :clusters, %{})
    |> Enum.map(fn {k, {c, pub, priv}} ->
      {:ok, pub_key} = ExPublicKey.loads(pub)
      {:ok, priv_key} = ExPublicKey.loads(priv)
      {k, {c, pub_key, priv_key}}
    end)
    |> Enum.into(%{})
  end

  def config(value, ret \\ nil), do: Application.get_env(:mldht, value, ret)

  def encrypt({secret, _, _}, payload) do
    do_encrypt(payload, secret)
  end

  def decrypt(payload, {secret, _, _}) do
    do_decrypt(payload, secret)
  end

  def verify_signature({_, nil, _}, value, signature), do: false

  def verify_signature({_, rsa_pub_key, _}, msg, signature) do
    case ExPublicKey.verify(msg, signature, rsa_pub_key) do
      {:ok, true} -> true
      _ -> false
    end
  end

  def sign(msg, rsa_priv_key) do
    ExPublicKey.sign(msg, rsa_priv_key)
  end

  defp do_encrypt(val, key) do
    iv = :crypto.strong_rand_bytes(32)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, to_string(val), @aad, true)

    iv <> tag <> ciphertext
  end

  defp do_decrypt(ciphertext, key) do
    <<iv::binary-32, tag::binary-16, ciphertext::binary>> = ciphertext
    :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, @aad, tag, false)
  end

  def wrap(header, body) do
    ["0A", header, body]
  end

  def unwrap("0A" <> <<cluster_header::binary-size(32), body::binary>>) do
    {cluster_header, body}
  end

  def encode_human(bin) do
    Base.url_encode64(bin, padding: false)
  end

  def combine_to_sign(list) do
    list
    |> Enum.map(&to_string(&1))
    |> Enum.join(".")
  end

  def check_generation(storage_pid, name, generation) do
    case Storage.get_name(storage_pid, name) do
      {value, saved_gen} -> saved_gen < generation
      _ -> true
    end
  end
end
