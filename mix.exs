defmodule MlDHT.Mixfile do
  use Mix.Project

  def project do
    [
      app: :mldht,
      version: "0.0.3",
      elixir: "~> 1.2",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      # mod: {MlDHT, []},
      applications: [:logger]
    ]
  end

  defp deps do
    [
      {:b58, "~> 1.0.2"},
      {:krpc_protocol, path: "../krpc_protocol"},
      {:ex_doc, "~> 0.19", only: :dev},
      {:pretty_hex, "~> 0.0.1", only: :dev},
      {:dialyxir, "~> 0.5.1", only: [:dev, :test]},
      {:ex_crypto, "~> 0.10.0"}
    ]
  end

  defp description do
    """
    Distributed Hash Table (DHT) is a storage and lookup system based on a peer-to-peer (P2P) system. The file sharing protocol BitTorrent makes use of a DHT to find new peers. MLDHT, in particular, is an elixir package that provides a mainline DHT implementation according to BEP 05.
    """
  end

  defp package do
    [
      name: :mldht,
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Florian Adamsky"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/cit/MLDHT"}
    ]
  end
end
