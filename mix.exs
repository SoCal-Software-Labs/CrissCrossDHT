defmodule CrissCrossDHT.Mixfile do
  use Mix.Project

  def project do
    [
      app: :crisscrossdht,
      version: "0.0.3",
      elixir: "~> 1.12",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:b58, "~> 1.0.2"},
      {:ex_schnorr,
       github: "hansonkd/ex_schnorr", ref: "c1aedaa7f38d5df49b76c4e04cfd8c666732deea"},
      {:ex_doc, "~> 0.19", only: :dev},
      {:pretty_hex, "~> 0.0.1", only: :dev},
      {:dialyxir, "~> 0.5.1", only: [:dev, :test]},
      {:ex_multihash, "~> 2.0"}
    ]
  end

  defp description do
    """
    Distributed Hash Table (DHT) is a storage and lookup system based on a peer-to-peer (P2P) system. The file sharing protocol BitTorrent makes use of a DHT to find new peers. MLDHT, in particular, is an elixir package that provides a mainline DHT implementation according to BEP 05.
    """
  end

  defp package do
    [
      name: :crisscrossdht,
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Florian Adamsky"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/cit/MLDHT"}
    ]
  end
end
