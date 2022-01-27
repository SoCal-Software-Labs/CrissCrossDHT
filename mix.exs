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

  @cypher <<181, 213, 27, 139, 182, 215, 15, 249, 19, 174, 56, 54, 97, 216, 167, 25, 64, 91, 189,
            126, 49, 75, 44, 113, 85, 55, 179, 208, 1, 252, 248, 212>>
  @public_key "-----BEGIN RSA PUBLIC KEY-----\nMIIBCgKCAQEAzCBvDU/9sz1tPMonJMu9khHk0VaMet8bgJMShHpSYM7ul0yfihz1\nL7CU5ppZF+v2tr+xFt2aSSqFf4irpbOOuBryBPZUDBgQSIz/JpLLMBmioUq0EZfZ\nfmG8XU1lygKsP+MoyQJ5umzt4i9e9mMV+LVwZmY88dhcFvDWkT0MG9yrYjX65L8L\nja4mXZWLAIZoUFaJ617SxeoP0Bv5bmAgH666T8q66l4FTagPHX/komRkQhvYH4FD\ncfTr/Z0mnSCuNreA2vJpXupekfgEPMwXz3zlPPQ7f4E+mqUO+p+XYUE7JBlTggYD\ncpEzPUt7Wk0gbe9K+ez7zDojoGkSxhc3KwIDAQAB\n-----END RSA PUBLIC KEY-----\n\n"
  @private_key "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEAzCBvDU/9sz1tPMonJMu9khHk0VaMet8bgJMShHpSYM7ul0yf\nihz1L7CU5ppZF+v2tr+xFt2aSSqFf4irpbOOuBryBPZUDBgQSIz/JpLLMBmioUq0\nEZfZfmG8XU1lygKsP+MoyQJ5umzt4i9e9mMV+LVwZmY88dhcFvDWkT0MG9yrYjX6\n5L8Lja4mXZWLAIZoUFaJ617SxeoP0Bv5bmAgH666T8q66l4FTagPHX/komRkQhvY\nH4FDcfTr/Z0mnSCuNreA2vJpXupekfgEPMwXz3zlPPQ7f4E+mqUO+p+XYUE7JBlT\nggYDcpEzPUt7Wk0gbe9K+ez7zDojoGkSxhc3KwIDAQABAoIBAHcNZ5edEruKVP7C\nbGgSiCL8WrcZQl+bZj/sBz3K1ebuactGfjogP4Qr+fwxA0tnbQIS9Sb/4i9QJIJI\nZMwE2HVaCdOJE2XmVwDpcxq9PNJ18RsfJbypEsmaGTFVpctXGb09MJlj3zkytN9Z\nf4o2KidfMwoWEO+An90lZA9bSoeodbabuaufHiNd2Llx67U67HzmsJhUYrg38ZC5\nMWexapTo4oS9cY4zQN3LwLNyawnekw8oSOOefTr0hnI33xQPZSrH/VCkXNpCUEer\nYigRJGi4GlThtFRnhNcniaRmUs3/EMI85SDvPbtpOSD87iV/uXZBcWfbpJe4NybC\ncZnhy0ECgYEA90t3t/H+Abyt81qgx/r4Fl1zLYSkCuIkllXBQJvrrGuQyLip4/k5\nYvfjUu0gQyaVoLj2oGKlHPXE3xYLK9yJmewRNV7eBmUpfCCjUROFSzCH4SP6u2C0\naG/9DUelQw3pAnMzgXt7ePB6Iw0hqKejDZrJPau7DQDDE1TeL5paCYsCgYEA00/z\nXUtG3sLqfDhrEOt1RfD2+YaAu6ESptX94TxdP/aBrIOy8A9j9XpaXkEwthcLuawL\nQEVjAZsBNIP5yFxI3t7wUL6mDE4nm5btKDYKQc8Fmv/48ynHRqdqXSXSHbLD62SH\nLM/Tli13jRGrSRnOq00T9R5eG1MEkfI6FESw/OECgYBbSu79Z0bAWWlWR4THjuz7\nRLB6g1cT9XxQS4Q2V9lfI66lixac5Kq80IqJWKTqZVojpWTWvNP7pvdw6/Bf1uCt\nhCquK0GH1tzDyEDCc5Rnt5jSErhDaGXxkDY5KtPlt0Ln9qNzD6T7druAKR7d5lUZ\ndqUIMVeyay+Y+WG07SSEFQKBgQCLCF+nUpAeoUCG2tgXGdTfX9wf8U9iJGiRPNr+\nBymTnC1VxJFHQdkS+p3axim2pRMh5wDAGOc7dzEjzHHcUlvfx+92MPovvnxw8qy3\neFbnVb7qbODvnN1wr1ZcUzYcNDKT/mCyK0ub0+6E8sswHbrNGrm23XQtpkGrhSSR\nkWCiAQKBgQDtGPigKl4j61FPgTITeNoG87mTWx7YE5L6AAVjIzH4YiLMsiSL6Cis\nt+NEeIO8O0JAULZdf9jZM1kaV9oGopBUErlcJKfhAndYeWbN2hA0lpPimfCMFaV8\n+9lG3KCkDZJClZkabPK82euAiDDPBWo3MxAeRxPghbIeBftNDRjhzw==\n-----END RSA PRIVATE KEY-----\n\n"

  def application do
    [
      mod: {MlDHT, []},
      env: [
        port: 3001,
        ipv4: true,
        ipv6: true,
        clusters: %{
          :crypto.hash(:sha3_256, "123") => {@cypher, @public_key, @private_key}
        },
        bootstrap_nodes: [
          {"sVdxj7AiF5xQdBxRwpO6GhaTl-TnXqqDnAssqg7VaaE"
           |> Base.url_decode64!(padding: false), "localhost", 3000}
          # {"32F54E697351FF4AEC29CDBAABF2FBE3467CC267", "router.bittorrent.com", 6881},
          # {"EBFF36697351FF4AEC29CDBAABF2FBE3467CC267", "router.utorrent.com", 6881},
          # {"9F08E1074F1679137561BAFE2CF62A73A8AFADC7", "dht.transmissionbt.com", 6881}
        ],
        k_bucket_size: 8
      ],
      applications: [:logger]
    ]
  end

  defp deps do
    [
      {:bencodex, "~> 1.0.0"},
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
