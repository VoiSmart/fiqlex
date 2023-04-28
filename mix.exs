defmodule FIQLEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :fiqlex,
      version: "0.1.8",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      name: "FIQLEx",
      description: "FIQL (Feed Item Query Language) parser and query build",
      package: package(),
      source_url: "https://github.com/calions-app/fiqlex",
      homepage_url: "https://github.com/calions-app/fiqlex",
      docs: [
        main: "FIQLEx",
        extras: ["README.md"]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  def package() do
    [
      name: "fiqlex",
      licenses: ["MIT"],
      links: %{GitHub: "https://github.com/calions-app/fiqlex"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.10"},
      {:timex, "~> 3.5"},
      {:ecto_sql, "~> 3.10", optional: true},
      {:credo, "~> 1.6", only: [:dev, :test]},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.14", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
