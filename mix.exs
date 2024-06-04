defmodule Kota.MixProject do
  use Mix.Project

  @version "0.1.0"
  @repo "https://github.com/lud/kota"

  def project do
    [
      app: :kota,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [flags: ["-Wno_improper_lists"]],
      elixirc_paths: elixirc_paths(Mix.env()),
      description: "A GenServer based rate limiter",
      package: package(),
      source_url: @repo
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.15.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34.0", only: [:dev, :test], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [name: "kota", licenses: ["MIT"], links: %{"Github" => @repo}]
  end
end
