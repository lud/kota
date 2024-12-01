# System.put_env("MIX_INSTALL_FORCE", "true")

Mix.install([
  {:benchee, "~> 1.3"},
  {:kota, path: Path.absname(Path.dirname(Path.dirname(__ENV__.file)))}
])

range = 1000

start_kota! = fn per_sec, adapter ->
  {:ok, pid} =
    Kota.start_link(
      max_allow: per_sec,
      range_ms: range,
      adapter: adapter
    )

  pid
end

Benchee.run(
  %{
    "DiscreteCounter" =>
      {&Kota.await/1, before_scenario: &start_kota!.(&1, Kota.Bucket.DiscreteCounter)},
    "SlidingWindow" =>
      {&Kota.await/1, before_scenario: &start_kota!.(&1, Kota.Bucket.SlidingWindow)}
  },
  inputs: %{"1,000" => 1000, "5,000" => 5_000, "10,000" => 10_000, "50,000" => 50_000},
  time: 5,
  memory_time: 0
)
