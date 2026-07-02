# System.put_env("MIX_INSTALL_FORCE", "true")

Mix.install(
  [
    {:rate_limiter, ">= 0.0.0"},
    {:ex_rated, ">= 0.0.0"},
    {:hammer, ">= 0.0.0"},
    {:kota, ">= 0.0.0"}
  ],
  consolidate_protocols: true
)

defmodule HammerFixWindow do
  use Hammer, backend: :ets, algorithm: :fix_window
end

defmodule HammerFixWindowPerKey do
  use Hammer, backend: :ets, algorithm: :fix_window_per_key
end

defmodule HammerSlidingWindow do
  use Hammer, backend: :ets, algorithm: :sliding_window
end

defmodule HammerLeakyBucket do
  use Hammer, backend: :ets, algorithm: :leaky_bucket
end

defmodule HammerTokenBucket do
  use Hammer, backend: :ets, algorithm: :token_bucket
end

defmodule Counter do
  def start_link(initial), do: GenServer.start_link(__MODULE__, initial)

  def increment(counter, add \\ 1) do
    send(counter, {:inc, add})
    :ok
  end

  def reset(counter), do: GenServer.call(counter, :reset)
  def init(initial), do: {:ok, initial}
  def get_stop(counter), do: GenServer.call(counter, :get_stop)
  def get(counter), do: GenServer.call(counter, :get)

  def handle_call(:get_stop, _from, n), do: {:stop, :normal, n, n}
  def handle_call(:get, _from, n), do: {:reply, n, n}
  def handle_call(:reset, _from, n), do: {:reply, n, 0}
  def handle_info({:inc, add}, n), do: {:noreply, n + add}
end

defmodule Checker do
  @time_interval 1000
  @max_in_time 100

  def intro do
    IO.puts("""

    Scenario: drain the end of a rate window, cross the window boundary, then
    immediately drain the next window, targeting a rate of #{@max_in_time}
    hits per #{@time_interval}ms.

    Limiters that enforce the limit on average (per window or per refill)
    accept up to twice the nominal rate in a short burst across the boundary,
    which suits server-side input limiting. Limiters that enforce the limit
    over any sliding window spread the hits instead, which suits client-side
    usage such as respecting a remote API quota.
    """)
  end

  def maybe_check(name, check_rate) do
    case System.get_env("KOTA_BENCHMARK") do
      nil ->
        check(name, check_rate)

      "" ->
        check(name, check_rate)

      name_part ->
        if name =~ name_part,
          do: check(name, check_rate),
          else: :ok
    end
  end

  # Accepts a function that tries to increment the counter to more than
  # @max_in_time in less than @time_interval.
  def check(name, check_rate) do
    indent = "   "
    IO.puts("== Executing #{name}")

    {:span, count, time} = check_rate.()

    message = "#{count} hits in #{time}ms"
    IO.puts(indent <> "Finished  #{name}")

    if count > @max_in_time and time <= @time_interval do
      IO.puts([IO.ANSI.yellow(), indent, "BURST ALLOWED    ", message, IO.ANSI.reset()])
    else
      IO.puts([IO.ANSI.green(), indent, "BURST PREVENTED  ", message, IO.ANSI.reset()])
    end
  end

  def count_span(expected, f) do
    {:ok, counter} = Counter.start_link(0)
    {span_time, _} = :timer.tc(fn -> f.(counter) end, :millisecond)
    ^expected = Counter.get_stop(counter)
    {:span, expected, span_time}
  end

  def demo_rate_limiter do
    rl = RateLimiter.new(@time_interval, @max_in_time)

    # Trigger a timespan on the rate limiter
    :ok = RateLimiter.hit(rl, 1)

    # Sleep to the end of the timespan
    Process.sleep(@time_interval - 50)

    # start counting
    count_span(199, fn counter ->
      Counter.reset(counter)
      # finish the current bucket of 100
      :ok = RateLimiter.hit(rl, @max_in_time - 1)
      :ok = Counter.increment(counter, @max_in_time - 1)

      # start the next bucket
      Process.sleep(101)
      :ok = RateLimiter.hit(rl, @max_in_time)
      :ok = Counter.increment(counter, @max_in_time)
    end)
  end

  def demo_ex_rated do
    # Same as RateLmiter but we need to position ourselves at the end of a
    # second in system time
    t = :erlang.system_time(:milli_seconds)
    next_second = trunc(t / 1000) * 1000 + 1000
    diff = next_second - t
    # sleep till the next start of second minus 100ms
    Process.sleep(diff + @time_interval - 100)

    # start counting
    count_span(200, fn counter ->
      # Finish the current bucket
      for _ <- 1..@max_in_time do
        {:ok, _} = ExRated.check_rate("demo", @time_interval, @max_in_time)
        :ok = Counter.increment(counter)
      end

      # sleep over to the next second
      Process.sleep(101)

      for _ <- 1..@max_in_time do
        {:ok, _} = ExRated.check_rate("demo", @time_interval, @max_in_time)
        :ok = Counter.increment(counter)
      end
    end)
  end

  def demo_kota(bucket_adapter) do
    {:ok, bucket} =
      Kota.start_link(
        adapter: bucket_adapter,
        max_allow: @max_in_time,
        range_ms: @time_interval
      )

    # Basic burst should be fine
    :ok = Kota.await(bucket)
    Process.sleep(900)

    count_span(199, fn counter ->
      for _ <- 1..99 do
        :ok = Kota.await(bucket)
        :ok = Counter.increment(counter)
      end

      Process.sleep(101)

      for _ <- 1..100 do
        :ok = Kota.await(bucket)
        :ok = Counter.increment(counter)
      end
    end)
  end

  def demo_hammer(limiter, kind) do
    args =
      case kind do
        # window algorithms take a scale in milliseconds and a limit
        :window ->
          [@time_interval, @max_in_time]

        # bucket algorithms take a rate per second and a capacity
        :bucket ->
          [div(@max_in_time * 1000, @time_interval), @max_in_time]
      end

    hit = fn -> apply(limiter, :hit, ["demo" | args]) end

    {:ok, _} = limiter.start_link(clean_period: :timer.minutes(10))

    {:allow, _} = hit.()
    Process.sleep(900)

    count_span(199, fn counter ->
      for _ <- 1..99 do
        :ok = hammer_wait(hit)
        :ok = Counter.increment(counter)
      end

      Process.sleep(101)

      for _ <- 1..100 do
        :ok = hammer_wait(hit)
        :ok = Counter.increment(counter)
      end
    end)
  end

  # Hammer has no blocking call, its API tells the caller when to retry. The
  # sliding window algorithm records denied hits, so honoring the returned
  # delay is required, a hot retry loop would starve itself.
  defp hammer_wait(hit) do
    case hit.() do
      {:allow, _} ->
        :ok

      {:deny, retry_after} ->
        Process.sleep(max(retry_after, 1))
        hammer_wait(hit)
    end
  end
end

Checker.intro()

Checker.maybe_check("RateLimiter", &Checker.demo_rate_limiter/0)
Checker.maybe_check("ExRated", &Checker.demo_ex_rated/0)

Checker.maybe_check("Hammer :fix_window (default)", fn ->
  Checker.demo_hammer(HammerFixWindow, :window)
end)

Checker.maybe_check("Hammer :fix_window_per_key", fn ->
  Checker.demo_hammer(HammerFixWindowPerKey, :window)
end)

Checker.maybe_check("Hammer :sliding_window", fn ->
  Checker.demo_hammer(HammerSlidingWindow, :window)
end)

Checker.maybe_check("Hammer :leaky_bucket", fn ->
  Checker.demo_hammer(HammerLeakyBucket, :bucket)
end)

Checker.maybe_check("Hammer :token_bucket", fn ->
  Checker.demo_hammer(HammerTokenBucket, :bucket)
end)

Checker.maybe_check("Kota.Bucket.DiscreteCounter", fn ->
  Checker.demo_kota(Kota.Bucket.DiscreteCounter)
end)

Checker.maybe_check("Kota.Bucket.SlidingWindow", fn ->
  Checker.demo_kota(Kota.Bucket.SlidingWindow)
end)
