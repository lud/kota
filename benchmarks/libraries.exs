Mix.install(
  [
    {:rate_limiter, ">= 0.0.0"},
    {:ark, ">= 0.0.0"},
    {:ex_rated, ">= 0.0.0"},
    {:hammer, "~> 6.1"}
  ],
  config: [
    hammer: [
      backend:
        {Hammer.Backend.ETS,
         [expiry_ms: 60_000 * 60 * 4, cleanup_interval_ms: 60_000 * 10]}
    ]
  ]
)

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

  # Accepts a function that tries to increment the counter to more than
  # @max_in_time in less than @time_interval.
  def check(name, check_rate) do
    IO.puts("== Executing #{name}")

    {:span, count, time} = check_rate.()

    message = "Counted #{count} in #{time}ms"
    IO.puts("Finished #{name}, ")

    if count > @max_in_time and time <= @time_interval do
      IO.puts([IO.ANSI.red(), "KO    ", message, IO.ANSI.reset()])
    else
      IO.puts([IO.ANSI.green(), "OK    ", message, IO.ANSI.reset()])
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

  # todo use Kota from hexpm

  def demo_ark_drip do
    alias Ark.Drip

    {:ok, bucket} = Drip.start_link(max_drops: @max_in_time, range_ms: @time_interval)

    # Basic burst should be fine
    :ok = Drip.await(bucket)
    Process.sleep(900)

    count_span(199, fn counter ->
      for _ <- 1..99 do
        :ok = Drip.await(bucket)
        :ok = Counter.increment(counter)
      end

      Process.sleep(101)

      for _ <- 1..100 do
        :ok = Drip.await(bucket)
        :ok = Counter.increment(counter)
      end
    end)
  end

  def demo_hammer do
    {:allow, _} = Hammer.check_rate("test", @max_in_time, @time_interval)
    Process.sleep(900)

    count_span(199, fn counter ->
      for _ <- 1..99 do
        :ok = hammer_wait("test", @time_interval, @max_in_time)
        :ok = Counter.increment(counter)
      end

      Process.sleep(101)

      for _ <- 1..100 do
        :ok = hammer_wait("test", @time_interval, @max_in_time)
        :ok = Counter.increment(counter)
      end
    end)
  end

  defp hammer_wait(bucket, time, max) do
    case Hammer.check_rate(bucket, time, max) do
      {:allow, _} -> :ok
      {:deny, _} -> hammer_wait(bucket, time, max)
    end
  end
end

Checker.check("RateLimiter", &Checker.demo_rate_limiter/0)
Checker.check("ExRated", &Checker.demo_ex_rated/0)
Checker.check("Hammer", &Checker.demo_hammer/0)
Checker.check("Ark.Drip", &Checker.demo_ark_drip/0)
