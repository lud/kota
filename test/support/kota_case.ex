# credo:disable-for-this-file Credo.Check.Refactor.LongQuoteBlocks
defmodule Kota.Case do
  @moduledoc """
  A test suite for testing `Kota` with a given bucket module.
  """
  alias Kota
  use ExUnit.CaseTemplate

  using use_ctx do
    bmod = Keyword.fetch!(use_ctx, :bmod)
    log = Keyword.fetch!(use_ctx, :log)

    quote location: :keep, bind_quoted: [bmod: bmod, log: log] do
      import Kota.Case

      setup ctx do
        log =
          case unquote(log) do
            # use "apply" so we can `sed 's/IO.puts/ctx.log./'`
            true -> fn msg -> apply(IO, :puts, [msg]) end
            _ -> fn _ -> :ok end
          end

        Map.merge(ctx, %{log: log, bmod: unquote(bmod)})
      end

      test "basic", ctx do
        {:ok, pid} = Kota.start_link(max_allow: 10, range_ms: 1000, bmod: ctx.bmod)
        t1 = :erlang.monotonic_time(:millisecond)

        tasks =
          for n <- 1..20 do
            task(pid, n, t1, ctx)
          end

        ctx.log.("awaiting")
        tasks |> Enum.map(&Task.await(&1, :infinity))
        t2 = :erlang.monotonic_time(:millisecond)
        # 20 drips at 10 per second should take at least 1 seconds
        # but less thant 2 seconds, since at time 0 we can run 10 tasks,
        # and at time 1 we can run the last 10
        assert t2 - t1 >= 1000
        assert t2 - t1 < 2000
      end

      test "1 drip slow", ctx do
        Kota.start_link(
          max_allow: 1,
          range_ms: 1_000,
          name: __MODULE__.DripSlow,
          bmod: ctx.bmod
        )

        # The first call should be immediate and the second should wait
        # 1000 ms
        t1 = :erlang.monotonic_time(:millisecond)
        task(__MODULE__.DripSlow, :slow_1, t1, ctx) |> Task.await()
        task(__MODULE__.DripSlow, :slow_2, t1, ctx) |> Task.await()
        t2 = :erlang.monotonic_time(:millisecond)
        assert t2 - t1 >= 1000
        assert t2 - t1 < 1100
        # If the Drip is idle (time is waited long before), the call should be
        # immediate, so we expect it took least than 10 ms
        sleep_log(1_000, ctx)
        t1 = :erlang.monotonic_time(:millisecond)
        task(__MODULE__.DripSlow, :slow_3, t1, ctx) |> Task.await()
        t2 = :erlang.monotonic_time(:millisecond)
        assert t2 - t1 < 10
      end

      test "drip timeout", ctx do
        {:ok, pid} =
          Kota.start_link(max_allow: 10, range_ms: 1000, slot_ms: 10, bmod: ctx.bmod)

        # Run different batches :
        # - batch 1 (20 tasks) with a timeout of 500 will have 10 tasks ok and 10
        #   tasks timeout
        # - refill will be at 1000 ms
        # - batch 2 (only 10 tasks) with a timeout of 1200 will wait until refill
        #   and be served
        # - refill will be at 2000 ms
        # - batch 3 (20 tasks) with timeout of 2500 wil have ok/fail 10/10

        # We will sleep in between to assume that the tasks are calling the server
        # before starting new tasks.

        batch_1 = for(n <- 1..20, do: task_timeout(pid, "A #{n}", 500, ctx))
        sleep_log(100, ctx)
        batch_2 = for(n <- 1..10, do: task_timeout(pid, "B #{n}", 1500, ctx))
        sleep_log(100, ctx)
        batch_3 = for(n <- 1..20, do: task_timeout(pid, "C #{n}", 2500, ctx))

        assert_batch(batch_1, 10, 10)
        assert_batch(batch_2, 10, 0)
        assert_batch(batch_3, 10, 10)
      end

      test "100 drips", ctx do
        name = __MODULE__.Drip100
        Kota.start_link(max_allow: 10, range_ms: 100, name: name, bmod: ctx.bmod)

        t1 = :erlang.monotonic_time(:millisecond)

        tasks =
          for n <- 1..100 do
            task(name, n, t1, ctx)
          end

        ctx.log.("awaiting")
        tasks |> Enum.map(&Task.await(&1, :infinity))
        t2 = :erlang.monotonic_time(:millisecond)

        assert t2 - t1 > 900
        assert t2 - t1 < 1100
      end

      test "window boundaries", ctx do
        # Here we will ask 3 drips right before the first window end, and 3 more
        # right after. The second group must be delayed. To verify that, there
        # should be one second difference between the 1st drip and the 4th.

        {:ok, drip} = Kota.start_link(max_allow: 3, range_ms: 1000, bmod: ctx.bmod)
        sleep_log(700, ctx)

        f = fn ->
          Kota.await(drip)

          :erlang.monotonic_time(:millisecond)
        end

        items = [f.(), f.(), f.(), f.(), f.(), f.()]

        assert Enum.at(items, 3) - Enum.at(items, 0) >= 1000
      end

      test "Under supervision", ctx do
        children = [
          {Kota, max_allow: 10, range_ms: 1100, bmod: ctx.bmod}
        ]

        # See https://hexdocs.pm/elixir/Supervisor.html
        # for other strategies and supported options
        opts = [strategy: :one_for_one, name: __MODULE__.TestSupervisor]
        assert {:ok, _} = Supervisor.start_link(children, opts)
      end

      test "gentle stress test", ctx do
        # launch many processes competing for the same bucket expect that taking
        # 10k drips at 1000/second takes less than 10 seconds And expect than
        # taking 1 more lasts at least 10 seconds
        n_procs = 10_000
        n_drips = 100
        range_ms = 1000
        await_drips = 10_000
        t1 = :erlang.monotonic_time(:millisecond)

        {:ok, kota} = Kota.start_link(max_allow: 1000, range_ms: range_ms, bmod: ctx.bmod)

        taker =
          recursive(fn
            _next, 0 ->
              :ok

            next, n ->
              ctx.log.("awaiting #{n}/#{n_drips}")
              Kota.await(kota)
              ctx.log.("stress #{n}/#{n_drips}")
              next.(n - 1)
          end)

        pidrefs =
          Enum.map(1..n_procs, fn _ ->
            Process.spawn(fn -> taker.(n_drips) end, [:link, :monitor])
          end)

        recursive(fn next ->
          case Kota.total_count(kota) do
            n when n >= 10_001 ->
              send(self(), {:time_at_more, :erlang.monotonic_time(:millisecond)})
              :ok

            10_000 ->
              send(self(), {:time_at_10k, :erlang.monotonic_time(:millisecond)})
              Process.sleep(10)
              next.()

            n ->
              ctx.log.("count #{ctx.bmod} #{n}/#{await_drips}")
              Process.sleep(100)
              next.()
          end
        end).()

        t2 = :erlang.monotonic_time(:millisecond)
        {_, t10k} = assert_received {:time_at_10k, _t10k}
        {_, tmore} = assert_received {:time_at_more, _tmore}

        ctx.log.(
          "stress test time for 10k #{inspect(ctx.bmod)}: #{format_time(t10k - t1)}"
        )

        assert t10k - t1 < 10_000

        ctx.log.(
          "stress test time for 10k+1+ #{inspect(ctx.bmod)}: #{format_time(tmore - t1)}"
        )

        assert tmore - t1 >= 10_000

        Enum.each(pidrefs, fn {pid, ref} ->
          Process.demonitor(ref, [:flush])
          true = Process.unlink(pid)
          Process.exit(pid, :kill)
        end)
      end
    end
  end

  def recursive(f) when is_function(f, 2) do
    fn next_acc -> f.(recursive(f), next_acc) end
  end

  def recursive(f) when is_function(f, 1) do
    fn -> f.(recursive(f)) end
  end

  def sleep_log(n, ctx) do
    ctx.log.("sleep #{n}")
    Process.sleep(n)
  end

  def assert_batch(tasks, expected_ok, expected_timeout) do
    {oks, tos} =
      tasks
      |> Enum.map(&Task.await(&1, :infinity))
      |> Enum.split_with(&(&1 == :ok))

    assert expected_ok == length(oks)
    assert expected_timeout == length(tos)
  end

  def task(kota, id, start_time, ctx) do
    Task.async(fn ->
      :ok = Kota.await(kota)

      ctx.log.(
        "took [#{id}] #{inspect(self())} #{:erlang.monotonic_time(:millisecond) - start_time}"
      )
    end)
  end

  def task_timeout(kota, id, timeout, ctx) do
    Task.async(fn ->
      ctx.log.("await [#{id}] timeout=#{timeout}")

      try do
        :ok = Kota.await(kota, timeout)
        ctx.log.("took [#{id}] #{inspect(self())} #{:erlang.monotonic_time(:second)}")
        :ok
      catch
        :exit, {:timeout, {GenServer, :call, _}} ->
          ctx.log.("exit [#{id}]")

          :timeout
      end
    end)
  end

  def format_time(ms) do
    format_time(ms, :ms)
  end

  defp format_time(ms, :ms) when ms >= 1000 do
    format_time(ms / 1000, :seconds)
  end

  defp format_time(s, :seconds) when s >= 60 do
    format_time(s / 60, :minutes)
  end

  defp format_time(n, unit) when is_integer(n),
    do: [Integer.to_string(n), " ", Atom.to_string(unit)]

  defp format_time(n, unit) when is_float(n),
    do: [:io_lib.format(~c"~.2f", [n]), " ", Atom.to_string(unit)]
end
