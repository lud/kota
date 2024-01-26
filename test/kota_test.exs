defmodule KotaTest do
  alias Kota
  use ExUnit.Case, async: true

  defmodule H do
    def task(kota, id, start_time) do
      Task.async(fn ->
        :ok = Kota.await(kota)

        IO.puts(
          "took [#{id}] #{inspect(self())} #{:erlang.monotonic_time(:millisecond) - start_time}"
        )
      end)
    end

    def task_timeout(kota, id, timeout) do
      Task.async(fn ->
        IO.puts("await [#{id}] timeout=#{timeout}")

        try do
          :ok = Kota.await(kota, timeout)
          IO.puts("took [#{id}] #{inspect(self())} #{:erlang.monotonic_time(:second)}")
          :ok
        catch
          :exit, {:timeout, {GenServer, :call, _}} ->
            IO.puts("exit [#{id}]")

            :timeout
        end
      end)
    end
  end

  test "basic" do
    {:ok, pid} = Kota.start_link(max_allow: 10, range_ms: 1000)
    t1 = :erlang.monotonic_time(:millisecond)

    tasks =
      for n <- 1..20 do
        H.task(pid, n, t1)
      end

    IO.puts("awaiting")
    tasks |> Enum.map(&Task.await(&1, :infinity))
    t2 = :erlang.monotonic_time(:millisecond)
    # 20 drips at 10 per second should take at least 1 seconds
    # but less thant 2 seconds, since at time 0 we can run 10 tasks,
    # and at time 1 we can run the last 10
    assert t2 - t1 >= 1000
    assert t2 - t1 < 2000
  end

  test "1 drip slow" do
    Kota.start_link(max_allow: 1, range_ms: 1_000, name: __MODULE__.DripSlow)
    # The first call should be immediate and the second should wait
    # 1000 ms
    t1 = :erlang.monotonic_time(:millisecond)
    H.task(__MODULE__.DripSlow, :slow_1, t1) |> Task.await()
    H.task(__MODULE__.DripSlow, :slow_2, t1) |> Task.await()
    t2 = :erlang.monotonic_time(:millisecond)
    assert t2 - t1 >= 1000
    assert t2 - t1 < 1100
    # If the Drip is idle (time is waited long before), the call should be
    # immediate, so we expect it took least than 10 ms
    sleep_log(1_000)
    t1 = :erlang.monotonic_time(:millisecond)
    H.task(__MODULE__.DripSlow, :slow_3, t1) |> Task.await()
    t2 = :erlang.monotonic_time(:millisecond)
    assert t2 - t1 < 10
  end

  test "drip timeout" do
    {:ok, pid} = Kota.start_link(max_allow: 10, range_ms: 1000, slot_ms: 10)

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

    batch_1 = for(n <- 1..20, do: H.task_timeout(pid, "A #{n}", 500))
    sleep_log(100)
    batch_2 = for(n <- 1..10, do: H.task_timeout(pid, "B #{n}", 1500))
    sleep_log(100)
    batch_3 = for(n <- 1..20, do: H.task_timeout(pid, "C #{n}", 2500))

    assert_batch(batch_1, 10, 10)
    assert_batch(batch_2, 10, 0)
    assert_batch(batch_3, 10, 10)
  end

  defp assert_batch(tasks, expected_ok, expected_timeout) do
    {oks, tos} =
      tasks
      |> Enum.map(&Task.await(&1, :infinity))
      |> Enum.split_with(&(&1 == :ok))

    assert expected_ok == length(oks)
    assert expected_timeout == length(tos)
  end

  test "100 drips" do
    name = __MODULE__.Drip100
    Kota.start_link(max_allow: 10, range_ms: 100, name: name)

    t1 = :erlang.monotonic_time(:millisecond)

    tasks =
      for n <- 1..100 do
        H.task(name, n, t1)
      end

    IO.puts("awaiting")
    tasks |> Enum.map(&Task.await(&1, :infinity))
    t2 = :erlang.monotonic_time(:millisecond)

    assert t2 - t1 > 900
    assert t2 - t1 < 1100
  end

  test "window boundaries" do
    # Here we will ask 3 drips right before the first window end, and 3 more
    # right after. The second group must be delayed. To verify that, there
    # should be one second difference between the 1st drip and the 4th.

    {:ok, drip} = Kota.start_link(max_allow: 3, range_ms: 1000)
    sleep_log(700)

    f = fn ->
      Kota.await(drip)

      :erlang.monotonic_time(:millisecond)
    end

    items = [f.(), f.(), f.(), f.(), f.(), f.()]

    assert Enum.at(items, 3) - Enum.at(items, 0) >= 1000
  end

  test "Under supervision" do
    children = [
      {Kota, max_allow: 10, range_ms: 1100}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: __MODULE__.TestSupervisor]
    assert {:ok, _} = Supervisor.start_link(children, opts)
  end

  defp sleep_log(n) do
    IO.puts("sleep #{n}")
    Process.sleep(n)
  end
end
