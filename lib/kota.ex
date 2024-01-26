defmodule Kota do
  @moduledoc false
  alias :queue, as: Q
  use GenServer

  # @mod Kota.Bucket.SlidingWindow
  @mod Kota.Bucket.DiscreteCounter

  def start_link(opts) do
    {gen_opts, opts} = split_gen_opts(opts)
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  def start(opts) do
    {gen_opts, opts} = split_gen_opts(opts)
    GenServer.start(__MODULE__, opts, gen_opts)
  end

  defp split_gen_opts(opts) when is_list(opts) do
    Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])
  end

  defp split_gen_opts({max_allow, range_ms}) do
    [max_allow: max_allow, range_ms: range_ms]
  end

  def stop(bucket) do
    GenServer.stop(bucket)
  end

  def await(bucket, timeout \\ :infinity)

  def await(bucket, :infinity) do
    GenServer.call(bucket, {:await, make_ref()}, :infinity)
  end

  def await(bucket, timeout) do
    ref = make_ref()

    try do
      GenServer.call(bucket, {:await, ref}, timeout)
    catch
      :exit, {:timeout, {GenServer, :call, _}} = e ->
        cancel(bucket, ref)
        exit(e)
    end
  end

  def cancel(bucket, ref) when is_reference(ref) do
    GenServer.cast(bucket, {:cancel, ref})
  end

  defmodule S do
    @moduledoc false
    @enforce_keys [:bucket, :clients]
    defstruct @enforce_keys
  end

  @impl GenServer
  def init(opts) do
    opts =
      opts
      |> Keyword.put_new(:start_time, now_ms())
      |> Keyword.put_new(:slot_ms, :one_tenth)

    {:ok, %S{bucket: @mod.new(opts), clients: Q.new()}}
  end

  @impl GenServer
  def handle_call({:await, ref}, from, %S{bucket: bucket, clients: q} = state) do
    now = now_ms()

    case @mod.take(bucket, now) do
      {:ok, bucket} ->
        state = %S{state | bucket: bucket}
        {:reply, :ok, state, :infinity}

      {:reject, bucket} ->
        state = %S{state | bucket: bucket, clients: Q.in({from, ref}, q)}

        {:noreply, state, next_timeout(state)}
    end
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    state = run_queue(state)

    {:noreply, state, next_timeout(state)}
  end

  defp run_queue(%S{clients: q, bucket: bucket} = state) do
    case Q.out(q) do
      {:empty, _} ->
        state

      {{:value, {from, _} = _client}, new_q} ->
        case @mod.take(bucket, now_ms()) do
          {:ok, bucket} ->
            GenServer.reply(from, :ok)

            run_queue(%S{state | bucket: bucket, clients: new_q})

          {:reject, bucket} ->
            %S{state | bucket: bucket}
        end
    end
  end

  @impl GenServer
  def handle_cast({:cancel, ref}, %S{clients: clients} = state) do
    clients =
      Q.filter(
        fn
          {_from, ^ref} ->
            false

          _ ->
            true
        end,
        clients
      )

    {:noreply, %S{state | clients: clients}, next_timeout(state)}
  end

  def now_ms do
    :erlang.system_time(:millisecond)
  end

  defp next_timeout(%{bucket: %mod{allowance: al} = bucket}) do
    if al == 0 do
      max(0, mod.next_refill!(bucket) - now_ms())
    else
      :infinity
    end
  end
end
