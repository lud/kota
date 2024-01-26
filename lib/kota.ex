defmodule Kota do
  @moduledoc false
  alias :queue, as: Q
  use GenServer

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

  def stop(server) do
    GenServer.stop(server)
  end

  def await(server, timeout \\ :infinity)

  def await(server, :infinity) do
    GenServer.call(server, {:await, make_ref()}, :infinity)
  end

  def await(server, timeout) do
    ref = make_ref()

    try do
      GenServer.call(server, {:await, ref}, timeout)
    catch
      :exit, {:timeout, {GenServer, :call, _}} = e ->
        cancel(server, ref)
        exit(e)
    end
  end

  def cancel(server, ref) when is_reference(ref) do
    GenServer.cast(server, {:cancel, ref})
  end

  def total_count(server) do
    GenServer.call(server, :get_count)
  end

  defmodule S do
    @moduledoc false
    @enforce_keys [:bmod, :bucket, :clients]
    defstruct @enforce_keys
  end

  @impl GenServer
  def init(opts) do
    {bmod, opts} = Keyword.pop!(opts, :bmod)

    opts =
      opts
      |> Keyword.put_new(:start_time, now_ms())
      |> Keyword.put_new(:slot_ms, :one_tenth)

    {:ok, %S{bmod: bmod, bucket: bmod.new(opts), clients: Q.new()}}
  end

  @impl GenServer
  def handle_call({:await, ref}, from, state) do
    %S{clients: q} = state
    state = %S{state | clients: Q.in({from, ref}, q)}
    {:noreply, state, {:continue, :run_queue}}
  end

  def handle_call(:get_count, _from, state) do
    {:reply, state.bucket.count, state, {:continue, :run_queue}}
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    {:noreply, state, {:continue, :run_queue}}
  end

  @impl GenServer
  def handle_continue(:run_queue, state) do
    state = run_queue(state)
    {:noreply, state, next_timeout(state)}
  end

  defp run_queue(state) do
    %S{clients: q, bucket: bucket} = state

    case Q.out(q) do
      {:empty, _} ->
        state

      {{:value, {from, _} = _client}, new_q} ->
        case state.bmod.take(bucket, now_ms()) do
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

    {:noreply, %S{state | clients: clients}, {:continue, :run_queue}}
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
