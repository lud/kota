defmodule Ark.Drip do
  use GenServer
  alias :queue, as: Q
  @dropped :"$dropped_drip"

  @doc false
  def __ark__(:doc) do
    """
    This module implements a `GenServer` and allows to throttle calls
    to a common resource.
    """
  end

  def start_link(opts) do
    args = Keyword.fetch!(opts, :spec)
    start_link(args, Keyword.delete(opts, :spec))
  end

  def start_link(args, opts) do
    args = read_args(args)
    GenServer.start_link(__MODULE__, args, opts)
  end

  def start(args, opts) do
    args = read_args(args)
    GenServer.start(__MODULE__, args, opts)
  end

  defp read_args({max_drips, range_ms}),
    do: [
      max_drips: max_drips,
      range_ms: range_ms
    ]

  defp read_args([{:max_drips, _}, {:range_ms, _}] = args), do: args
  defp read_args([{:range_ms, _}, {:max_drips, _}] = args), do: args

  def stop(bucket) do
    GenServer.stop(bucket)
  end

  def await(bucket, timeout \\ :infinity)

  def await(bucket, :infinity) do
    GenServer.call(bucket, :await, :infinity)
  end

  def await(bucket, timeout) do
    try do
      GenServer.call(bucket, :await, timeout)
    catch
      :exit, e ->
        cancel(bucket, self())
        exit(e)
    end
  end

  def cancel(bucket, pid) when is_pid(pid) do
    GenServer.cast(bucket, {:cancel, pid})
  end

  @doc false
  def init(args) do
    max_drips = Keyword.fetch!(args, :max_drips)
    range_ms = Keyword.fetch!(args, :range_ms)
    if max_drips < 1, do: raise("Minimum drip per period is 1, got: #{inspect(max_drips)}")
    if range_ms < 1, do: raise("Minimum period is 1, got: #{inspect(range_ms)}")

    state = %{
      current_drips: 0,
      max_drips: max_drips,
      range_ms: range_ms,
      clients: Q.new()
    }

    {:ok, state}
  end

  def handle_call(:await, _from, state = %{current_drips: current, max_drips: max_drips})
      when current < max_drips and current >= 0 do
    {:reply, :ok, bump_task(state)}
  end

  def handle_call(:await, from, %{current_drips: max_drips, max_drips: max_drips} = state) do
    # We are already at max drips, we will await a running task and rerun one immediately
    {:noreply, enqueue_client(state, from)}
  end

  def handle_cast({:cancel, pid}, state) do
    clients =
      Q.filter(
        fn
          {^pid, _} -> false
          _ -> true
        end,
        state.clients
      )

    {:noreply, %{state | clients: clients}}
  end

  defp bump_task(%{current_drips: current, max_drips: max_drips, range_ms: ms} = state)
       when current < max_drips do
    start_task(ms)
    %{state | current_drips: current + 1}
  end

  defp start_task(ms) do
    this = self()

    spawn_link(fn ->
      Process.sleep(ms)
      send(this, @dropped)
    end)
  end

  defp enqueue_client(state, client) do
    %{state | clients: Q.in(client, state.clients)}
  end

  def handle_info(@dropped, state) do
    case Q.out(state.clients) do
      {:empty, _} ->
        {:noreply, %{state | current_drips: state.current_drips - 1}}

      {{:value, client}, new_clients} ->
        GenServer.reply(client, :ok)
        start_task(state.range_ms)
        {:noreply, %{state | clients: new_clients}}
    end
  end
end
