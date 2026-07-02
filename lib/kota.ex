defmodule Kota do
  @moduledoc """
  Kota is a simple rate limiter.

  ### Basic example

      {:ok, kota} = Kota.start_link(max_allow: 10, range_ms: 1000, adapter: Kota.Bucket.DiscreteCounter)
      :ok = Kota.await(kota)


  ### Adapters

  * `Kota.Bucket.DiscreteCounter` - This adapter just counts every call at the
    expense of memory usage.
  * `Kota.Bucket.SlidingWindow` - This adapter uses a sliding window technique
    to save on memory and processing but will result in a lower rate if used
    constantly without time to recover time lost by the window timespan.

  """

  alias :queue, as: Q
  use GenServer

  @doc """
  Starts a rate limiter process linked to the calling process.

      iex> {:ok, kota} =
      ...>   Kota.start_link(
      ...>     max_allow: 10,
      ...>     range_ms: 1000,
      ...>     adapter: Kota.Bucket.DiscreteCounter
      ...>   )
      iex> Kota.await(kota)
      :ok

  ### Options

  * `:adapter` - the bucket implementation, either `Kota.Bucket.DiscreteCounter`
    or `Kota.Bucket.SlidingWindow`. Required.
  * `:max_allow` - the number of allowances granted within a time range.
    Required.
  * `:range_ms` - the duration of the time range, in milliseconds. Required.
  * `:slot_ms` - the duration of the accounting slots used by
    `Kota.Bucket.SlidingWindow`, in milliseconds. Defaults to `:one_tenth`,
    which resolves to a tenth of `:range_ms` and requires `:range_ms` to be at
    least 10.
  * `:start_time` - the timestamp marking the start of the first time range, in
    milliseconds. Defaults to the current time.

  The `:name`, `:debug`, `:timeout`, `:spawn_opt` and `:hibernate_after` options
  of `GenServer.start_link/3` are also accepted.

  The limiter can also be started as part of a supervision tree:

      children = [
        {Kota, max_allow: 10, range_ms: 1000, adapter: Kota.Bucket.SlidingWindow}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """
  def start_link(opts) do
    {gen_opts, opts} = split_gen_opts(opts)
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Starts a rate limiter process without a link to the calling process.

  Accepts the same options as `start_link/1`.
  """
  def start(opts) do
    {gen_opts, opts} = split_gen_opts(opts)
    GenServer.start(__MODULE__, opts, gen_opts)
  end

  defp split_gen_opts(opts) when is_list(opts) do
    Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])
  end

  @doc """
  Synchronously stops the rate limiter process.
  """
  def stop(server) do
    GenServer.stop(server)
  end

  @doc """
  Blocks the calling process until the rate limiter grants an allowance, then
  returns `:ok`.

  Callers are served in order of arrival. When the maximum number of allowances
  for the current time range has been reached, the call waits until an
  allowance is available again.

      iex> {:ok, kota} =
      ...>   Kota.start_link(
      ...>     max_allow: 10,
      ...>     range_ms: 1000,
      ...>     adapter: Kota.Bucket.SlidingWindow
      ...>   )
      iex> Kota.await(kota)
      :ok

  The `timeout` argument accepts `:infinity` (the default) or a duration in
  milliseconds. When the timeout is reached before an allowance is granted, the
  request is removed from the waiting queue and the calling process exits, as
  with a `GenServer.call/3` timeout.
  """
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

  @doc false
  def cancel(server, ref) when is_reference(ref) do
    GenServer.cast(server, {:cancel, ref})
  end

  @doc """
  Returns the total number of allowances granted by the rate limiter since it
  started.

      iex> {:ok, kota} =
      ...>   Kota.start_link(
      ...>     max_allow: 10,
      ...>     range_ms: 1000,
      ...>     adapter: Kota.Bucket.DiscreteCounter
      ...>   )
      iex> :ok = Kota.await(kota)
      iex> :ok = Kota.await(kota)
      iex> Kota.total_count(kota)
      2
  """
  def total_count(server) do
    GenServer.call(server, :get_count)
  end

  defmodule S do
    @moduledoc false
    @enforce_keys [:adapter, :bucket, :clients]
    defstruct @enforce_keys
  end

  @impl GenServer
  def init(opts) do
    {adapter, opts} = Keyword.pop!(opts, :adapter)

    opts =
      opts
      |> Keyword.put_new(:start_time, now_ms())
      |> Keyword.put_new(:slot_ms, :one_tenth)

    {:ok, %S{adapter: adapter, bucket: adapter.new(opts), clients: Q.new()}}
  end

  @impl GenServer
  def handle_call({:await, ref}, from, state) do
    %S{clients: q} = state
    state = %{state | clients: Q.in({from, ref}, q)}
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
        case state.adapter.take(bucket, now_ms()) do
          {:ok, bucket} ->
            GenServer.reply(from, :ok)
            run_queue(%{state | bucket: bucket, clients: new_q})

          {:reject, bucket} ->
            %{state | bucket: bucket}
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

    {:noreply, %{state | clients: clients}, {:continue, :run_queue}}
  end

  @doc """
  Returns the current system time in milliseconds.

  This is the time source used by the rate limiter and its bucket adapters.
  """
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
