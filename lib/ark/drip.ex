defmodule Ark.Drip do
  use GenServer
  alias :queue, as: Q
  require Logger

  @doc false
  def __ark__(:doc) do
    """
    This module allows to throttle calls to a shared resource with a simple
    broker process.
    """
  end

  defmodule Bucket do
    @enforce_keys [
      # The two time period to count : {low_range, high_range}. For instance, for
      # 1000 ms we would have {333, 334}. 333 for two time periods and 334 for the
      # other, as (333 * 2) + 334 = 1000.
      # The high range is actually used by the first time period
      :ranges,

      # How much drips in the time period, used to calculate the sliding window
      :max_drips,

      # 1 | 2 | 3
      :stage,

      # A 3-tuple representing the current usage of the two previous time slots
      # and the present slot
      :usage,

      # The maximum usage available for the current slot.
      :allowance,

      # The time at which the current slot ends
      :slot_end,

      # The total dropped count
      :count
    ]

    defstruct @enforce_keys

    def new_ok(max_drips, range_ms) do
      new_ok(max_drips: max_drips, range_ms: range_ms)
    end

    def new_ok(opts) do
      with {:ok, max_drips} <- validate_pos_integer(opts, :max_drips),
           {:ok, range_ms} <- validate_pos_integer(opts, :range_ms),
           {:ok, now} <- validate_non_neg_integer(opts, :start_time),
           range_ms |> IO.inspect(label: "range_ms"),
           {:ok, {_, high_range} = ranges} <- calc_ranges(range_ms) do
        bucket = %__MODULE__{
          ranges: ranges,
          max_drips: max_drips,
          stage: 1,
          usage: {0, 0, 0},
          allowance: max_drips,
          slot_end: now + high_range,
          count: 0
        }

        {:ok, bucket}
      end
    end

    defp calc_ranges(range_ms) do
      range_ms |> IO.inspect(label: "range_ms")
      low_range = floor(range_ms / 3)
      high_range = range_ms - low_range * 2
      {:ok, {low_range, high_range}}
    end

    def drop(%__MODULE__{ranges: {low_range, high_range}, slot_end: slend} = bucket, now)
        when now >= slend do
      # if two empty slots have passed, we should not loop until we reach the
      # current time, because if used in a long lived application, maybe full days
      # have passed since the last call.
      # if so much time has passed, we will simply reset
      twice = high_range * 2

      bucket = if now - slend > twice, do: reset(bucket, now), else: rotate(bucket, now)

      drop(bucket, now)
    end

    def drop(%__MODULE__{allowance: al, usage: {u1, u2, u3}, count: c} = bucket, now)
        when al > 0 do
      {:ok,
       %__MODULE__{bucket | allowance: al - 1, usage: {u1, u2, u3 + 1}, count: c + 1}}
    end

    def drop(%__MODULE__{allowance: al} = bucket, now) do
      # since we will have called rotate(), we still return the updated bucket
      {:reject, bucket}
    end

    defp reset(%{max_drips: max_drips, ranges: {_, high_range}} = bucket, now) do
      %__MODULE__{
        bucket
        | stage: 1,
          usage: {0, 0, 0},
          allowance: max_drips,
          slot_end: now + high_range
      }
    end

    defp rotate(bucket, now) do
      %{
        stage: stage,
        max_drips: max,
        ranges: {low, high},
        slot_end: slend,
        usage: {u1, u2, u3}
      } = bucket

      {stage, slend} =
        case stage do
          1 -> {2, slend + low}
          2 -> {3, slend + low}
          3 -> {1, slend + high}
        end

      allowance = max - (u2 + u3)
      usage = {u2, u3, 0}

      %__MODULE__{
        bucket
        | stage: stage,
          slot_end: slend,
          usage: usage,
          allowance: allowance
      }
    end

    defp validate_pos_integer(opts, key) do
      case Keyword.fetch(opts, key) do
        {:ok, val} when is_integer(val) and val >= 1 ->
          {:ok, val}

        {:ok, val} ->
          {:error, "option #{inspect(key)} is not a positive integer: #{inspect(val)}"}

        :error ->
          {:error, "missing option #{inspect(key)}"}
      end
    end

    defp validate_non_neg_integer(opts, key) do
      case Keyword.fetch(opts, key) do
        {:ok, val} when is_integer(val) and val >= 0 ->
          {:ok, val}

        {:ok, val} ->
          {:error,
           "option #{inspect(key)} is not zero or a positive integer: #{inspect(val)}"}

        :error ->
          {:error, "missing option #{inspect(key)}"}
      end
    end
  end

  # defmacro print_usage(state) do
  #   if Mix.env() == :test do
  #     quote do
  #       st = _print_usage(unquote(state))
  #       Logger.flush()
  #       st
  #     end
  #   else
  #     quote do
  #       state
  #     end
  #   end
  # end

  # def _print_usage(%{count: count, used: used} = state) do
  #   Logger.debug("count: #{count + used} (#{used})")
  #   state
  # end

  # @moduledoc false

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

  defp split_gen_opts({max_drips, range_ms}) do
    [max_drips: max_drips, range_ms: range_ms]
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
      :exit, e ->
        cancel(bucket, ref)
        exit(e)
    end
  end

  def cancel(bucket, ref) when is_reference(ref) do
    GenServer.cast(bucket, {:cancel, ref})
  end

  defmodule S do
    @enforce_keys [:bucket, :clients]

    defstruct @enforce_keys
  end

  @impl GenServer
  def init(opts) do
    opts = Keyword.put_new(opts, :start_time, now_ms())
    opts |> IO.inspect(label: "opts")

    with {:ok, bucket} <- Bucket.new_ok(opts) do
      {:ok, %S{bucket: bucket, clients: Q.new()}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:await, ref}, from, %S{bucket: bucket, clients: q} = state) do
    now = now_ms()

    case Bucket.drop(bucket, now) do
      {:ok, bucket} ->
        Logger.debug("direct handle")
        state = %S{state | bucket: bucket}
        {:reply, :ok, state, :infinity}

      {:reject, bucket} ->
        Logger.debug("enqueue")
        state = %S{state | bucket: bucket, clients: Q.in({from, ref}, q)}
        {:noreply, state, next_timeout(state, now)}
    end
  end

  defp next_timeout(%{bucket: %{slot_end: slend}}, now) do
    max(0, slend - now)
  end

  # def handle_call({:await, ref}, from, %S{} = state) do
  #   Logger.debug("enqueuing")
  #   state = enqueue_client(state, {from, ref})
  #   {:noreply, state, next_timeout(state)}
  # end

  @impl GenServer
  def handle_info(:timeout, %S{bucket: bucket, clients: q} = state) do
    Logger.debug("-- timeout -----------------------")
    state = run_queue(state)
    {:noreply, state, next_timeout(state, now_ms())}
  end

  defp run_queue(%S{clients: q, bucket: bucket} = state) do
    case Q.out(q) do
      {:empty, _} ->
        state

      {{:value, {from, _} = _client}, new_q} ->
        case Bucket.drop(bucket, now_ms()) do
          {:ok, bucket} ->
            GenServer.reply(from, :ok)
            run_queue(%S{state | bucket: bucket, clients: new_q})

          {:reject, bucket} ->
            %S{state | bucket: bucket}
        end
    end
  end

  # defp run_queue(
  #        %S{clients: q, used: 0, max_drips: max, slot_ms: slot, next_allow: next} = state
  #      ) do
  #   # TODO @optimize we should keep the queue length around instead of computing
  #   # it everytime.
  #   len = Q.len(q)

  #   take_n = if len >= max, do: max, else: len

  #   Logger.debug("running #{take_n} from queue")

  #   {runnables, q} = Q.split(take_n, q)

  #   _ = Q.fold(fn {from, _}, _ -> GenServer.reply(from, :ok) end, nil, runnables)

  #   print_usage(%S{state | used: take_n, next_allow: next + slot * take_n, clients: q})
  # end

  # defp run_queue(%S{} = state) do
  #   state
  # end

  @impl GenServer
  def handle_cast({:cancel, ref}, %S{clients: clients} = state) do
    clients =
      Q.filter(
        fn
          {_from, ^ref} -> false
          _ -> true
        end,
        clients
      )

    {:noreply, %S{state | clients: clients}}
  end

  # defp enqueue_client(state, client) do
  #   q = Q.in(client, state.clients)
  #   %S{state | clients: q}
  # end

  # defp next_timeout(%{clients: clients, next_allow: next} = state) do
  #   # timeout = max(0, next - now_ms())

  #   t =
  #     case Q.is_empty(clients) do
  #       true -> :infinity
  #       false -> max(0, next - now_ms())
  #     end

  #   Logger.debug("next timeout: #{t}")
  #   Logger.flush()
  #   t
  # end

  def now_ms do
    :erlang.system_time(:millisecond)
  end
end
