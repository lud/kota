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
      # How much drips in the time period, used to calculate the sliding window
      :max_drops,

      # Duration limiting allowing max_drops drops
      :range_ms,

      # The maximum usage available for the current time.
      :allowance,

      # Count of drops in the current time slot
      :slot_usage,

      # The duration of a slot
      :slot_time,

      # The absolute time at which the current slot ends, exclusive (a call
      # coming at that exact timestamp will belong to the new slot)
      :slot_end,

      # The total dropped count
      :count,

      # a queue of new allowances to come. New allowances are created at the end
      # of each time slot plus range_ms, incrementing allowance with the same
      # amount of the current usage
      :refills,

      # Timestamp of the last used, which will delimit the new time slot and
      # refill time
      :last_use
    ]

    defstruct @enforce_keys

    def new_ok(max_drops, range_ms) do
      new_ok(max_drops: max_drops, range_ms: range_ms)
    end

    def new_ok(opts) do
      with {:ok, max_drops} <- validate_pos_integer(opts, :max_drops),
           {:ok, range_ms} <- validate_pos_integer(opts, :range_ms),
           {:ok, slot_time} <- validate_pos_integer(opts, :slot_time),
           {:ok, now} <- validate_non_neg_integer(opts, :start_time),
           :ok <- verify_slot_time(range_ms, slot_time) do
        bucket = %__MODULE__{
          allowance: max_drops,
          count: 0,
          max_drops: max_drops,
          range_ms: range_ms,
          refills: Q.new(),
          slot_end: now + slot_time,
          slot_time: slot_time,
          slot_usage: 0,
          last_use: now
        }

        {:ok, bucket}
      end
    end

    defp verify_slot_time(range_ms, slot_time) do
      if slot_time <= range_ms do
        :ok
      else
        {:error, "slot time #{slot_time} is greater than range #{range_ms}"}
      end
    end

    def drop(%__MODULE__{slot_end: slend} = bucket, now) when now >= slend do
      bucket
      |> rotate(now)
      |> refill(now)
      |> drop(now)
    end

    def drop(%__MODULE__{allowance: al, count: c, slot_usage: used} = bucket, now)
        when al > 0 do
      bucket = %__MODULE__{
        bucket
        | allowance: al - 1,
          slot_usage: used + 1,
          count: c + 1,
          last_use: now
      }

      {:ok, bucket}
    end

    def drop(bucket, _now) do
      # since we may have called rotate(), we still return the updated bucket.
      {:reject, bucket}
    end

    defp rotate(bucket, now) do
      %__MODULE__{
        last_use: last,
        range_ms: range_ms,
        slot_time: sltime,
        slot_end: old_slend,
        refills: q,
        slot_usage: usage
      } = bucket

      if now > last + range_ms do
        reset(bucket, now)
      else
        do_rotate(bucket, now)
      end
    end

    defp reset(bucket, now) do
      %__MODULE__{max_drops: max_drops, slot_time: slot_time} = bucket

      %__MODULE__{
        bucket
        | allowance: max_drops,
          refills: Q.new(),
          slot_end: now + slot_time,
          slot_usage: 0,
          last_use: now
      }
    end

    defp do_rotate(bucket, now) do
      %__MODULE__{
        last_use: last,
        range_ms: range_ms,
        slot_time: sltime,
        slot_end: old_slend,
        refills: q,
        slot_usage: usage
      } = bucket

      IO.puts("rotate slend: #{old_slend}, now: #{now}")

      slend = last + sltime
      refill_time = last + range_ms

      # in order to force the data to advance in time we force the last usage
      # date to the end of the slot that just finished
      last = old_slend

      q =
        case usage do
          0 -> q
          _ -> Q.in({refill_time, usage}, q)
        end

      %__MODULE__{bucket | refills: q, slot_end: slend, slot_usage: 0, last_use: last}
    end

    defp refill(bucket, now) do
      %__MODULE__{refills: q, allowance: al} = bucket

      case Q.out(q) do
        {{:value, {refill_time, amount}}, new_q} when refill_time <= now ->
          refill(%__MODULE__{bucket | allowance: al + amount, refills: new_q}, now)

        _ ->
          bucket
      end
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

  defp split_gen_opts({max_drops, range_ms}) do
    [max_drops: max_drops, range_ms: range_ms]
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
  #        %S{clients: q, used: 0, max_drops: max, slot_ms: slot, next_allow: next} = state
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

  defp next_timeout(%{bucket: %{slot_end: slend}}, now) do
    t = max(0, slend - now)

    if t != :ehllo do
      raise "use a :continue to run the queue before new demands"
    end
  end
end
