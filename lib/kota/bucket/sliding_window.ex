defmodule Kota.Bucket.SlidingWindow do
  alias Kota.Bucket
  @moduledoc false
  @enforce_keys [
    # Duration within which `max_allow` allowances will be given.
    :range_ms,

    # How much drips can be taken from the bucket in the `range_ms` period, used
    # to calculate the sliding window.
    :max_allow,

    # The maximum usage available for the current time.
    :allowance,

    # Count of allowed in the current time slot.
    :slot_usage,

    # The duration of a slot.
    :slot_ms,

    # The absolute time at which the current slot ends, (exclusive, a call
    # coming at that exact timestamp will belong to the new slot).
    :slot_end,

    # a queue of new allowances to come. New allowances are created at the end
    # of each time slot plus range_ms, incrementing allowance with the same
    # amount of the current usage.
    :refills,

    # Timestamp of the last take or slot closing, which will delimit the new
    # time slot and refill time.
    :last_change,

    # The total taken count from that bucket, used for tests.
    :count
  ]

  defstruct @enforce_keys

  def new(opts) do
    with {:ok, max_allow} <- Bucket.validate_pos_integer(opts, :max_allow),
         {:ok, range_ms} <- Bucket.validate_pos_integer(opts, :range_ms),
         {:ok, now} <- Bucket.validate_non_neg_integer(opts, :start_time),
         {:ok, slot_ms} <- validate_slot_ms(opts, range_ms),
         :ok <- verify_slot_ms(range_ms, slot_ms) do
      bucket = %__MODULE__{
        allowance: max_allow,
        count: 0,
        max_allow: max_allow,
        range_ms: range_ms,
        refills: :queue.new(),
        slot_end: now + slot_ms,
        slot_ms: slot_ms,
        slot_usage: 0,
        last_change: now
      }

      bucket
    else
      {:error, msg} -> raise ArgumentError, message: msg
    end
  end

  defp validate_slot_ms(opts, range_ms) do
    case opts[:slot_ms] do
      :one_tenth -> {:ok, div(range_ms, 10)}
      _ -> Bucket.validate_pos_integer(opts, :slot_ms)
    end
  end

  defp verify_slot_ms(range_ms, slot_ms) do
    if slot_ms <= range_ms do
      :ok
    else
      {:error, "slot time #{slot_ms} is greater than range #{range_ms}"}
    end
  end

  def take(%__MODULE__{slot_end: slend} = bucket, now) when now >= slend do
    bucket
    |> close_slot(now)
    |> refill(now)
    |> take(now)
  end

  def take(%__MODULE__{allowance: al, count: c, slot_usage: used} = bucket, now)
      when al > 0 do
    bucket = %__MODULE__{
      bucket
      | allowance: al - 1,
        slot_usage: used + 1,
        count: c + 1,
        last_change: now
    }

    # If the new allowance will be zero we can immediately close_slot

    case al do
      1 -> {:ok, close_slot(bucket, now)}
      _ -> {:ok, bucket}
    end
  end

  def take(bucket, _now) do
    # on failure, since we may have called close_slot(), we still return the updated
    # bucket.
    {:reject, bucket}
  end

  defp close_slot(bucket, now) do
    %__MODULE__{last_change: last, range_ms: range_ms} = bucket

    if now > last + range_ms do
      reset(bucket, now)
    else
      do_close_slot(bucket)
    end
  end

  defp do_close_slot(bucket) do
    %__MODULE__{
      last_change: last,
      range_ms: range_ms,
      slot_ms: slot_ms,
      slot_end: slot_end,
      refills: q,
      slot_usage: usage
    } = bucket

    new_slot_end = last + slot_ms
    refill_time = last + range_ms

    # in order to force the data to advance in time we force the last usage
    # date to the end of the slot that just finished
    last = slot_end

    q =
      case usage do
        0 -> q
        _ -> :queue.in({refill_time, usage}, q)
      end

    %{
      bucket
      | refills: q,
        slot_end: new_slot_end,
        slot_usage: 0,
        last_change: last
    }
  end

  defp reset(bucket, now) do
    %__MODULE__{max_allow: max_allow, slot_ms: slot_ms} = bucket

    %{
      bucket
      | allowance: max_allow,
        refills: :queue.new(),
        slot_end: now + slot_ms,
        slot_usage: 0,
        last_change: now
    }
  end

  defp refill(bucket, now) do
    %__MODULE__{refills: q, allowance: al} = bucket

    case :queue.peek(q) do
      {:value, {refill_time, amount}} when refill_time <= now ->
        refill(%{bucket | allowance: al + amount, refills: :queue.drop(q)}, now)

      _ ->
        bucket
    end
  end

  def next_refill!(%__MODULE__{refills: q}) do
    {:value, {refill_time, _}} = :queue.peek(q)
    refill_time
  end
end
