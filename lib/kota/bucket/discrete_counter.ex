defmodule Kota.Bucket.DiscreteCounter do
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

    # A minimal reimplementation of the :queue where we only balance the queue
    # when we need to, and not when inserting.
    :q_in,
    :q_out,

    # The total taken count from that bucket, used for tests.
    :count
  ]

  defstruct @enforce_keys

  def new(opts) do
    with {:ok, max_allow} <- Bucket.validate_pos_integer(opts, :max_allow),
         {:ok, range_ms} <- Bucket.validate_pos_integer(opts, :range_ms) do
      bucket = %__MODULE__{
        allowance: max_allow,
        count: 0,
        max_allow: max_allow,
        range_ms: range_ms,
        q_in: [],
        q_out: []
      }

      bucket
    else
      {:error, msg} -> raise ArgumentError, message: msg
    end
  end

  def take(%{allowance: 0} = bucket, now) do
    bucket
    |> refill(now)
    |> do_take(now)
  end

  def take(bucket, now) do
    do_take(bucket, now)
  end

  def do_take(%{allowance: 0} = bucket, _now) do
    {:reject, bucket}
  end

  def do_take(bucket, now) do
    %{allowance: al, count: c, range_ms: range_ms, q_in: q_in} = bucket
    al = al - 1
    c = c + 1
    q_in = insert_refill(q_in, now + range_ms)
    {:ok, %{bucket | allowance: al, count: c, q_in: q_in}}
  end

  defp insert_refill([time | t], time) do
    [{time, 2} | t]
  end

  defp insert_refill([{time, allow} | t], time) do
    [{time, allow + 1} | t]
  end

  defp insert_refill(list, time) do
    [time | list]
  end

  defp refill(bucket, now) do
    %{q_in: q_in, q_out: q_out, allowance: al} = bucket
    {q_in, q_out, add_allow} = rotate(q_in, q_out, now, 0)
    %{bucket | q_in: q_in, q_out: q_out, allowance: al + add_allow}
  end

  defp rotate([], [], _now, add) do
    {[], [], add}
  end

  defp rotate(q_in, [], now, add) do
    rotate([], :lists.reverse(q_in), now, add)
  end

  defp rotate(q_in, [time | tail], now, add) when is_integer(time) and time <= now do
    rotate(q_in, tail, now, add + 1)
  end

  defp rotate(q_in, [{time, allow} | t], now, add) when time <= now do
    rotate(q_in, t, now, add + allow)
  end

  defp rotate(q_in, q_out, _now, add) do
    {q_in, q_out, add}
  end

  def next_refill!(%{q_in: _, q_out: [h | _]}), do: next_refill(h)
  def next_refill!(%{q_in: [_ | _] = q_in, q_out: []}), do: next_refill(:lists.last(q_in))

  defp next_refill({time, _allow}), do: time
  defp next_refill(time), do: time
end
