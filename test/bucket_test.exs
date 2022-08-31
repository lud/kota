defmodule Ark.Drip.BucketTest do
  use ExUnit.Case, async: true
  alias Ark.Drip.Bucket

  defp test_bucket(max_drops, range_ms, start_time, slot_time \\ 10) do
    assert {:ok, bucket} =
             Bucket.new_ok(
               max_drops: max_drops,
               range_ms: range_ms,
               start_time: start_time,
               slot_time: slot_time
             )

    expected_slot_time = start_time + slot_time
    expected_slot_end = expected_slot_time

    assert %Bucket{
             range_ms: ^range_ms,
             slot_usage: 0,
             slot_time: ^expected_slot_time,
             slot_end: ^expected_slot_end,
             count: 0,
             refills: _,
             allowance: ^max_drops,
             max_drops: ^max_drops
           } = bucket

    bucket
  end

  test "bucket force divisible time slot" do
    assert {:ok, _} =
             Bucket.new_ok(max_drops: 1, range_ms: 1000, start_time: 0, slot_time: 10)

    assert {:error, "slot time 999999 is greater than range 1000"} =
             Bucket.new_ok(
               max_drops: 1,
               range_ms: 1000,
               start_time: 0,
               slot_time: 999_999
             )
  end

  test "threes, consume early" do
    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at time zero in slot 1/3

    assert {:ok, b} = Bucket.drop(b, 0)
    assert {:ok, b} = Bucket.drop(b, 0)
    assert {:ok, b} = Bucket.drop(b, 0)

    assert %{allowance: 0} = b

    # now that all drips have been consumed, further calls are rejected

    assert {:reject, b} = Bucket.drop(b, 0)

    # we are moving time to slot 2/3. calls should still be rejected

    assert {:reject, b} = Bucket.drop(b, 111)

    # same with slot 3/3

    assert {:reject, b} = Bucket.drop(b, 222)

    # now our bucket should be full and we can drop anew

    assert {:ok, b} = Bucket.drop(b, 300)
    assert {:ok, b} = Bucket.drop(b, 300)
    assert {:ok, b} = Bucket.drop(b, 300)
  end

  test "ignoring returned bucket from rejections works" do
    # this test should be the same as the one above, please update accordingly.
    # the only difference is that we discard any bucket state returned on
    # rejection

    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at time zero in slot 1/3

    assert {:ok, b} = Bucket.drop(b, 0)
    assert {:ok, b} = Bucket.drop(b, 0)
    assert {:ok, b} = Bucket.drop(b, 0)

    assert %{allowance: 0} = b

    # now that all drips have been consumed, further calls are rejected

    assert {:reject, _} = Bucket.drop(b, 0)

    # we are moving time to slot 2/3. calls should still be rejected

    assert {:reject, _} = Bucket.drop(b, 111)

    # same with slot 3/3

    assert {:reject, _} = Bucket.drop(b, 222)

    # now our bucket should be full and we can drop anew

    assert {:ok, b} = Bucket.drop(b, 300)
    assert {:ok, b} = Bucket.drop(b, 300)
    assert {:ok, b} = Bucket.drop(b, 300)
  end

  test "threes, consume late" do
    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at the end of the period

    assert {:ok, b} = Bucket.drop(b, 297)
    assert {:ok, b} = Bucket.drop(b, 298)
    assert {:ok, b} = Bucket.drop(b, 299)

    assert %{allowance: 0} = b

    # since we consumed 3 on the third third of the time period, we should only
    # be able to consume in the third third of the second period

    assert {:reject, b} = Bucket.drop(b, 300 + 0)
    assert {:reject, b} = Bucket.drop(b, 300 + 50)
    assert {:reject, b} = Bucket.drop(b, 300 + 100)
    assert {:reject, b} = Bucket.drop(b, 300 + 150)
    assert {:reject, b} = Bucket.drop(b, 300 + 200)
    assert {:reject, b} = Bucket.drop(b, 300 + 250)

    # The time slot is 10 by default in this test, and the last successful call was
    # at 299, so at 300 + 299 (599) we should be able to call

    assert {:reject, b} = Bucket.drop(b, 300 + 297)
    assert {:reject, b} = Bucket.drop(b, 300 + 298)

    assert {:ok, b} = Bucket.drop(b, 300 + 299)
    assert {:ok, b} = Bucket.drop(b, 300 + 299)
    assert {:ok, b} = Bucket.drop(b, 300 + 299)
  end

  test "threes, consume irregular" do
    b = test_bucket(3, 1000, 0)

    # we consume one in the 1/3, and two in the 2/3

    assert {:ok, b} = Bucket.drop(b, 0)
    assert {:ok, b} = Bucket.drop(b, 400)
    assert {:ok, b} = Bucket.drop(b, 400)

    # we cannot consume more in the 2/3 or in 3/3

    assert {:reject, b} = Bucket.drop(b, 400)
    assert {:reject, b} = Bucket.drop(b, 999)

    # in the second period we can consume one from the 1/3 and 2 in the 2/3

    assert {:ok, branch1} = Bucket.drop(b, 1000 + 0)
    assert {:reject, branch1} = Bucket.drop(branch1, 1000 + 0)
    assert {:ok, branch1} = Bucket.drop(b, 1000 + 400)
    assert {:ok, branch1} = Bucket.drop(b, 1000 + 400)

    # but if we don't, we can also consume 3 if we wait for the 3/3

    assert {:ok, _} = Bucket.drop(b, 1000 + 670)
  end

  test "consume in loop and max time" do
    # - we will create a bucket that can drop 20 in 100.
    # - we will drop 3000 drips
    # - whenever we encounter an :error (rejection), we warp 10ms in the
    #   future.
    # - this should take 15 seconds, so 15,000 ms. We will assert that we were
    #   able to do so in less than 15,000 ms

    # bucket config
    max_drops = 20
    range_ms = 100
    start_time = 0

    # test config
    iterations = 100
    warp_time = 10
    maximum_expected_time = iterations / max_drops * range_ms
    jitter = 3

    IO.puts(
      "maximum_expected_time = (#{iterations} / #{max_drops}) * #{range_ms} = #{round(iterations / max_drops)} * #{range_ms} = #{round(maximum_expected_time)}"
    )

    bucket = test_bucket(max_drops, range_ms, start_time)
    accin = {bucket, start_time}

    # a function that will increment time until the drop is accepted, and
    # returns the new bucket and the new time.

    loop = fn f, bucket, now ->
      case Bucket.drop(bucket, now) do
        {:reject, bucket} ->
          new_now = now + warp_time
          f.(f, bucket, new_now)

        {:ok, bucket} ->
          {bucket, now}
      end
    end

    {b, end_time, times} =
      Enum.reduce(1..iterations, {bucket, 0, []}, fn n, {bucket, now, times} ->
        {bucket, accepted_now} = loop.(loop, bucket, now)
        next_now = accepted_now + rem(accepted_now, 3)
        {bucket, next_now, [accepted_now | times]}
      end)

    assert iterations == b.count

    # we will generate each sliding window and assert that not too much drops
    # were made.
    freqs = times |> Enum.frequencies() |> Map.to_list() |> Enum.sort()
    # freqs |> IO.inspect(label: "freqs")

    windows =
      for index_start <- 0..(length(freqs) - 1) do
        {_, list} = Enum.split(freqs, index_start)
        [{first_freq_start, _} | _] = list

        list =
          Enum.take_while(list, fn {freq_start, _} ->
            freq_start < first_freq_start + range_ms
          end)
      end

    # windows |> IO.inspect(label: "windows")

    sums =
      Enum.map(windows, fn window ->
        sum = Enum.reduce(window, 0, fn {_, n}, acc -> acc + n end)

        if sum > max_drops do
          assert sum <= max_drops
        end
      end)

    assert end_time < maximum_expected_time
  end

  test "large gaps in time will simply reset the stage" do
    b = test_bucket(3, 1000, 0)

    # we can immediately enqueue the full capacity at time zero

    assert {:ok, b} = Bucket.drop(b, 997)
    assert {:ok, b} = Bucket.drop(b, 998)
    assert {:ok, b} = Bucket.drop(b, 999)

    assert %{allowance: 0} = b

    # ten seconds after, we should be in slot 3/3 but so much time has passed,
    # the state will reset

    # We use very large numbers so without the optimization the test never
    # completes

    assert {:ok, b} = Bucket.drop(b, 999_999_999_999_999_999)

    # the count is not reset
    assert 4 == b.count
    assert 2 == b.allowance
  end
end
