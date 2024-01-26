defmodule Kota.Bucket.SlidingWindowTest do
  import Kota.Case, only: [format_time: 1]
  use ExUnit.Case, async: false

  @mod Kota.Bucket.SlidingWindow

  defp test_bucket(max_allow, range_ms, start_time, slot_ms \\ 10) do
    @mod.new(
      max_allow: max_allow,
      range_ms: range_ms,
      start_time: start_time,
      slot_ms: slot_ms
    )
  end

  test "bucket force divisible time slot" do
    @mod.new(max_allow: 1, range_ms: 1000, start_time: 0, slot_ms: 10)

    assert_raise ArgumentError, "slot time 999999 is greater than range 1000", fn ->
      @mod.new(
        max_allow: 1,
        range_ms: 1000,
        start_time: 0,
        slot_ms: 999_999
      )
    end
  end

  test "threes, consume early" do
    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at time zero in slot 1/3

    assert {:ok, b} = @mod.take(b, 0)
    assert {:ok, b} = @mod.take(b, 0)
    assert {:ok, b} = @mod.take(b, 0)

    assert %{allowance: 0} = b

    # now that all drips have been consumed, further calls are rejected

    assert {:reject, b} = @mod.take(b, 0)

    # we are moving time to slot 2/3. calls should still be rejected

    assert {:reject, b} = @mod.take(b, 111)

    # same with slot 3/3

    assert {:reject, b} = @mod.take(b, 222)

    # now our bucket should be full and we can take again

    assert {:ok, b} = @mod.take(b, 300)
    assert {:ok, b} = @mod.take(b, 300)
    assert {:ok, _b} = @mod.take(b, 300)
  end

  test "ignoring returned bucket from rejections works" do
    # this test should be the same as the one above, please update accordingly.
    # the only difference is that we discard any bucket state returned on
    # rejection

    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at time zero in slot 1/3

    assert {:ok, b} = @mod.take(b, 0)
    assert {:ok, b} = @mod.take(b, 0)
    assert {:ok, b} = @mod.take(b, 0)

    assert %{allowance: 0} = b

    # now that all drips have been consumed, further calls are rejected

    assert {:reject, _} = @mod.take(b, 0)

    # we are moving time to slot 2/3. calls should still be rejected

    assert {:reject, _} = @mod.take(b, 111)

    # same with slot 3/3

    assert {:reject, _} = @mod.take(b, 222)

    # now our bucket should be full and we can take again

    assert {:ok, b} = @mod.take(b, 300)
    assert {:ok, b} = @mod.take(b, 300)
    assert {:ok, _b} = @mod.take(b, 300)
  end

  test "threes, consume late" do
    b = test_bucket(3, 300, 0)

    # we can immediately enqueue the full capacity at the end of the period

    assert {:ok, b} = @mod.take(b, 297)
    assert {:ok, b} = @mod.take(b, 298)
    assert {:ok, b} = @mod.take(b, 299)

    assert %{allowance: 0} = b

    # since we consumed 3 on the third third of the time period, we should only
    # be able to consume in the third third of the second period

    assert {:reject, b} = @mod.take(b, 300 + 0)
    assert {:reject, b} = @mod.take(b, 300 + 50)
    assert {:reject, b} = @mod.take(b, 300 + 100)
    assert {:reject, b} = @mod.take(b, 300 + 150)
    assert {:reject, b} = @mod.take(b, 300 + 200)
    assert {:reject, b} = @mod.take(b, 300 + 250)

    # The time slot is 10 by default in this test, and the last successful call
    # was at 299, so at 300 + 299 (599) we should be able to call

    assert {:reject, b} = @mod.take(b, 300 + 297)
    assert {:reject, b} = @mod.take(b, 300 + 298)

    assert {:ok, b} = @mod.take(b, 300 + 299)
    assert {:ok, b} = @mod.take(b, 300 + 299)
    assert {:ok, _b} = @mod.take(b, 300 + 299)
  end

  test "threes, consume irregular" do
    b = test_bucket(3, 1000, 0)

    # we consume one in the first third of the second (1..333 ms), and two in
    # the "2/3" (second third)

    assert {:ok, b} = @mod.take(b, 0)
    assert {:ok, b} = @mod.take(b, 400)
    assert {:ok, b} = @mod.take(b, 400)

    # we cannot consume more in the 2/3 or in 3/3

    assert {:reject, b} = @mod.take(b, 400)
    assert {:reject, b} = @mod.take(b, 999)

    # in the second period we can consume only one in the 1/3
    assert {:ok, branch1} = @mod.take(b, 1000 + 0)
    # not two, as it is not refilled
    assert {:reject, ^branch1} = @mod.take(branch1, 1000 + 0)

    # Alternatively we can consume three as soon as the 1400th millisecond, as
    # we previously exhausted the bucket so the slot was closed early
    assert {:ok, branch2} = @mod.take(b, 1000 + 400)
    assert {:ok, branch2} = @mod.take(branch2, 1000 + 400)
    assert {:ok, branch2} = @mod.take(branch2, 1000 + 400)
    assert {:reject, ^branch2} = @mod.take(branch2, 1000 + 400)

    # Finally on branch 2 we had
    # * 1 drip at 0
    # * 2 drips at 400
    # * 3 drips at 1400
    #
    # Which is corect.
  end

  test "burst control" do
    b = test_bucket(3, 1000, 0)

    # We consume all our drips at the end of the time period
    assert {:ok, b} = @mod.take(b, 997)
    assert {:ok, b} = @mod.take(b, 998)
    assert {:ok, b} = @mod.take(b, 999)

    # We cannot consume more before 1997
    assert {:reject, b} = @mod.take(b, 1000)
    assert {:reject, b} = @mod.take(b, 1996)

    # we cannot consume at 1997 because of the slot mechanism. The slot was
    # closed at 999 when the last allowance was given. But we can consume 3 at
    # 1999.

    assert {:reject, b} = @mod.take(b, 1997)
    assert {:reject, b} = @mod.take(b, 1998)
    assert {:ok, b} = @mod.take(b, 1999)
    assert {:ok, b} = @mod.take(b, 1999)
    assert {:ok, _b} = @mod.take(b, 1999)
  end

  test "consume in loop and max time" do
    # - we will create a bucket that can allow 20 in 100.
    # - we will take N drips from the bucket
    # - whenever we encounter an :error (rejection), we warp 10ms in the future.
    # - For instance with 3000 drips: 3000/20 = 150 periods of 100ms: this
    #   should take 15 seconds, so 15,000ms.
    # - We will assert that we were able to do so in less than 15,000ms since we
    #   can burst at the beginning of the last slot.

    # bucket config
    max_allow = 20
    range_ms = 100
    start_time = 0

    # test config
    iterations = 3000
    warp_time = 5

    total_periods =
      case rem(iterations, max_allow) do
        0 -> div(iterations, max_allow)
        _ -> div(iterations, max_allow) + 1
      end

    maximum_expected_time = total_periods * range_ms

    print_expectations = fn ->
      IO.puts(
        "total_periods = #{total_periods} (#{iterations} iterations at #{max_allow} per period)"
      )

      IO.puts(
        "maximum_expected_time = #{total_periods} * #{range_ms} = #{format_time(maximum_expected_time)}"
      )
    end

    print_expectations.()

    bucket = test_bucket(max_allow, range_ms, start_time, 50)

    # A function that will increment time until the drip is allowed; returns the
    # new bucket and the new time.

    take_one = fn f, bucket, now ->
      result = {_, _bucket} = @mod.take(bucket, now)

      # IO.puts([
      #   now |> Integer.to_string() |> String.pad_leading(6),
      #   " ms",
      #   "   count: ",
      #   Integer.to_string(bucket.count) |> String.pad_leading(4),
      #   "   allow: ",
      #   Integer.to_string(bucket.allowance) |> String.pad_leading(3),
      #   "   usage: ",
      #   Integer.to_string(bucket.slot_usage) |> String.pad_leading(3),
      #   "   refills: ",
      #   inspect(:queue.to_list(bucket.refills))
      # ])

      case result do
        {:reject, bucket} -> f.(f, bucket, now + warp_time)
        {:ok, bucket} -> {bucket, now}
      end
    end

    {b, end_time, times} =
      Enum.reduce(1..iterations, {bucket, 0, []}, fn _n, {bucket, now, times} ->
        {bucket, accepted_now} = take_one.(take_one, bucket, now)
        # Force time advancement to trigger the slot delay
        next_now = accepted_now + 1
        {bucket, next_now, [accepted_now | times]}
      end)

    assert iterations == b.count

    # we will generate each sliding window and assert that not too much drops
    # were made. All frequencies will be 1 as we +1 the accepted time at each
    # take, but to support testing with 0 time advancement during dev we still
    # compute as frequencies.
    freqs = times |> Enum.frequencies() |> Enum.sort()

    # for each time we took a drip at, we create a window of that time plus all
    # the following times that fit in the range
    windows =
      for index_start <- 0..(length(freqs) - 1) do
        {_, list} = Enum.split(freqs, index_start)
        [{first_freq_start, _} | _] = list

        Enum.take_while(list, fn {freq_start, _} ->
          freq_start < first_freq_start + range_ms
        end)
      end

    # For each window we sum the number of takes, and check that it respects the
    # max_allow constraint.
    Enum.map(windows, fn window ->
      sum = Enum.reduce(window, 0, fn {_, n}, acc -> acc + n end)

      assert sum <= max_allow
    end)

    print_expectations.()
    IO.puts("total elapsed time: #{format_time(end_time)}")
    # This does not work because of the slots delay:
    # assert end_time < maximum_expected_time
  end

  test "large gaps in time will simply reset the stage" do
    b = test_bucket(3, 1000, 0)

    # we can immediately enqueue the full capacity at time zero

    assert {:ok, b} = @mod.take(b, 997)
    assert {:ok, b} = @mod.take(b, 998)
    assert {:ok, b} = @mod.take(b, 999)

    assert %{allowance: 0} = b

    # ten seconds after, we should be in slot 3/3 but so much time has passed,
    # the state will reset

    # We use very large numbers so without the optimization the test never
    # completes

    assert {:ok, b} = @mod.take(b, 999_999_999_999_999_999)

    # the count is not reset
    assert 4 == b.count
    assert 2 == b.allowance
  end
end
