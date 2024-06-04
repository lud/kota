defmodule Kota.With.SlidingWindowTest do
  alias Kota

  use Kota.Case,
    async: true,
    adapter: Kota.Bucket.SlidingWindow,
    log: false
end

defmodule Kota.With.DiscreteCounterTest do
  alias Kota

  use Kota.Case,
    async: true,
    adapter: Kota.Bucket.DiscreteCounter,
    log: false
end
