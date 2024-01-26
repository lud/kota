defmodule Kota.With.SlidingWindowTest do
  alias Kota

  use Kota.Case,
    async: true,
    bmod: Kota.Bucket.SlidingWindow,
    log: false
end

defmodule Kota.With.DiscreteCounterTest do
  alias Kota

  use Kota.Case,
    async: true,
    bmod: Kota.Bucket.DiscreteCounter,
    log: false
end
