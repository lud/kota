defmodule Kota.Bucket do
  @moduledoc false

  def validate_pos_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, val} when is_integer(val) and val >= 1 ->
        {:ok, val}

      {:ok, val} ->
        {:error, "option #{inspect(key)} is not a positive integer: #{inspect(val)}"}

      :error ->
        {:error, "missing option #{inspect(key)}"}
    end
  end

  def validate_non_neg_integer(opts, key) do
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
