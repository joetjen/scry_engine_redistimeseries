defmodule Scry.Engine.RedisTimeSeries.RangeQueryPropertyTest do
  @moduledoc """
  Property coverage for `RangeQuery`'s own bound-tightening invariant:
  whatever order a mix of `:ge`/`:le` timestamp bounds appears in
  `wheres`, the compiled `from` must equal the *maximum* of every
  `:ge` value seen and `to` the *minimum* of every `:le` value seen --
  the "multiple bounds on the same axis narrow to the tightest" claim
  this module's own moduledoc makes, exactly the kind of "must hold
  for every input" invariant AGENTS.md calls for a property test over,
  not just the one or two hand-picked examples the unit tests cover.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Scry.Engine.RedisTimeSeries.RangeQuery

  property "from is always the max of every :ge bound, to the min of every :le bound" do
    check all(
            ge_values <- StreamData.list_of(StreamData.integer(0..1_000_000), min_length: 1),
            le_values <-
              StreamData.list_of(StreamData.integer(1_000_001..2_000_000), min_length: 1)
          ) do
      wheres =
        Enum.map(ge_values, &{:cmp, :ge, ["timestamp"], &1}) ++
          Enum.map(le_values, &{:cmp, :le, ["timestamp"], &1})

      assert {:ok, %{from: from, to: to}} = RangeQuery.compile(wheres, %{})
      assert from == Integer.to_string(Enum.max(ge_values))
      assert to == Integer.to_string(Enum.min(le_values))
    end
  end

  property "value bounds narrow the same way, min/max as floats" do
    check all(
            ge_values <-
              StreamData.list_of(StreamData.float(min: 0.0, max: 1000.0), min_length: 1),
            le_values <-
              StreamData.list_of(StreamData.float(min: 1001.0, max: 2000.0), min_length: 1)
          ) do
      wheres =
        Enum.map(ge_values, &{:cmp, :ge, ["value"], &1}) ++
          Enum.map(le_values, &{:cmp, :le, ["value"], &1})

      assert {:ok, %{value_filter: {min, max}}} = RangeQuery.compile(wheres, %{})
      assert min == Float.to_string(Enum.max(ge_values))
      assert max == Float.to_string(Enum.min(le_values))
    end
  end
end
