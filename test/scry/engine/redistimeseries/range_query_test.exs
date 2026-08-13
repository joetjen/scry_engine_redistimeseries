defmodule Scry.Engine.RedisTimeSeries.RangeQueryTest do
  use ExUnit.Case, async: true

  alias Scry.Engine.RedisTimeSeries.RangeQuery

  describe "compile/2 -- unbounded" do
    test "no wheres compiles to the -/+ sentinels" do
      assert RangeQuery.compile([], %{}) == {:ok, %{from: "-", to: "+", value_filter: nil}}
    end
  end

  describe "compile/2 -- timestamp bounds" do
    test ":ge sets from directly" do
      assert RangeQuery.compile([{:cmp, :ge, ["timestamp"], 1000}], %{}) ==
               {:ok, %{from: "1000", to: "+", value_filter: nil}}
    end

    test ":gt adjusts by +1ms" do
      assert RangeQuery.compile([{:cmp, :gt, ["timestamp"], 1000}], %{}) ==
               {:ok, %{from: "1001", to: "+", value_filter: nil}}
    end

    test ":le sets to directly" do
      assert RangeQuery.compile([{:cmp, :le, ["timestamp"], 5000}], %{}) ==
               {:ok, %{from: "-", to: "5000", value_filter: nil}}
    end

    test ":lt adjusts by -1ms" do
      assert RangeQuery.compile([{:cmp, :lt, ["timestamp"], 5000}], %{}) ==
               {:ok, %{from: "-", to: "4999", value_filter: nil}}
    end

    test ":eq narrows both from and to to the same point" do
      assert RangeQuery.compile([{:cmp, :eq, ["timestamp"], 3000}], %{}) ==
               {:ok, %{from: "3000", to: "3000", value_filter: nil}}
    end

    test "AND combines a lower and upper bound" do
      wheres = [{:and, {:cmp, :ge, ["timestamp"], 1000}, {:cmp, :le, ["timestamp"], 5000}}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:ok, %{from: "1000", to: "5000", value_filter: nil}}
    end

    test "multiple bounds on the same side narrow to the tightest" do
      wheres = [{:cmp, :ge, ["timestamp"], 1000}, {:cmp, :ge, ["timestamp"], 3000}]
      assert RangeQuery.compile(wheres, %{}) == {:ok, %{from: "3000", to: "+", value_filter: nil}}
    end

    test "a DateTime.t() converts to its own Unix millisecond value" do
      dt = DateTime.add(~U[1970-01-01 00:00:00Z], 1500, :millisecond)

      assert RangeQuery.compile([{:cmp, :ge, ["timestamp"], dt}], %{}) ==
               {:ok, %{from: "1500", to: "+", value_filter: nil}}
    end

    test "a NaiveDateTime.t() converts too, assumed UTC" do
      ndt = NaiveDateTime.add(~N[1970-01-01 00:00:00], 2500, :millisecond)

      assert RangeQuery.compile([{:cmp, :ge, ["timestamp"], ndt}], %{}) ==
               {:ok, %{from: "2500", to: "+", value_filter: nil}}
    end

    test ":not_eq declines" do
      assert RangeQuery.compile([{:cmp, :not_eq, ["timestamp"], 3000}], %{}) ==
               {:error, {:unsupported, {:construct, :timestamp_not_eq}}}
    end
  end

  describe "compile/2 -- value bounds" do
    test ":ge sets value_min, open-ended via +inf" do
      assert RangeQuery.compile([{:cmp, :ge, ["value"], 10.0}], %{}) ==
               {:ok, %{from: "-", to: "+", value_filter: {"10.0", "+inf"}}}
    end

    test ":le sets value_max, open-ended via -inf" do
      assert RangeQuery.compile([{:cmp, :le, ["value"], 90.0}], %{}) ==
               {:ok, %{from: "-", to: "+", value_filter: {"-inf", "90.0"}}}
    end

    test "AND combines a value range both ways" do
      wheres = [{:and, {:cmp, :ge, ["value"], 10.0}, {:cmp, :le, ["value"], 90.0}}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:ok, %{from: "-", to: "+", value_filter: {"10.0", "90.0"}}}
    end

    test ":eq narrows to a single point" do
      assert RangeQuery.compile([{:cmp, :eq, ["value"], 42.0}], %{}) ==
               {:ok, %{from: "-", to: "+", value_filter: {"42.0", "42.0"}}}
    end

    test "an integer value literal still compiles (coerced to float)" do
      assert RangeQuery.compile([{:cmp, :ge, ["value"], 10}], %{}) ==
               {:ok, %{from: "-", to: "+", value_filter: {"10.0", "+inf"}}}
    end

    test ":gt declines -- no exact float adjustment exists" do
      assert RangeQuery.compile([{:cmp, :gt, ["value"], 10.0}], %{}) ==
               {:error, {:unsupported, {:construct, {:value_bound, :gt}}}}
    end

    test ":lt declines" do
      assert RangeQuery.compile([{:cmp, :lt, ["value"], 10.0}], %{}) ==
               {:error, {:unsupported, {:construct, {:value_bound, :lt}}}}
    end

    test ":not_eq declines" do
      assert RangeQuery.compile([{:cmp, :not_eq, ["value"], 10.0}], %{}) ==
               {:error, {:unsupported, {:construct, {:value_bound, :not_eq}}}}
    end
  end

  describe "compile/2 -- $param resolution" do
    test "a {:param, name} resolves against params" do
      assert RangeQuery.compile([{:cmp, :ge, ["timestamp"], {:param, "since"}}], %{
               "since" => 2000
             }) ==
               {:ok, %{from: "2000", to: "+", value_filter: nil}}
    end

    test "a missing param is a clear query_error" do
      assert RangeQuery.compile([{:cmp, :ge, ["timestamp"], {:param, "since"}}], %{}) ==
               {:error, {:query_error, {:missing_param, "since"}}}
    end
  end

  describe "compile/2 -- declined shapes" do
    test "OR declines" do
      wheres = [{:or, {:cmp, :ge, ["timestamp"], 1}, {:cmp, :le, ["timestamp"], 2}}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:error, {:unsupported, {:construct, :non_range_predicate}}}
    end

    test "NOT declines" do
      wheres = [{:not, {:cmp, :ge, ["timestamp"], 1}}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:error, {:unsupported, {:construct, :non_range_predicate}}}
    end

    test "IN declines" do
      wheres = [{:in, ["timestamp"], [1, 2, 3]}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:error, {:unsupported, {:construct, :non_range_predicate}}}
    end

    test "a field other than timestamp/value declines" do
      wheres = [{:cmp, :eq, ["region"], "us"}]

      assert RangeQuery.compile(wheres, %{}) ==
               {:error, {:unsupported, {:construct, :non_range_predicate}}}
    end
  end
end
