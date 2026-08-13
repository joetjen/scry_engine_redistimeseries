defmodule Scry.Engine.RedisTimeSeriesTest do
  @moduledoc """
  `Scry.Engine.RedisTimeSeries` -- confirms `execute/3` translates a
  flat, `AND`-combined time-range `WHERE` (exactly the shape `scry_
  time_series`'s own `LAST`-lowering produces) into a real `TS.RANGE`
  call against a real RedisTimeSeries key, that `ORDER BY .. DESC`/
  `LIMIT`/`OFFSET` all compose correctly via `Scry.Core.QueryOps.
  run_flat/3` (never pushed down, this package's own moduledoc has the
  full reasoning), that `OR`/`NOT`/a non-`timestamp`/`value` field/
  `GROUP BY`/`HAVING`/`DISTINCT`/an exclusive `value` bound all decline
  outright, and that an unknown key is a clear, tagged error -- all
  composing correctly end to end through a real `Scry.Core.Executor.
  run/4` call.

  **Requires a real, reachable RedisTimeSeries-capable Redis server**
  -- run one locally via `docker run -d -p 6379:6379
  redis/redis-stack-server:latest`. Runs `async: false` -- every test
  shares one real server and a small, fixed key, recreated in
  `setup_all`.
  """

  use ExUnit.Case, async: false

  alias Scry.Core.{Cursor, Executor, Query}
  alias Scry.Engine.RedisTimeSeries, as: Engine
  alias Scry.Engine.RedisTimeSeries.Conn

  @key "scry_test_metric"

  setup_all do
    {:ok, conn} = Conn.open()

    Redix.command(conn.pid, ["DEL", @key])
    {:ok, "OK"} = Redix.command(conn.pid, ["TS.CREATE", @key, "LABELS", "sensor", "1"])

    Enum.each([{1000, 10.0}, {2000, 20.0}, {3000, 30.0}, {4000, 40.0}, {5000, 50.0}], fn {ts, v} ->
      {:ok, _} =
        Redix.command(conn.pid, ["TS.ADD", @key, Integer.to_string(ts), Float.to_string(v)])
    end)

    {:ok, conn: conn}
  end

  defp names(cursor), do: cursor |> Enum.to_list() |> Enum.map(& &1["timestamp"])

  describe "execute/3 -- plain range queries" do
    test "no wheres returns every sample", %{conn: conn} do
      query = %Query{source: [@key], select: [{:field, ["timestamp"]}, {:field, ["value"]}]}
      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [1000, 2000, 3000, 4000, 5000]
    end

    test "timestamp >= narrows the lower bound", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :ge, ["timestamp"], 3000}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [3000, 4000, 5000]
    end

    test "timestamp > excludes the boundary via the +1ms adjustment", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :gt, ["timestamp"], 3000}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [4000, 5000]
    end

    test "an AND-combined lower and upper timestamp bound narrows both ways -- the LAST <from> TO <to> shape",
         %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [
          {:and, {:cmp, :ge, ["timestamp"], 2000}, {:cmp, :le, ["timestamp"], 4000}}
        ],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [2000, 3000, 4000]
    end

    test "timestamp = narrows to a single point", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :eq, ["timestamp"], 3000}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [3000]
    end

    test "a DateTime.t() timestamp bound converts to its own Unix millisecond value", %{
      conn: conn
    } do
      # epoch + 3000ms == the third sample's own timestamp
      dt = DateTime.add(~U[1970-01-01 00:00:00Z], 3000, :millisecond)

      query = %Query{
        source: [@key],
        wheres: [{:cmp, :ge, ["timestamp"], dt}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [3000, 4000, 5000]
    end

    test "value >= narrows by FILTER_BY_VALUE, open-ended via +inf", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :ge, ["value"], 30.0}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [3000, 4000, 5000]
    end

    test "an AND-combined value range narrows both ways", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:and, {:cmp, :ge, ["value"], 20.0}, {:cmp, :le, ["value"], 40.0}}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [2000, 3000, 4000]
    end

    test "a $param resolves against params", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :ge, ["timestamp"], {:param, "since"}}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{"since" => 4000})
      assert names(cursor) == [4000, 5000]
    end
  end

  describe "execute/3 -- ORDER BY / LIMIT / OFFSET, never pushed down, always via run_flat/3" do
    test "ORDER BY timestamp DESC reverses correctly", %{conn: conn} do
      query = %Query{
        source: [@key],
        order_bys: [{["timestamp"], :desc}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [5000, 4000, 3000, 2000, 1000]
    end

    test "LIMIT + OFFSET page correctly over the full fetched range", %{conn: conn} do
      query = %Query{
        source: [@key],
        order_bys: [{["timestamp"], :asc}],
        limit: 2,
        offset: 1,
        select: [{:field, ["timestamp"]}]
      }

      assert {:ok, cursor} = Engine.execute(conn, query, %{})
      assert names(cursor) == [2000, 3000]
    end
  end

  describe "stated scope limits" do
    test "an OR node declines -- no single TS.RANGE can represent it", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:or, {:cmp, :ge, ["timestamp"], 4000}, {:cmp, :le, ["timestamp"], 1000}}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, :non_range_predicate}}} =
               Engine.execute(conn, query, %{})
    end

    test "a NOT node declines", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:not, {:cmp, :ge, ["timestamp"], 4000}}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, :non_range_predicate}}} =
               Engine.execute(conn, query, %{})
    end

    test "a field other than timestamp/value declines", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :eq, ["sensor"], "1"}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, :non_range_predicate}}} =
               Engine.execute(conn, query, %{})
    end

    test "an exclusive (gt/lt) value bound declines -- no exact float adjustment exists", %{
      conn: conn
    } do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :gt, ["value"], 20.0}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, {:value_bound, :gt}}}} =
               Engine.execute(conn, query, %{})
    end

    test "timestamp != declines -- no single range represents it", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :not_eq, ["timestamp"], 3000}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, :timestamp_not_eq}}} =
               Engine.execute(conn, query, %{})
    end

    test "GROUP BY declines -- no time-bucketing construct in the language yet", %{conn: conn} do
      query = %Query{
        source: [@key],
        group_bys: [["timestamp"]],
        select: [
          {:field, ["timestamp"]},
          {:computed, "n", {:call, "count", [{:field, ["value"]}]}}
        ]
      }

      assert {:error, {:unsupported, {:construct, :group_by}}} = Engine.execute(conn, query, %{})
    end

    test "HAVING declines", %{conn: conn} do
      query = %Query{
        source: [@key],
        havings: [{:cmp, :gt, ["value"], 1}],
        select: [{:field, ["timestamp"]}]
      }

      assert {:error, {:unsupported, {:construct, :having}}} = Engine.execute(conn, query, %{})
    end

    test "DISTINCT declines", %{conn: conn} do
      query = %Query{source: [@key], distinct: true, select: [{:field, ["value"]}]}
      assert {:error, {:unsupported, {:construct, :distinct}}} = Engine.execute(conn, query, %{})
    end
  end

  describe "an unknown key is a clear, tagged query_error, never a crash" do
    test "TS.RANGE against a nonexistent key", %{conn: conn} do
      query = %Query{source: ["ghost_metric_xyz"], select: [{:field, ["timestamp"]}]}
      assert {:error, {:query_error, _}} = Engine.execute(conn, query, %{})
    end
  end

  describe "end to end through Scry.Core.Executor.run/4" do
    test "a bounded range query executes correctly through the full pipeline", %{conn: conn} do
      query = %Query{
        source: [@key],
        wheres: [{:cmp, :ge, ["timestamp"], 4000}],
        select: [{:field, ["timestamp"]}, {:field, ["value"]}]
      }

      assert {:ok, cursor} = Executor.run(query, Engine, conn)

      assert Cursor.to_list(cursor) == [
               %{"timestamp" => 4000, "value" => 40.0},
               %{"timestamp" => 5000, "value" => 50.0}
             ]
    end
  end

  describe "execute/3 -- delegated to Scry.Core.QueryOps.run_document/4" do
    test "a WITH-bound source is delegated and produces correct results", %{conn: conn} do
      query = %Query{
        source: ["recent"],
        select: [{:field, ["timestamp"]}],
        with_bindings: %{
          "recent" => %Query{
            source: [@key],
            wheres: [{:cmp, :ge, ["timestamp"], 4000}],
            select: [{:field, ["timestamp"]}]
          }
        }
      }

      assert {:ok, cursor} = Executor.run(query, Engine, conn)
      assert Enum.map(Cursor.to_list(cursor), & &1["timestamp"]) == [4000, 5000]
    end
  end

  describe "describe_source/2 (Scry.Core.EngineBehaviour's optional callback)" do
    test "reports the fixed timestamp/value fields, always non-nullable", %{conn: conn} do
      assert {:ok, fields} = Engine.describe_source(conn, @key)

      assert Enum.sort_by(fields, & &1.name) == [
               %{name: "timestamp", nullable: false, scalar: :integer},
               %{name: "value", nullable: false, scalar: :float}
             ]
    end

    test "an unknown key is {:error, :not_found}", %{conn: conn} do
      assert Engine.describe_source(conn, "ghost_metric_xyz") == {:error, :not_found}
    end
  end
end
