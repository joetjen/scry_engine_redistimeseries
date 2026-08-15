defmodule Scry.Engine.RedisTimeSeries do
  @moduledoc """
  A real `Scry.Core.EngineBehaviour` implementation over
  [RedisTimeSeries](https://redis.io/docs/latest/develop/data-types/timeseries/),
  via [`redix`](https://hex.pm/packages/redix). `execute/3` translates
  a flat time-range `WHERE` into `TS.RANGE`'s own native `from`/`to`/
  `FILTER_BY_VALUE` bounds (`Scry.Engine.RedisTimeSeries.RangeQuery`),
  the real time-series-shaped counterpart to `scry_engine_ch`'s own
  general SQL translation -- validating the time-series kind against a
  second real product, this time one with no query language at all to
  translate *into*, only a fixed numeric-range command shape.

  `scry_time_series`'s own `LAST`-lowering pass rewrites `LAST
  <duration> OF <field>`/`LAST <from> TO <to> OF <field>` into an
  ordinary comparison (or two, `AND`-combined) before any engine ever
  sees it -- the identical "zero new code needed" finding `scry_engine_
  postgrex`'s own TimescaleDB validation and `scry_engine_ch`'s own
  landing both already established. This package needs no time-series-
  specific code of its own either; a correct, general translation of
  an ordinary time-bounded `WHERE` is already exactly what `LAST`
  becomes.

  ## Every row has exactly two fields: `timestamp` and `value`

  A RedisTimeSeries key holds nothing but an ordered sequence of
  `(timestamp, value)` samples -- there is no broader per-row schema
  the way a SQL table or an Elasticsearch document has, so this
  package exposes exactly those two fixed field names on every row,
  always: `"timestamp"` (a Unix-millisecond integer) and `"value"` (a
  double). A query's own `LAST ... OF <field>` must name that field
  `timestamp` for the lowered comparison to reference this package's
  own row shape correctly -- naming it anything else means the
  lowered predicate compares against a field no row has, which
  `Scry.Core.QueryOps`'s own generic null-safety check already catches
  as a real, clear error on its own (comparing a genuinely absent
  field is exactly the null-safety violation that check requires
  raising for) -- no special handling needed here for that case.

  Unlike `timestamp`/`value` in most SQL/document stores, **both
  fields are genuinely, always non-`nil` here** -- a "gap" in a series
  is an *absent sample* (the row itself doesn't exist), never a
  present row with a `nil` value, confirmed directly. `describe_
  source/2` reports both as `nullable: false` honestly, not as a
  simplifying assumption.

  ## What compiles, and what's declined

  `Scry.Engine.RedisTimeSeries.RangeQuery`'s own moduledoc has the
  full account of what `wheres` shapes translate. `GROUP BY`/any
  aggregate call in `select`, `HAVING`, and `DISTINCT` are all
  declined outright -- not merely unimplemented: `TS.RANGE`'s own
  native `AGGREGATION` construct buckets *by time*, not by an
  arbitrary field the way SQL `GROUP BY` does, and Scry's own language
  has no time-bucketing/hypertable construct yet to translate a real
  `GROUP BY` into that shape even in principle (the corrected
  `scry_engine_timescaledb` entry already found the identical gap
  from the SQL side). `ORDER BY`/`LIMIT`/`OFFSET` are
  **not** pushed down to `TS.RANGE` at all -- deliberately: `TS.RANGE`
  has no server-side sort-direction or offset concept, and pushing
  down only `COUNT` (`LIMIT`) while leaving direction to be applied
  afterward would silently return the wrong *set* of rows whenever the
  requested order is `DESC` (the first `N` in ascending storage order
  is not the same rows as the first `N` when sorted descending) --
  found reasoning through the interaction directly, not discovered as
  a bug. The full matching range is always fetched and handed to
  `Scry.Core.QueryOps.run_flat/3` for `ORDER BY`/`LIMIT`/`OFFSET`/
  projection, generically and correctly, the same "clear what's
  already handled, delegate the rest" split `scry_engine_elasticsearch`
  already established -- except here nothing about ordering/pagination
  was ever pushed down in the first place, so nothing needs clearing:
  only `wheres` is real pushdown.

  Key names are validated as a non-empty string, not against the
  identifier-safety pattern this family's SQL/HTTP-based siblings use
  -- deliberately: `redix` sends each command argument as its own
  length-prefixed RESP bulk string, never concatenated into a parsable
  command grammar the way a SQL string or a URL path is, so there is
  no injection surface to guard against here at all. A real Redis key
  name routinely contains characters (`:`, `/`) an identifier pattern
  would wrongly reject.
  """

  @behaviour Scry.Core.EngineBehaviour

  alias Scry.Core.{CombinedQuery, EngineBehaviour, Query, QueryOps}
  alias Scry.Engine.RedisTimeSeries.{Conn, RangeQuery}

  @aggregate_names ~w(sum avg count min max)

  @impl true
  def execute(conn, %CombinedQuery{} = combined, params),
    do: QueryOps.run_document(conn, combined, params, __MODULE__)

  def execute(%Conn{} = conn, %Query{source: source} = query, params) do
    if Enum.any?(query.select, &match?(%Query{}, &1)) or with_bound_source?(query) do
      QueryOps.run_document(conn, query, params, __MODULE__)
    else
      execute_flat(conn, source, query, params)
    end
  end

  defp with_bound_source?(%Query{source: [name], with_bindings: with_bindings}),
    do: Map.has_key?(with_bindings, name)

  defp with_bound_source?(_query), do: false

  defp execute_flat(conn, source, query, params) do
    with :ok <- check(query.havings == [], {:construct, :having}),
         :ok <- check(not aggregate_query?(query), {:construct, :group_by}),
         :ok <- check(not query.distinct, {:construct, :distinct}),
         {:ok, key} <- key_name(source),
         {:ok, range} <- RangeQuery.compile(query.wheres, params),
         {:ok, pairs} <- ts_range(conn, key, range) do
      rows = Enum.map(pairs, &to_row/1)
      QueryOps.run_flat(rows, %{query | wheres: []}, params)
    end
  end

  defp check(true, _detail), do: :ok
  defp check(false, detail), do: {:error, {:unsupported, detail}}

  defp aggregate_query?(query),
    do: query.group_bys != [] or Enum.any?(query.select, &aggregate_body_item?/1)

  defp aggregate_body_item?({:computed, _alias, {:call, name, _args}}),
    do: name in @aggregate_names

  defp aggregate_body_item?(_other), do: false

  defp key_name([name]) when is_binary(name) and byte_size(name) > 0, do: {:ok, name}
  defp key_name(source), do: {:error, {:unsupported, {:source, source}}}

  defp ts_range(%Conn{pid: pid}, key, %{from: from, to: to, value_filter: value_filter}) do
    command = ["TS.RANGE", key, from, to] ++ filter_clause(value_filter)

    case Redix.command(pid, command) do
      {:ok, pairs} -> {:ok, pairs}
      {:error, reason} -> {:error, {:query_error, reason}}
    end
  end

  defp filter_clause(nil), do: []
  defp filter_clause({min, max}), do: ["FILTER_BY_VALUE", min, max]

  defp to_row([timestamp, value]) do
    %{"timestamp" => timestamp, "value" => parse_value(value)}
  end

  defp parse_value(v) when is_number(v), do: v * 1.0

  defp parse_value(v) when is_binary(v) do
    case Float.parse(v) do
      {value, _rest} ->
        value

      :error ->
        raise ArgumentError, "RedisTimeSeries returned a non-numeric sample value: #{inspect(v)}"
    end
  end

  @doc """
  `Scry.Core.EngineBehaviour`'s optional `describe_source/2` callback
  -- confirms `source` exists via `TS.INFO`, then reports this
  package's own fixed, always-`nullable: false` `timestamp`/`value`
  field pair (this module's own moduledoc has the full reasoning for
  why both are genuinely, honestly non-nullable here).
  """
  @impl true
  @spec describe_source(Conn.t(), String.t()) ::
          {:ok, [EngineBehaviour.introspected_field()]}
          | {:error, :not_found}
          | {:error, {:introspection_error, term()}}
  def describe_source(%Conn{pid: pid}, source) do
    case Redix.command(pid, ["TS.INFO", source]) do
      {:ok, _info} ->
        {:ok,
         [
           %{name: "timestamp", nullable: false, scalar: :integer},
           %{name: "value", nullable: false, scalar: :float}
         ]}

      {:error, %Redix.Error{message: message}} ->
        # RedisTimeSeries has no structured "not found" error code
        # distinct from its own free-text message -- confirmed
        # directly, this is the one real message it sends for a
        # missing key, matched here deliberately rather than treating
        # every `TS.INFO` failure as introspection failure.
        if String.contains?(message, "does not exist") do
          {:error, :not_found}
        else
          {:error, {:introspection_error, message}}
        end

      {:error, reason} ->
        {:error, {:introspection_error, reason}}
    end
  end
end
