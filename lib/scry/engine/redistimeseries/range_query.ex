defmodule Scry.Engine.RedisTimeSeries.RangeQuery do
  @moduledoc """
  Translates a `Scry.Core.Query.t()`'s own `wheres` into `TS.RANGE`'s
  own native `from`/`to`/`FILTER_BY_VALUE min max` bounds -- for
  `Scry.Engine.RedisTimeSeries`'s own `execute/3`. A genuinely
  different shape from every SQL-engine sibling in this family (and
  from `scry_engine_elasticsearch`'s own Query DSL): `TS.RANGE` has no
  boolean query language at all, only two independent numeric ranges
  (one on the series' own timestamp axis, one on its value axis) --
  there is no way to represent an `OR`/`NOT` as a single `TS.RANGE`
  call, so this module supports exactly the shape a time-range query
  actually needs and nothing wider.

  ## What compiles

  Every predicate leaf must be `{:cmp, op, [field], value}` where
  `field` is exactly `"timestamp"` or `"value"` -- the two fixed field
  names `Scry.Engine.RedisTimeSeries` always exposes per row (that
  module's own moduledoc has the full reasoning) -- combined only via
  `{:and, l, r}` (an implicit multi-item `wheres` list is `AND`ed the
  same way; a literal `{:and, ...}` node parses to the identical
  shape). This is deliberately exactly wide enough for lang_spec.md
  §8.2's own two `LAST` forms once `scry_time_series`'s own lowering
  pass has already rewritten them into ordinary comparisons before
  this module ever sees them: `LAST <duration> OF <field>` lowers to
  one lower timestamp bound, `LAST <from> TO <to> OF <field>` lowers
  to two bounds `AND`-combined -- both already flat `{:and, ...}` trees
  of `{:cmp, ...}` leaves, never anything wider. An `{:or, ...}`/
  `{:not, ...}` node, an `{:in, ...}` leaf, or a field other than
  `"timestamp"`/`"value"` all decline outright -- none reduce to a
  single numeric range.

  On `"timestamp"`: `:eq` narrows `from`/`to` to the same single
  millisecond value (a point query); `:ge`/`:le` set `from`/`to`
  directly; `:gt`/`:lt` adjust by one millisecond (`from: v + 1`/
  `to: v - 1`) since `TS.RANGE`'s own bounds are always inclusive and
  a timestamp is always a whole millisecond integer, so "strictly
  after `v`" and "at or after `v + 1`" are exactly the same set of
  points -- a safe, exact adjustment, not an approximation. A
  `DateTime.t()`/`NaiveDateTime.t()` value converts to its own Unix
  millisecond timestamp; a bare integer is assumed already-milliseconds
  (the native query-builder API's own equivalent).

  On `"value"`: `:eq` narrows `FILTER_BY_VALUE` to a single point the
  same way; `:ge`/`:le` set the min/max bound directly. **`:gt`/`:lt`
  decline outright here** -- unlike the integer timestamp axis, a
  `value` is a real double, and there is no exact, safe adjustment
  analogous to the millisecond one (no "next representable float" the
  query itself specifies) to make an exclusive bound into `TS.RANGE`'s
  own inclusive one without risking silently including or excluding a
  boundary value a real float comparison wouldn't.

  Multiple bounds on the same axis narrow (the tightest `from` and the
  tightest `to`/`FILTER_BY_VALUE` win, matching how `AND` already
  combines them logically) -- a combination that can never be satisfied
  (`from > to`, or a `value` min above its own max) is left for
  `TS.RANGE` itself to correctly return zero rows for, not specially
  detected here.
  """

  alias Scry.Core.Query

  @typedoc "Compiled `TS.RANGE` arguments, ready to append after `key`."
  @type compiled :: %{
          from: String.t(),
          to: String.t(),
          value_filter: {String.t(), String.t()} | nil
        }

  @type bounds :: %{
          from: integer() | nil,
          to: integer() | nil,
          value_min: float() | nil,
          value_max: float() | nil
        }

  @spec compile([Query.predicate()], map()) ::
          {:ok, compiled()} | {:error, {:unsupported, term()}}
  def compile(wheres, params) do
    initial = %{from: nil, to: nil, value_min: nil, value_max: nil}

    wheres
    |> Enum.reduce_while({:ok, initial}, fn predicate, {:ok, acc} ->
      case collect_bounds(predicate, params, acc) do
        {:ok, _acc} = ok -> {:cont, ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, bounds} -> {:ok, finalize(bounds)}
      {:error, _} = err -> err
    end
  end

  defp collect_bounds({:and, l, r}, params, acc) do
    with {:ok, acc2} <- collect_bounds(l, params, acc) do
      collect_bounds(r, params, acc2)
    end
  end

  defp collect_bounds({:cmp, op, ["timestamp"], value}, params, acc) do
    with {:ok, resolved} <- resolve_value(value, params),
         {:ok, ms} <- timestamp_ms(resolved) do
      apply_timestamp_bound(op, ms, acc)
    end
  end

  defp collect_bounds({:cmp, op, ["value"], value}, params, acc) do
    with {:ok, resolved} <- resolve_value(value, params),
         {:ok, num} <- numeric_value(resolved) do
      apply_value_bound(op, num, acc)
    end
  end

  defp collect_bounds(_other, _params, _acc),
    do: {:error, {:unsupported, {:construct, :non_range_predicate}}}

  defp apply_timestamp_bound(:eq, v, acc), do: {:ok, tighten(acc, from: v, to: v)}
  defp apply_timestamp_bound(:ge, v, acc), do: {:ok, tighten(acc, from: v)}
  defp apply_timestamp_bound(:gt, v, acc), do: {:ok, tighten(acc, from: v + 1)}
  defp apply_timestamp_bound(:le, v, acc), do: {:ok, tighten(acc, to: v)}
  defp apply_timestamp_bound(:lt, v, acc), do: {:ok, tighten(acc, to: v - 1)}

  defp apply_timestamp_bound(:not_eq, _v, _acc),
    do: {:error, {:unsupported, {:construct, :timestamp_not_eq}}}

  defp apply_value_bound(:eq, v, acc), do: {:ok, tighten(acc, value_min: v, value_max: v)}
  defp apply_value_bound(:ge, v, acc), do: {:ok, tighten(acc, value_min: v)}
  defp apply_value_bound(:le, v, acc), do: {:ok, tighten(acc, value_max: v)}

  defp apply_value_bound(op, _v, _acc) when op in [:gt, :lt, :not_eq],
    do: {:error, {:unsupported, {:construct, {:value_bound, op}}}}

  defp tighten(bounds, from: v), do: Map.update!(bounds, :from, &max_or(&1, v))
  defp tighten(bounds, to: v), do: Map.update!(bounds, :to, &min_or(&1, v))

  defp tighten(bounds, from: f, to: t) do
    bounds |> Map.update!(:from, &max_or(&1, f)) |> Map.update!(:to, &min_or(&1, t))
  end

  defp tighten(bounds, value_min: v), do: Map.update!(bounds, :value_min, &max_or(&1, v))
  defp tighten(bounds, value_max: v), do: Map.update!(bounds, :value_max, &min_or(&1, v))

  defp tighten(bounds, value_min: mn, value_max: mx) do
    bounds |> Map.update!(:value_min, &max_or(&1, mn)) |> Map.update!(:value_max, &min_or(&1, mx))
  end

  defp max_or(nil, v), do: v
  defp max_or(current, v), do: max(current, v)
  defp min_or(nil, v), do: v
  defp min_or(current, v), do: min(current, v)

  defp resolve_value({:param, name}, params) do
    case Map.fetch(params, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:query_error, {:missing_param, name}}}
    end
  end

  defp resolve_value(value, _params), do: {:ok, value}

  defp timestamp_ms(%DateTime{} = dt), do: {:ok, DateTime.to_unix(dt, :millisecond)}

  defp timestamp_ms(%NaiveDateTime{} = dt) do
    {:ok, dt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:millisecond)}
  end

  defp timestamp_ms(v) when is_integer(v), do: {:ok, v}
  defp timestamp_ms(other), do: {:error, {:unsupported, {:timestamp_value, other}}}

  defp numeric_value(v) when is_number(v), do: {:ok, v * 1.0}
  defp numeric_value(other), do: {:error, {:unsupported, {:value_value, other}}}

  defp finalize(%{from: from, to: to, value_min: mn, value_max: mx}) do
    %{
      from: bound_to_string(from, "-"),
      to: bound_to_string(to, "+"),
      value_filter: value_filter(mn, mx)
    }
  end

  defp bound_to_string(nil, sentinel), do: sentinel
  defp bound_to_string(v, _sentinel), do: Integer.to_string(v)

  defp value_filter(nil, nil), do: nil

  defp value_filter(mn, mx),
    do: {float_to_string(mn || :neg_infinity), float_to_string(mx || :infinity)}

  defp float_to_string(:neg_infinity), do: "-inf"
  defp float_to_string(:infinity), do: "+inf"
  defp float_to_string(v), do: Float.to_string(v)
end
