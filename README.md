# scry_engine_redistimeseries

A real [`Scry.Core.EngineBehaviour`](https://github.com/joetjen/scry_core)
implementation over [RedisTimeSeries](https://redis.io/docs/latest/develop/data-types/timeseries/),
via [`redix`](https://hex.pm/packages/redix). Translates a flat,
time-range `WHERE` into `TS.RANGE`'s own native `from`/`to`/
`FILTER_BY_VALUE` bounds -- the real time-series-shaped counterpart to
[`scry_engine_ch`](https://github.com/joetjen/scry_engine_ch)'s own
general SQL translation, validating the time-series kind against a
second real product, this time one with no query language at all to
translate *into*, only a fixed numeric-range command shape.

Named for the driver, product-qualified rather than driver-qualified:
`redix` also backs [`scry_engine_redisearch`](https://github.com/joetjen/scry_engine_redisearch)
(a different kind contract, same driver), so neither can claim the
bare `scry_engine_redix` (impl_spec.md §2's own naming-collision
fallback rule).

Source: <https://github.com/joetjen/scry_engine_redistimeseries>.
Specs live in the separate [`scry`](https://github.com/joetjen/scry)
repository; the behaviour this implements lives in
[`scry_core`](https://github.com/joetjen/scry_core).

## Usage

```elixir
{:ok, conn} = Scry.Engine.RedisTimeSeries.Conn.open()

{:ok, query} = Scry.Core.parse(~s(SELECT sensor1 LAST 5m OF timestamp { timestamp, value }))
{:ok, cursor} = Scry.Core.Executor.run(query, Scry.Engine.RedisTimeSeries, conn)
rows = Scry.Core.Cursor.to_list(cursor)
# rows == [%{"timestamp" => 1786655515097, "value" => 99.9}]
```

## Every row has exactly two fields: `timestamp` and `value`

A RedisTimeSeries key holds nothing but an ordered sequence of
`(timestamp, value)` samples -- there is no broader per-row schema the
way a SQL table has, so this package exposes exactly those two fixed
field names on every row, always: `"timestamp"` (a Unix-millisecond
integer) and `"value"` (a double). A query's own `LAST ... OF <field>`
must name that field `timestamp` for the lowered comparison to
reference this package's own row shape correctly.

## What compiles, and what's declined

Every predicate leaf in `WHERE` must be a plain comparison on
`"timestamp"` or `"value"`, combined only via `AND` -- exactly wide
enough for `LAST <duration> OF <field>`/`LAST <from> TO <to> OF
<field>` once `scry_time_series`'s own lowering pass has already
rewritten them into ordinary comparisons, and nothing wider: `TS.RANGE`
has no boolean query language, so an `OR`/`NOT` node, an `IN` leaf, or
a field other than `timestamp`/`value` all decline outright.

`GROUP BY`/aggregates/`HAVING`/`DISTINCT` all decline -- `TS.RANGE`'s
own native `AGGREGATION` buckets *by time*, not by an arbitrary field
the way SQL `GROUP BY` does, and Scry's own language has no
time-bucketing construct yet to translate a real `GROUP BY` into that
shape even in principle.

`ORDER BY`/`LIMIT`/`OFFSET` are **not** pushed down to `TS.RANGE` at
all, deliberately: `TS.RANGE` has no server-side sort-direction or
offset concept, and pushing down only `COUNT` while leaving direction
to be applied afterward would silently return the wrong *set* of rows
whenever the requested order is `DESC`. The full matching range is
always fetched and handed to `Scry.Core.QueryOps.run_flat/3` for
`ORDER BY`/`LIMIT`/`OFFSET`/projection instead.

## Installation

```elixir
def deps do
  [
    {:scry_engine_redistimeseries, "~> 0.1.0"}
  ]
end
```

## Documentation

Documentation is generated with [ExDoc](https://github.com/elixir-lang/ex_doc):

- Released versions are published to [HexDocs](https://hexdocs.pm) once the
  package ships, at <https://hexdocs.pm/scry_engine_redistimeseries>.
