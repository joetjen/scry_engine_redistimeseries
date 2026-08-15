defmodule Scry.Engine.RedisTimeSeries.Conn do
  @moduledoc """
  Wraps a `redix` connection pid -- opened once via `open/1` and meant
  to be reused across many `Scry.Engine.RedisTimeSeries.execute/3`
  calls, matching the connection/config struct every real adapter
  exposes. Unlike `Scry.Engine.Ch.Conn`/`Scry.Engine.
  Myxql.Conn` (both `DBConnection`-based pool processes), `redix` is
  its own lightweight `GenServer` wrapping a single reconnecting TCP
  socket -- `Redix.start_link/1` already returns a plain, directly
  usable `pid`, the same low-ceremony shape `exqlite`/`duckdbex`'s own
  native handles have, just over the network instead of embedded.
  """

  @type t :: %__MODULE__{pid: pid()}

  defstruct pid: nil

  @default_opts [host: "127.0.0.1", port: 6379]

  @doc """
  Starts a `redix` connection against `opts` (`Redix.start_link/1`'s
  own options), merged over this module's own explicit local-Docker
  defaults (`host: "127.0.0.1", port: 6379`).
  """
  @spec open(keyword()) :: {:ok, t()} | {:error, term()}
  def open(opts \\ []) do
    with {:ok, pid} <- Redix.start_link(Keyword.merge(@default_opts, opts)) do
      {:ok, %__MODULE__{pid: pid}}
    end
  end

  @doc "Stops the wrapped connection."
  @spec close(t()) :: :ok
  def close(%__MODULE__{pid: pid}), do: GenServer.stop(pid)
end
