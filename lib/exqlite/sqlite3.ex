defmodule Exqlite.Sqlite3 do
  @moduledoc """
  The interface to the NIF implementation.
  """

  # If the database reference is closed, any prepared statements should be
  # dereferenced as well. It is entirely possible that an application does
  # not properly remove a stale reference.
  #
  # Will need to add a test for this and think of possible solution.

  # Need to figure out if we can just stream results where we use this
  # module as a sink.

  alias Exqlite.Flags
  alias Exqlite.Sqlite3NIF
  alias Exqlite.Error

  @type db() :: reference()
  @type statement() :: reference()
  @type row() :: list()
  @type open_mode :: :readwrite | :readonly | :nomutex
  @type open_opt :: {:mode, :readwrite | :readonly | [open_mode()]}

  @doc """
  Opens a new sqlite database at the Path provided.

  `path` can be `":memory"` to keep the sqlite database in memory.

  ## Options

    * `:mode` - use `:readwrite` to open the database for reading and writing
      , `:readonly` to open it in read-only mode or `[:readonly | :readwrite, :nomutex]`
      to open it with no mutex mode. `:readwrite` will also create
      the database if it doesn't already exist. Defaults to `:readwrite`.
      Note: [:readwrite, :nomutex] is not recommended.
  """
  @spec open(String.t(), [open_opt()]) :: {:ok, db()} | {:error, Error.t()}
  def open(path, opts \\ []) do
    mode = Keyword.get(opts, :mode, :readwrite)
    wrap_err(Sqlite3NIF.open(path, flags_from_mode(mode)))
  end

  defp flags_from_mode(:nomutex) do
    raise ArgumentError,
          "expected mode to be `:readwrite` or `:readonly`, can't use a single :nomutex mode"
  end

  defp flags_from_mode(:readwrite),
    do: do_flags_from_mode([:readwrite], [])

  defp flags_from_mode(:readonly),
    do: do_flags_from_mode([:readonly], [])

  defp flags_from_mode([_ | _] = modes),
    do: do_flags_from_mode(modes, [])

  defp flags_from_mode(mode) do
    raise ArgumentError,
          "expected mode to be `:readwrite`, `:readonly` or list of modes, but received #{inspect(mode)}"
  end

  defp do_flags_from_mode([:readwrite | tail], acc),
    do: do_flags_from_mode(tail, [:sqlite_open_readwrite, :sqlite_open_create | acc])

  defp do_flags_from_mode([:readonly | tail], acc),
    do: do_flags_from_mode(tail, [:sqlite_open_readonly | acc])

  defp do_flags_from_mode([:nomutex | tail], acc),
    do: do_flags_from_mode(tail, [:sqlite_open_nomutex | acc])

  defp do_flags_from_mode([mode | _tail], _acc) do
    raise ArgumentError,
          "expected mode to be `:readwrite`, `:readonly` or `:nomutex`, but received #{inspect(mode)}"
  end

  defp do_flags_from_mode([], acc),
    do: Flags.put_file_open_flags(acc)

  @doc """
  Closes the database and releases any underlying resources.
  """
  @spec close(db() | nil) :: :ok | {:error, Error.t()}
  def close(nil), do: :ok
  def close(conn), do: wrap_err(Sqlite3NIF.close(conn))

  @doc """
  Interrupt a long-running query.
  """
  @spec interrupt(db() | nil) :: :ok | {:error, Error.t()}
  def interrupt(nil), do: :ok
  def interrupt(conn), do: wrap_err(Sqlite3NIF.interrupt(conn))

  @doc """
  Executes an sql script. Multiple stanzas can be passed at once.
  """
  @spec execute(db(), String.t()) :: :ok | {:error, Error.t()}
  def execute(conn, sql), do: wrap_err(Sqlite3NIF.execute(conn, sql))

  @doc """
  Get the number of changes recently.

  **Note**: If triggers are used, the count may be larger than expected.

  See: https://sqlite.org/c3ref/changes.html
  """
  @spec changes(db()) :: {:ok, integer()} | {:error, Error.t()}
  def changes(conn), do: wrap_err(Sqlite3NIF.changes(conn))

  @spec prepare(db(), String.t()) :: {:ok, statement()} | {:error, Error.t()}
  def prepare(conn, sql), do: wrap_err(Sqlite3NIF.prepare(conn, sql))

  @spec bind(db(), statement(), nil) :: :ok | {:error, Error.t()}
  def bind(conn, statement, nil), do: bind(conn, statement, [])

  @spec bind(db(), statement(), list()) :: :ok | {:error, Error.t()}
  def bind(conn, statement, args) do
    wrap_err(Sqlite3NIF.bind(conn, statement, Enum.map(args, &convert/1)))
  end

  @spec columns(db(), statement()) :: {:ok, [binary()]} | {:error, Error.t()}
  def columns(conn, statement), do: wrap_err(Sqlite3NIF.columns(conn, statement))

  @spec step(db(), statement()) :: :done | :busy | {:row, row()} | {:error, Error.t()}
  def step(conn, statement), do: wrap_err(Sqlite3NIF.step(conn, statement))

  @spec multi_step(db(), statement()) ::
          :busy | {:rows, [row()]} | {:done, [row()]} | {:error, Error.t()}
  def multi_step(conn, statement) do
    chunk_size = Application.get_env(:exqlite, :default_chunk_size, 50)
    multi_step(conn, statement, chunk_size)
  end

  @spec multi_step(db(), statement(), integer()) ::
          :busy | {:rows, [row()]} | {:done, [row()]} | {:error, Error.t()}
  def multi_step(conn, statement, chunk_size) do
    case Sqlite3NIF.multi_step(conn, statement, chunk_size) do
      :busy ->
        :busy

      {:error, reason} ->
        wrap_err({:error, reason})

      {:rows, rows} ->
        {:rows, Enum.reverse(rows)}

      {:done, rows} ->
        {:done, Enum.reverse(rows)}
    end
  end

  @spec last_insert_rowid(db()) :: {:ok, integer()}
  def last_insert_rowid(conn), do: Sqlite3NIF.last_insert_rowid(conn)

  @spec transaction_status(db()) :: {:ok, :idle | :transaction}
  def transaction_status(conn), do: Sqlite3NIF.transaction_status(conn)

  @doc """
  Causes the database connection to free as much memory as it can. This is
  useful if you are on a memory restricted system.
  """
  @spec shrink_memory(db()) :: :ok | {:error, Error.t()}
  def shrink_memory(conn) do
    wrap_err(Sqlite3NIF.execute(conn, "PRAGMA shrink_memory"))
  end

  @spec fetch_all(db(), statement(), integer()) :: {:ok, [row()]} | {:error, Error.t()}
  def fetch_all(conn, statement, chunk_size) do
    {:ok, try_fetch_all(conn, statement, chunk_size)}
  catch
    :throw, {:error, _reason} = error -> wrap_err(error)
  end

  defp try_fetch_all(conn, statement, chunk_size) do
    case multi_step(conn, statement, chunk_size) do
      {:done, rows} -> rows
      {:rows, rows} -> rows ++ try_fetch_all(conn, statement, chunk_size)
      {:error, _reason} = error -> throw(error)
      :busy -> throw({:error, "Database busy"})
    end
  end

  @spec fetch_all(db(), statement()) :: {:ok, [row()]} | {:error, Error.t()}
  def fetch_all(conn, statement) do
    # Should this be done in the NIF? It can be _much_ faster to build a list
    # there, but at the expense that it could block other dirty nifs from
    # getting work done.
    #
    # For now this just works
    chunk_size = Application.get_env(:exqlite, :default_chunk_size, 50)
    fetch_all(conn, statement, chunk_size)
  end

  @doc """
  Serialize the contents of the database to a binary.
  """
  @spec serialize(db(), String.t()) :: {:ok, binary()} | {:error, Error.t()}
  def serialize(conn, database \\ "main") do
    wrap_err(Sqlite3NIF.serialize(conn, database))
  end

  @doc """
  Disconnect from database and then reopen as an in-memory database based on
  the serialized binary.
  """
  @spec deserialize(db(), String.t(), binary()) :: :ok | {:error, Error.t()}
  def deserialize(conn, database \\ "main", serialized) do
    wrap_err(Sqlite3NIF.deserialize(conn, database, serialized))
  end

  def release(_conn, nil), do: :ok

  @doc """
  Once finished with the prepared statement, call this to release the underlying
  resources.

  This should be called whenever you are done operating with the prepared statement. If
  the system has a high load the garbage collector may not clean up the prepared
  statements in a timely manner and causing higher than normal levels of memory
  pressure.

  If you are operating on limited memory capacity systems, definitely call this.
  """
  @spec release(db(), statement()) :: :ok | {:error, Error.t()}
  def release(conn, statement) do
    wrap_err(Sqlite3NIF.release(conn, statement))
  end

  @doc """
  Allow loading native extensions.
  """
  @spec enable_load_extension(db(), boolean()) :: :ok | {:error, Error.t()}
  def enable_load_extension(conn, flag) do
    result =
      if flag do
        Sqlite3NIF.enable_load_extension(conn, 1)
      else
        Sqlite3NIF.enable_load_extension(conn, 0)
      end

    wrap_err(result)
  end

  @doc """
  Send data change notifications to a process.

  Each time an insert, update, or delete is performed on the connection provided
  as the first argument, a message will be sent to the pid provided as the second argument.

  The message is of the form: `{action, db_name, table, row_id}`, where:

    * `action` is one of `:insert`, `:update` or `:delete`
    * `db_name` is a string representing the database name where the change took place
    * `table` is a string representing the table name where the change took place
    * `row_id` is an integer representing the unique row id assigned by SQLite

  ## Restrictions

    * There are some conditions where the update hook will not be invoked by SQLite.
      See the documentation for [more details](https://www.sqlite.org/c3ref/update_hook.html)
    * Only one pid can listen to the changes on a given database connection at a time.
      If this function is called multiple times for the same connection, only the last pid will
      receive the notifications
    * Updates only happen for the connection that is opened. For example, there
      are two connections A and B. When an update happens on connection B, the
      hook set for connection A will not receive the update, but the hook for
      connection B will receive the update.
  """
  @spec set_update_hook(db(), pid()) :: :ok | {:error, Error.t()}
  def set_update_hook(conn, pid) do
    wrap_err(Sqlite3NIF.set_update_hook(conn, pid))
  end

  @doc """
  Send log messages to a process.

  Each time a message is logged in SQLite a message will be sent to the pid provided as the argument.

  The message is of the form: `{:log, rc, message}`, where:

    * `rc` is an integer [result code](https://www.sqlite.org/rescode.html) or an [extended result code](https://www.sqlite.org/rescode.html#extrc)
    * `message` is a string representing the log message

  See [`SQLITE_CONFIG_LOG`](https://www.sqlite.org/c3ref/c_config_covering_index_scan.html) and
  ["The Error And Warning Log"](https://www.sqlite.org/errlog.html) for more details.

  ## Restrictions

    * Only one pid can listen to the log messages at a time.
      If this function is called multiple times, only the last pid will
      receive the notifications
  """
  @spec set_log_hook(pid()) :: :ok | {:error, Error.t()}
  def set_log_hook(pid) do
    wrap_err(Sqlite3NIF.set_log_hook(pid))
  end

  defp convert(%Date{} = val), do: Date.to_iso8601(val)
  defp convert(%Time{} = val), do: Time.to_iso8601(val)
  defp convert(%NaiveDateTime{} = val), do: NaiveDateTime.to_iso8601(val)
  defp convert(%DateTime{time_zone: "Etc/UTC"} = val), do: NaiveDateTime.to_iso8601(val)

  defp convert(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  defp convert(val), do: val

  defp wrap_err({:error, usage_error}) when is_atom(usage_error) do
    raise ArgumentError, usage_error
  end

  defp wrap_err({:error, sqlite_error_info}) when is_map(sqlite_error_info) do
    %{errcode: errcode, errstr: errstr, errmsg: errmsg} = sqlite_error_info
    {:error, %Exqlite.Error{code: errcode, reason: errstr, message: errmsg}}
  end

  defp wrap_err(success), do: success
end
