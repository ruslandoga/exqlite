defmodule Exqlite do
  @moduledoc """
  The interface to the NIF implementation.
  """

  # TODO
  # If the database reference is closed, any prepared statements should be
  # dereferenced as well. It is entirely possible that an application does
  # not properly remove a stale reference.
  #
  # Will need to add a test for this and think of possible solution.

  # TODO
  # Need to figure out if we can just stream results where we use this
  # module as a sink.

  alias Exqlite.{Nif, Error}

  @type conn :: reference()
  @type stmt :: reference()

  # https://www.sqlite.org/c3ref/c_open_autoproxy.html
  open_flag_mappings = [
    readonly: 0x00000001,
    readwrite: 0x00000002,
    create: 0x00000004,
    deleteonclose: 0x00000008,
    exclusive: 0x00000010,
    autoproxy: 0x00000020,
    uri: 0x00000040,
    memory: 0x00000080,
    main_db: 0x00000100,
    temp_db: 0x00000200,
    transient_db: 0x00000400,
    main_journal: 0x00000800,
    temp_journal: 0x00001000,
    subjournal: 0x00002000,
    super_journal: 0x00004000,
    nomutex: 0x00008000,
    fullmutex: 0x00010000,
    sharedcache: 0x00020000,
    privatecache: 0x00040000,
    wal: 0x00080000,
    nofollow: 0x01000000,
    exrescode: 0x02000000
  ]

  open_flag_names = Enum.map(open_flag_mappings, fn {name, _value} -> name end)
  open_flag_union = Enum.reduce(open_flag_names, &{:|, [], [&1, &2]})
  @type open_flag :: unquote(open_flag_union)

  for {name, value} <- open_flag_mappings do
    defp open_flag_value(unquote(name)), do: unquote(value)
  end

  @doc """
  Opens a new sqlite database at the Path provided.

  `path` can be `":memory"` to keep the sqlite database in memory.

  ## Options

    * `:flags` - flags to use to open the database for reading and writing.
      Defaults to `[:readwrite, :create, :exrescode]`.
      See https://www.sqlite.org/c3ref/c_open_autoproxy.html for more options.

  """
  @spec open(String.t(), [open_flag]) :: {:ok, conn} | {:error, Error.t()}
  def open(path, flags \\ [:readwrite, :create, :exrescode]) do
    path = String.to_charlist(path)

    flags =
      Enum.reduce(flags, 0, fn flag, acc ->
        Bitwise.bor(acc, open_flag_value(flag))
      end)

    case Nif.open(path, flags) do
      {:ok, _conn} = ok -> ok
      {:error, reason} -> {:error, Error.exception(message: reason)}
      {:error, code, name} -> {:error, Error.exception(code: code, codename: name)}
    end
  end

  @doc """
  Closes the database and releases any underlying resources.
  """
  @spec close(conn) :: :ok | {:error, Error.t()}
  def close(conn), do: wrap_error(Nif.close(conn))

  @doc """
  Executes an sql script. Multiple stanzas can be passed at once.
  """
  @spec execute(conn, iodata) :: :ok | {:error, Error.t()}
  def execute(conn, sql), do: wrap_error(Nif.execute(conn, sql))

  @doc """
  Get the number of changes recently.

  **Note**: If triggers are used, the count may be larger than expected.

  See: https://sqlite.org/c3ref/changes.html
  """
  @spec changes(conn) :: {:ok, non_neg_integer} | {:error, Error.t()}
  def changes(conn), do: wrap_error(Nif.changes(conn))

  @spec prepare(conn, String.t()) :: {:ok, stmt} | {:error, Error.t()}
  def prepare(conn, sql), do: wrap_error(Nif.prepare(conn, sql))

  @spec bind(conn, stmt, [arg]) :: :ok | {:error, Error.t()}
        when arg: atom | binary | number | {:blob, binary}
  def bind(conn, stmt, args), do: wrap_error(Nif.bind(conn, stmt, args))

  @spec columns(conn, stmt) :: {:ok, [String.t()]} | {:error, Error.t()}
  def columns(conn, stmt), do: wrap_error(Nif.columns(conn, stmt))

  @spec step(conn, stmt) :: {:row, row} | :done | {:error, Error.t()}
        when row: list(binary | number | nil)
  def step(conn, stmt), do: wrap_error(Nif.step(conn, stmt))

  @spec multi_step(conn, stmt, pos_integer) ::
          {:rows, [row]} | {:done, [row]} | {:error, Error.t()}
        when row: list(binary | number | nil)
  def multi_step(conn, stmt, max_rows) do
    case Nif.multi_step(conn, stmt, max_rows) do
      {:rows, rows} -> {:rows, :lists.reverse(rows)}
      {:done, rows} -> {:done, :lists.reverse(rows)}
      error -> wrap_error(error)
    end
  end

  @spec last_insert_rowid(conn) :: {:ok, integer} | {:error, Error.t()}
  def last_insert_rowid(conn), do: wrap_error(Nif.last_insert_rowid(conn))

  @spec transaction_status(conn) :: {:ok, :idle | :transaction} | {:error, Error.t()}
  def transaction_status(conn), do: wrap_error(Nif.transaction_status(conn))

  @spec fetch_all(conn, stmt, pos_integer) :: {:ok, [row]} | {:error, Error.t()}
        when row: list(binary | number | nil)
  def fetch_all(conn, stmt, max_rows \\ 50) when is_reference(stmt) do
    {:ok, try_fetch_all(conn, stmt, max_rows)}
  catch
    :throw, error -> error
  end

  defp try_fetch_all(conn, stmt, max_rows) do
    case multi_step(conn, stmt, max_rows) do
      {:done, rows} -> rows
      {:rows, rows} -> rows ++ try_fetch_all(conn, stmt, max_rows)
      error -> throw(error)
    end
  end

  @spec prepare_fetch_all(conn, iodata, [arg], pos_integer) ::
          {:ok, [row]} | {:error, Error.t()}
        when arg: atom | binary | number | {:blob, binary},
             row: list(binary | number | nil)
  def prepare_fetch_all(conn, sql, args \\ [], max_rows \\ 50) do
    with {:ok, stmt} <- prepare(conn, sql) do
      try do
        with :ok <- bind(conn, stmt, args) do
          fetch_all(conn, stmt, max_rows)
        end
      after
        :ok = release(stmt)
      end
    end
  end

  # TODO handle segfault when rows are not list of lists
  # TODO max_rows
  @spec multi_bind_step(conn, stmt, [[arg]]) :: :ok | {:error, Error.t()}
        when arg: atom | binary | number | {:blob, binary}
  defp multi_bind_step(conn, stmt, rows) do
    case Nif.multi_bind_step(conn, stmt, rows) do
      :ok = ok -> ok
      {:ok, rows_left} -> multi_bind_step(conn, stmt, rows_left)
      error -> wrap_error(error)
    end
  end

  @spec insert_all(conn, stmt, [[arg]]) :: :ok | {:error, Error.t()}
        when arg: atom | binary | number | {:blob, binary}
  def insert_all(conn, stmt, rows) do
    with :ok <- execute(conn, "begin immediate") do
      try do
        with :ok <- multi_bind_step(conn, stmt, rows) do
          execute(conn, "commit")
        end
      catch
        class, reason ->
          :ok = execute(conn, "rollback")
          :erlang.raise(class, reason, __STACKTRACE__)
      end
    end
  end

  def prepare_insert_all(conn, sql, rows) do
    with {:ok, stmt} <- prepare(conn, sql) do
      try do
        insert_all(conn, stmt, rows)
      after
        :ok = release(stmt)
      end
    end
  end

  @doc """
  Serialize the contents of the database to a binary.
  """
  @spec serialize(conn, String.t()) :: {:ok, binary} | {:error, Error.t()}
  def serialize(conn, database \\ "main") do
    wrap_error(Nif.serialize(conn, to_charlist(database)))
  end

  @doc """
  Disconnect from database and then reopen as an in-memory database based on
  the serialized binary.
  """
  @spec deserialize(conn, String.t(), binary) :: :ok | {:error, Error.t()}
  def deserialize(conn, database \\ "main", serialized) do
    wrap_error(Nif.deserialize(conn, to_charlist(database), serialized))
  end

  @doc """
  Once finished with the prepared statement, call this to release the underlying
  resources.

  This should be called whenever you are done operating with the prepared statement. If
  the system has a high load the garbage collector may not clean up the prepared
  statements in a timely manner and causing higher than normal levels of memory
  pressure.

  If you are operating on limited memory capacity systems, definitely call this.
  """
  @spec release(stmt) :: :ok | {:error, Error.t()}
  def release(stmt), do: wrap_error(Nif.release(stmt))

  @doc """
  Allow loading native extensions.
  """
  @spec enable_load_extension(conn) :: :ok | {:error, Error.t()}
  def enable_load_extension(conn), do: wrap_error(Nif.enable_load_extension(conn, 1))

  @doc """
  Forbid loading native extensions.
  """
  @spec disable_load_extension(conn) :: :ok | {:error, Error.t()}
  def disable_load_extension(conn), do: wrap_error(Nif.enable_load_extension(conn, 0))

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
  @spec set_update_hook(conn, pid) :: :ok | {:error, Error.t()}
  def set_update_hook(conn, pid), do: wrap_error(Nif.set_update_hook(conn, pid))

  # TODO sql / statement
  defp wrap_error({:error, code, codename, message}) do
    {:error, Error.exception(code: code, codename: codename, message: message)}
  end

  # TODO
  defp wrap_error({:error, reason}) do
    {:error, Error.exception(message: reason)}
  end

  defp wrap_error(success), do: success
end
