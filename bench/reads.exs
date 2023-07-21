tmp = Path.join([System.tmp_dir!(), "reads.db"])
if File.exists?(tmp), do: File.rm!(tmp)
{:ok, conn} = Exqlite.RWConnection.start_link(database: tmp)

{:ok, _result} =
  Exqlite.RWConnection.query(conn, "create table drinks(name text) strict")

drinks_count = 5_000_000
IO.puts("making #{drinks_count} drinks")

{:ok, _result} =
  Exqlite.RWConnection.query(conn, """
  with recursive generate_drinks(name, level) as (
    select 'drink1' as name, 1 as level
    union all
    select 'drink' || (level + 1), level + 1 from generate_drinks where level < #{drinks_count}
  )
  insert into drinks(name) select name from generate_drinks;
  """)

queries = [
  # "select 1",
  # "select * from drinks",
  # "select * from drinks limit 1000",
  # "select count(*) from drinks",
  "select * from drinks where rowid >= ? and rowid < ?"
]

{:ok, sup} = Task.Supervisor.start_link()

Benchee.run(
  %{
    "read_query async_stream=1" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 1
      )
      |> Stream.run()
    end,
    "read_query async_stream=2" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 2
      )
      |> Stream.run()
    end,
    "read_query async_stream=4" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 4
      )
      |> Stream.run()
    end,
    "read_query async_stream=8" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 8
      )
      |> Stream.run()
    end,
    "read_query async_stream=16" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 16
      )
      |> Stream.run()
    end,
    "read_query async_stream=250" => fn query ->
      Task.Supervisor.async_stream_nolink(
        sup,
        1..floor(drinks_count / 50),
        fn i ->
          Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
        end,
        ordered: false,
        max_concurrency: 250,
        timeout: :infinity
      )
      |> Stream.run()
    end
  },
  inputs: Map.new(queries, fn q -> {q, q} end)
)
