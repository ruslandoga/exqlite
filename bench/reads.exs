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

{:ok, sup} = Task.Supervisor.start_link()
concurrency = [1, 2, 4, 8, 16, 250]

IO.puts("\n\n# cpu benchmarks\n\n")

queries = [
  "select 1",
  # "select * from drinks",
  # "select * from drinks limit 1000",
  "select count(*) from drinks"
]

cases =
  Map.new(concurrency, fn concurrency ->
    {"read_query max_concurrency=#{concurrency}",
     fn query ->
       Task.Supervisor.async_stream_nolink(
         sup,
         1..1000,
         fn _ -> Exqlite.RWConnection.read_query(conn, query) end,
         ordered: false,
         max_concurrency: concurrency,
         timeout: :timer.seconds(30)
       )
       |> Stream.run()
     end}
  end)

Benchee.run(
  cases,
  inputs: Map.new(queries, fn q -> {q, q} end)
)

IO.puts("\n\n# io benchmarks\n\n")

queries = [
  "select * from drinks where rowid >= ? and rowid < ?"
]

cases =
  Map.new(concurrency, fn concurrency ->
    {"read_query max_concurrency=#{concurrency}",
     fn query ->
       Task.Supervisor.async_stream_nolink(
         sup,
         1..floor(drinks_count / 50),
         fn i ->
           Exqlite.RWConnection.read_query(conn, query, [(i - 1) * 50, i * 50])
         end,
         ordered: false,
         max_concurrency: concurrency,
         timeout: :timer.seconds(30)
       )
       |> Stream.run()
     end}
  end)

Benchee.run(
  cases,
  inputs: Map.new(queries, fn q -> {q, q} end)
)
