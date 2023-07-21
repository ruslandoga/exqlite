tmp = Path.join([System.tmp_dir!(), "reads.db"])
if File.exists?(tmp), do: File.rm!(tmp)

drinks_count = 5_000_000
pool_sizes = [1, 2, 4, 8, 16]

pools =
  for pool_size <- pool_sizes do
    {:ok, conn} = Exqlite.start_link(database: tmp, pool_size: 1)
    {pool_size, conn}
  end

{:ok, db} = Exqlite.Sqlite3.open(tmp)

try do
  :ok = Exqlite.Sqlite3.execute(db, "create table drinks(name text) strict")

  IO.puts("making #{drinks_count} drinks")

  :ok =
    Exqlite.Sqlite3.execute(db, """
    with recursive generate_drinks(name, level) as (
      select 'drink1' as name, 1 as level
      union all
      select 'drink' || (level + 1), level + 1 from generate_drinks where level < #{drinks_count}
    )
    insert into drinks(name) select name from generate_drinks;
    """)
after
  :ok = Exqlite.Sqlite3.close(db)
end

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
  for concurrency <- concurrency, {size, pool} <- pools, into: %{} do
    {"query max_concurrency=#{concurrency} pool_size=#{size}",
     fn query ->
       Task.Supervisor.async_stream_nolink(
         sup,
         1..1000,
         fn _ -> Exqlite.query(pool, query) end,
         ordered: false,
         max_concurrency: concurrency,
         timeout: :timer.seconds(30)
       )
       |> Stream.run()
     end}
  end

Benchee.run(
  cases,
  inputs: Map.new(queries, fn q -> {q, q} end)
)

IO.puts("\n\n# io benchmarks\n\n")

queries = [
  "select * from drinks where rowid >= ? and rowid < ?"
]

cases =
  for concurrency <- concurrency, {size, pool} <- pools, into: %{} do
    {"read_query max_concurrency=#{concurrency} pool_size=#{size}",
     fn query ->
       Task.Supervisor.async_stream_nolink(
         sup,
         1..floor(drinks_count / 50),
         fn i ->
           Exqlite.query(pool, query, [(i - 1) * 50, i * 50])
         end,
         ordered: false,
         max_concurrency: concurrency,
         timeout: :timer.seconds(30)
       )
       |> Stream.run()
     end}
  end

Benchee.run(
  cases,
  inputs: Map.new(queries, fn q -> {q, q} end)
)
