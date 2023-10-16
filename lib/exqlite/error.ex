defmodule Exqlite.Error do
  @moduledoc """
  The error emitted from SQLite or a general error with the library.
  """

  defexception [:code, :codename, :message, :statement]

  @type t :: %__MODULE__{
          code: integer | nil,
          codename: String.t() | nil,
          message: String.t() | nil,
          statement: String.t() | nil
        }

  @impl true
  def message(%{codename: codename, message: message, statement: statement}) do
    [
      if(codename, do: to_string(codename)),
      if(message, do: if(is_binary(message), do: message, else: inspect(message))),
      if(statement, do: to_string(statement))
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(": ")
  end
end
