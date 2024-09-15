defmodule Exqlite.Error do
  @moduledoc """
  The error emitted from SQLite.
  """

  defexception [:code, :reason, :message, :statement]

  @type t :: %__MODULE__{
          code: integer,
          reason: atom,
          message: String.t(),
          statement: String.t()
        }

  @impl true
  def message(%__MODULE__{message: message, statement: nil}), do: message

  def message(%__MODULE__{message: message, statement: statement}),
    do: "#{message}\n#{statement}"
end
