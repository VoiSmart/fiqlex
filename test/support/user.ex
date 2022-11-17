defmodule FIQLEx.Test.Support.User do
  @moduledoc false
  use Ecto.Schema

  schema "users" do
    field(:username, :string)
    field(:firstname, :string)
    field(:lastname, :string)
    field(:middlename, :string)
    field(:sessionexpire, :integer, default: 3600)
    field(:enabled, :boolean)
    timestamps()
  end
end
