defmodule FIQLEx.Test.Support.Domain do
  @moduledoc false
  use Ecto.Schema

  schema "domain" do
    field(:realm, :string)
    field(:organization, :string)

    timestamps()
  end
end
