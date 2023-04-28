defmodule FIQLEx.Test.Support.Group do
  @moduledoc false
  use Ecto.Schema

  alias FIQLEx.Test.Support.GroupsUsers, as: GroupUserSchema

  schema "groups" do
    field(:name, :string)
    field(:description, :string)
    field(:enabled, :boolean)
    field(:sessionexpire, :integer, default: 3600)

    has_many(:groups_users, GroupUserSchema,
      foreign_key: :group_id,
      on_replace: :delete
    )

    has_many(:users, through: [:groups_users, :user])
    timestamps()
  end
end
