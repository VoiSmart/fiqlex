defmodule FIQLEx.Test.Support.User do
  @moduledoc false
  use Ecto.Schema

  alias FIQLEx.Test.Support.Domain, as: DomainSchema

  alias FIQLEx.Test.Support.GroupsUsers, as: GroupUserSchema

  schema "users" do
    field(:username, :string)
    field(:firstname, :string)
    field(:lastname, :string)
    field(:middlename, :string)
    field(:sessionexpire, :integer, default: 3600)
    field(:enabled, :boolean)
    belongs_to(:domain, DomainSchema)
    timestamps()

    has_many(:groups_users, GroupUserSchema,
      foreign_key: :user_id,
      on_replace: :delete
    )

    has_many(:groups, through: [:groups_users, :group])
  end
end
