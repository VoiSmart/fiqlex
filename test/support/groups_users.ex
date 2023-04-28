defmodule FIQLEx.Test.Support.GroupsUsers do
  @moduledoc false
  use Ecto.Schema

  alias FIQLEx.Test.Support.{Group, User}

  schema "groups_users" do
    belongs_to(:group, Group)
    belongs_to(:user, User)
    timestamps()
  end
end
