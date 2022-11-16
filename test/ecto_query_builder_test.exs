defmodule EctoQueryBuilderTest do
  @moduledoc false
  use ExUnit.Case

  import Ecto.Query

  alias FIQLEx.QueryBuilders.EctoQueryBuilder
  alias FIQLEx.Test.Support.User, as: UserSchema

  test "single fiql filter with select field from selectors" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"'John'",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with select all fields" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :all
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"'John'", order_by: [])

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with select some fields" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: [:firstname, :username]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"'John'",
        order_by: [],
        select: [:firstname, :username]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing integer numbers with gt and lt operator" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("sessionexpire=gt=25,sessionexpire=lt=18"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.sessionexpire > ^"25" or u0.sessionexpire < ^"18",
        order_by: [],
        select: [:sessionexpire]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter with a list of value" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("sessionexpire==(13,18)"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.sessionexpire in ^["13", "18"],
        order_by: [],
        select: [:sessionexpire]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing integer numbers with ge and le operator" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("sessionexpire=ge=25,sessionexpire=le=18"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.sessionexpire >= ^"25" or u0.sessionexpire <= ^"18",
        order_by: [],
        select: [:sessionexpire]
      )

    assert inspect(expected) == inspect(result)
  end

  test "invalid comparison fiql filter " do
    {:error, :invalid_comparison_value} =
      FIQLEx.build_query(
        FIQLEx.parse!("name=ge=John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )
  end

  test "fiql filter comparing duration" do
    {:ok, _expected} =
      FIQLEx.build_query(
        FIQLEx.parse!("inserted_at=le=P5Y;inserted_at=le=P5Y"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )
  end

  test "fiql filter comparing with negative duration" do
    {:ok, _expected} =
      FIQLEx.build_query(
        FIQLEx.parse!("inserted_at=le=-P5Y;inserted_at=le=P5Y"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )
  end

  test "fiql filter comparing with invalid duration" do
    {:error, _error} =
      FIQLEx.build_query(
        FIQLEx.parse!("inserted_at=le=-P5K;inserted_at=le=P5Y"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )
  end

  test "fiql filter comparing dates with gt and lt" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("inserted_at=gt=2022-10-02T18:23:03Z;inserted_at=lt=2022-10-31T18:23:03Z"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.inserted_at > fragment("?::date", ^"'2022-10-02T18:23:03Z'") and
            u0.inserted_at < fragment("?::date", ^"'2022-10-31T18:23:03Z'"),
        order_by: [],
        select: [:inserted_at]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing dates with ge and le" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("inserted_at=ge=2022-10-02T18:23:03Z;inserted_at=le=2022-10-31T18:23:03Z"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.inserted_at >= fragment("?::date", ^"'2022-10-02T18:23:03Z'") and
            u0.inserted_at <= fragment("?::date", ^"'2022-10-31T18:23:03Z'"),
        order_by: [],
        select: [:inserted_at]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing true boolean field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("enabled!=true"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.enabled != ^"true",
        order_by: [],
        select: [:enabled]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing false boolean field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("enabled!=false"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.enabled != ^"false",
        order_by: [],
        select: [:enabled]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter for not null field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: not is_nil(u0.firstname),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter for not in field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname!=(1,2,Hello)"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname not in ^["1", "2", "'Hello'"],
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter for not equal field with multiple strings" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname!='Hello \\'World'"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname != ^"'Hello ''World'",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter comparing not like field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname!=*Hello"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: not like(u0.firstname, ^"'%Hello'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing like field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==Hello*"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: like(u0.firstname, ^"'Hello%'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter with multiple fields in and" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==Hello;sessionexpire=ge=10;username==Malcom"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.firstname == ^"'Hello'" and
            (u0.sessionexpire >= ^"10" and u0.username == ^"'Malcom'"),
        order_by: [],
        select: [:firstname, :sessionexpire, :username]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with multiple fields in or" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==Hello,sessionexpire=ge=10,username==Malcom"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.firstname == ^"'Hello'" or (u0.sessionexpire >= ^"10" or u0.username == ^"'Malcom'"),
        order_by: [],
        select: [:firstname, :sessionexpire, :username]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter with multiple fields in logic or with a not null field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==Hello,sessionexpire=ge=10,username==Malcom,lastname"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.firstname == ^"'Hello'" or
            (u0.sessionexpire >= ^"10" or (u0.username == ^"'Malcom'" or not is_nil(u0.lastname))),
        order_by: [],
        select: [:firstname, :sessionexpire, :username, :lastname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with multiple fields in logic and with a not null field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==Hello;sessionexpire=ge=10;username==Malcom,lastname"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.firstname == ^"'Hello'" and
            (u0.sessionexpire >= ^"10" and (u0.username == ^"'Malcom'" or not is_nil(u0.lastname))),
        order_by: [],
        select: [:firstname, :sessionexpire, :username, :lastname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with only invalid selector field" do
    {:error, :selector_not_allowed} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        only: ["bad"]
      )
  end

  test "fiql filter with only valid selector field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        only: ["firstname"]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"'John'", order_by: [])

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with except valid selector field" do
    {:error, :selector_not_allowed} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        except: ["firstname"]
      )
  end

  test "fiql filter with except invalid selector field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        except: ["bad"]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"'John'", order_by: [])

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with insensitive selector and equal comparison" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        case_sensitive: false
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: fragment("lower(?)", u0.firstname) == fragment("lower(?)", ^"'John'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with insensitive selector and not equal comparison" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname!=John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        case_sensitive: false
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: fragment("lower(?)", u0.firstname) != fragment("lower(?)", ^"'John'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter with insensitive selector and not like comparison" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname!=*Hello"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        case_sensitive: false
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: not ilike(u0.firstname, ^"'%Hello'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter with insensitive selector and like comparison" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==*Hello"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        case_sensitive: false
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: ilike(u0.firstname, ^"'%Hello'"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(result) == inspect(expected)
  end

  test "single fiql filter with order by sorting" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        order_by: [{:asc, :firstname}]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"'John'",
        order_by: [asc: u0.firstname],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with invalid comparison operator" do
    {:error, _error} =
      FIQLEx.build_query(FIQLEx.parse!("firstname=gk=John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        order_by: [{:asc, :firstname}]
      )
  end

  test "fiql filter with invalid comparison operator with a number value" do
    {:error, _error} =
      FIQLEx.build_query(FIQLEx.parse!("firstname=gk=2"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        order_by: [{:asc, :firstname}]
      )
  end
end
