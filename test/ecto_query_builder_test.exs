defmodule EctoQueryBuilderTest do
  @moduledoc false
  use ExUnit.Case

  import Ecto.Query

  alias FIQLEx.QueryBuilders.EctoQueryBuilder
  alias FIQLEx.Test.Support.Group, as: GroupSchema
  alias FIQLEx.Test.Support.User, as: UserSchema

  test "fiql filter with invalid associations" do
    res =
      FIQLEx.build_query(FIQLEx.parse!("shops.name==develop"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    assert res == {:error, :selector_not_allowed}
  end

  test "fiql filter with invalid association's field" do
    res =
      FIQLEx.build_query(FIQLEx.parse!("groups.invalidfield==develop"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    assert res == {:error, :selector_not_allowed}
  end

  test "fiql filter with associations and binary equal filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name==develop;firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).name == ^"develop",
              select: u0.id
            )
          ) and u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with multiple associations and binary equal filter" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("groups.name==develop;domain.organization==acme;firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).name == ^"develop",
              select: u0.id
            )
          ) and
            (u0.id in subquery(
               from(u0 in FIQLEx.Test.Support.User,
                 join: g1 in assoc(u0, :domain),
                 as: :domain,
                 where: as(:domain).organization == ^"acme",
                 select: u0.id
               )
             ) and
               u0.firstname == ^"John"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and binary equal filter and case insensitive" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name==develop;firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        case_sensitive: false,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: fragment("lower(?)", as(:groups).name) == fragment("lower(?)", ^"develop"),
              select: u0.id
            )
          ) and
            fragment("lower(?)", u0.firstname) == fragment("lower(?)", ^"John"),
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and binary_like filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name==*develop;firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: like(as(:groups).name, ^"%develop"),
              select: u0.id
            )
          ) and u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and binary like filter and case insensitive" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name==*develop"), EctoQueryBuilder,
        schema: UserSchema,
        case_sensitive: false,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: ilike(as(:groups).name, ^"%develop"),
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and not equal filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name!=develop"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).name != ^"develop",
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and not equal filter and case insensitive" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name!=develop"), EctoQueryBuilder,
        schema: UserSchema,
        case_sensitive: false,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: fragment("lower(?)", as(:groups).name) != fragment("lower(?)", ^"develop"),
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and binary not like filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name!=*develop;firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: not like(as(:groups).name, ^"%develop"),
              select: u0.id
            )
          ) and u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and binary not like filter and case_insensitive" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.name!=*develop"), EctoQueryBuilder,
        schema: UserSchema,
        case_sensitive: false,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: not ilike(as(:groups).name, ^"%develop"),
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and list filter" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("groups.name==(develop, research);firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).name in ^["develop", "research"],
              select: u0.id
            )
          ) and u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and not in list filter" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("groups.name!=(develop, research);firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).name not in ^["develop", "research"],
              select: u0.id
            )
          ) and u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and true boolean filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.enabled==true"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).enabled == ^"true",
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and not true boolean filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.enabled!=true"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).enabled != ^"true",
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and false boolean filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.enabled==false"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).enabled == ^"false",
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and not false boolean filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.enabled!=false"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).enabled != ^"false",
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations and isnull filter" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("groups.description=isnull=false"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: is_nil(as(:groups).description) == ^false,
              select: u0.id
            )
          ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with associations comparing dates with gt and lt" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!(
          "groups.inserted_at=gt=2022-10-02T18:23:03Z;groups.inserted_at=lt=2022-10-31T18:23:03Z"
        ),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).inserted_at > fragment("?::date", ^"2022-10-02T18:23:03Z"),
              select: u0.id
            )
          ) and
            u0.id in subquery(
              from(u0 in FIQLEx.Test.Support.User,
                join: g1 in assoc(u0, :groups),
                as: :groups,
                where: as(:groups).inserted_at < fragment("?::date", ^"2022-10-31T18:23:03Z"),
                select: u0.id
              )
            ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter associations comparing dates with ge and le" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!(
          "groups.inserted_at=ge=2022-10-02T18:23:03Z;groups.inserted_at=le=2022-10-31T18:23:03Z"
        ),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).inserted_at >= fragment("?::date", ^"2022-10-02T18:23:03Z"),
              select: u0.id
            )
          ) and
            u0.id in subquery(
              from(u0 in FIQLEx.Test.Support.User,
                join: g1 in assoc(u0, :groups),
                as: :groups,
                where: as(:groups).inserted_at <= fragment("?::date", ^"2022-10-31T18:23:03Z"),
                select: u0.id
              )
            ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter associations comparing integer numbers with ge and le operator" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("groups.sessionexpire=ge=25,groups.sessionexpire=le=18"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).sessionexpire >= ^25,
              select: u0.id
            )
          ) or
            u0.id in subquery(
              from(u0 in FIQLEx.Test.Support.User,
                join: g1 in assoc(u0, :groups),
                as: :groups,
                where: as(:groups).sessionexpire <= ^18,
                select: u0.id
              )
            ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter associations comparing integer numbers with gt and lt operator" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("groups.sessionexpire=gt=25,groups.sessionexpire=lt=18"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where:
          u0.id in subquery(
            from(u0 in FIQLEx.Test.Support.User,
              join: g1 in assoc(u0, :groups),
              as: :groups,
              where: as(:groups).sessionexpire > ^25,
              select: u0.id
            )
          ) or
            u0.id in subquery(
              from(u0 in FIQLEx.Test.Support.User,
                join: g1 in assoc(u0, :groups),
                as: :groups,
                where: as(:groups).sessionexpire < ^18,
                select: u0.id
              )
            ),
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with select field from selectors" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with an unicode value" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==さよなら"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"さよなら",
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

    expected = from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"John", order_by: [])

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
        where: u0.firstname == ^"John",
        order_by: [],
        select: [:firstname, :username]
      )

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with select some binary fields" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: ["firstname", "username"]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John",
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
        where: u0.sessionexpire > ^25 or u0.sessionexpire < ^18,
        order_by: [],
        select: [:sessionexpire]
      )

    assert inspect(result) == inspect(expected)
  end

  test "fiql filter comparing binary with gt operator" do
    res =
      FIQLEx.build_query(
        FIQLEx.parse!("sessionexpire=gt=abc"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    assert res == {:error, :invalid_comparison_value}
  end

  test "fiql filter comparing binary with lt operator" do
    res =
      FIQLEx.build_query(
        FIQLEx.parse!("sessionexpire=lt=abc"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    assert res == {:error, :invalid_comparison_value}
  end

  test "fiql filter using isnull operator" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("middlename=isnull=true"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: is_nil(u0.middlename) == ^true,
        order_by: [],
        select: [:middlename]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter using isnull operator with non-boolean value" do
    res =
      FIQLEx.build_query(
        FIQLEx.parse!("middlename=isnull=foo"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    assert res == {:error, :invalid_value}
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
        where: u0.sessionexpire in ^[13, 18],
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
        where: u0.sessionexpire >= ^25 or u0.sessionexpire <= ^18,
        order_by: [],
        select: [:sessionexpire]
      )

    assert inspect(expected) == inspect(result)
  end

  test "invalid comparison fiql filter " do
    {:error, :invalid_comparison_value} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname=ge=John"),
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
          u0.inserted_at > fragment("?::date", ^"2022-10-02T18:23:03Z") and
            u0.inserted_at < fragment("?::date", ^"2022-10-31T18:23:03Z"),
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
          u0.inserted_at >= fragment("?::date", ^"2022-10-02T18:23:03Z") and
            u0.inserted_at <= fragment("?::date", ^"2022-10-31T18:23:03Z"),
        order_by: [],
        select: [:inserted_at]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing true boolean field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("enabled==true"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.enabled == ^"true",
        order_by: [],
        select: [:enabled]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing false boolean field" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("enabled==false"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.enabled == ^"false",
        order_by: [],
        select: [:enabled]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter comparing not true boolean field" do
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

  test "fiql filter comparing not false boolean field" do
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
        where: u0.firstname not in ^[1, 2, "Hello"],
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
        where: u0.firstname != ^"Hello 'World",
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
        where: not like(u0.firstname, ^"%Hello"),
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
        where: like(u0.firstname, ^"Hello%"),
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
          u0.firstname == ^"Hello" and
            (u0.sessionexpire >= ^10 and u0.username == ^"Malcom"),
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
        where: u0.firstname == ^"Hello" or (u0.sessionexpire >= ^10 or u0.username == ^"Malcom"),
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
          u0.firstname == ^"Hello" or
            (u0.sessionexpire >= ^10 or (u0.username == ^"Malcom" or not is_nil(u0.lastname))),
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
          (u0.firstname == ^"Hello" and
             (u0.sessionexpire >= ^10 and u0.username == ^"Malcom")) or
            not is_nil(u0.lastname),
        order_by: [],
        select: [:firstname, :sessionexpire, :username, :lastname]
      )

    assert(inspect(expected) == inspect(result))
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

  test "fiql filter with selector not in ecto schema" do
    {:error, :selector_not_allowed} =
      FIQLEx.build_query(
        FIQLEx.parse!("name=ge=John"),
        EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors
      )
  end

  test "fiql filter with only valid selector atom field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John,username==user"),
        EctoQueryBuilder,
        schema: UserSchema,
        only: [:firstname, :username]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John" or u0.username == ^"user",
        order_by: []
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with only valid selector field" do
    {:ok, result} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        only: ["firstname"]
      )

    expected = from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"John", order_by: [])

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

  test "fiql filter with except valid selector atom field" do
    {:error, :selector_not_allowed} =
      FIQLEx.build_query(
        FIQLEx.parse!("firstname==John"),
        EctoQueryBuilder,
        schema: UserSchema,
        except: [:firstname]
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

    expected = from(u0 in FIQLEx.Test.Support.User, where: u0.firstname == ^"John", order_by: [])

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
        where: fragment("lower(?)", u0.firstname) == fragment("lower(?)", ^"John"),
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
        where: fragment("lower(?)", u0.firstname) != fragment("lower(?)", ^"John"),
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
        where: not ilike(u0.firstname, ^"%Hello"),
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
        where: ilike(u0.firstname, ^"%Hello"),
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
        where: u0.firstname == ^"John",
        order_by: [asc: u0.firstname],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with order by sorting only with valid selector" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        order_by: [{:asc, :firstname}, {:asc, :invalid_field}]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John",
        order_by: [asc: u0.firstname],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter with order by sorting only with atom and binary selectors" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        order_by: [{:asc, :firstname}, {:desc, "lastname"}]
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John",
        order_by: [asc: u0.firstname, desc: u0.lastname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "single fiql filter with limit" do
    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        limit: 103
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.firstname == ^"John",
        order_by: [],
        limit: ^103,
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "give inital query" do
    initial_query =
      from(UserSchema,
        join: g in GroupSchema,
        on: true
      )

    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        schema: UserSchema,
        select: :from_selectors,
        initial_query: initial_query
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        join: g1 in FIQLEx.Test.Support.Group,
        on: true,
        where: u0.firstname == ^"John",
        order_by: [],
        select: [:firstname]
      )

    assert inspect(expected) == inspect(result)
  end

  test "fiql filter using a trasnformer function" do
    transformer_fn = fn _selector, value -> {"lastname", "Another#{value}"} end

    {:ok, result} =
      FIQLEx.build_query(FIQLEx.parse!("lastname==John"), EctoQueryBuilder,
        schema: UserSchema,
        transformer: transformer_fn
      )

    expected =
      from(u0 in FIQLEx.Test.Support.User,
        where: u0.lastname == ^"AnotherJohn",
        order_by: []
      )

    assert(inspect(expected) == inspect(result))
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

  test "build ecto query without a schema give error" do
    {:error, :selector_not_allowed} =
      FIQLEx.build_query(FIQLEx.parse!("firstname==John"), EctoQueryBuilder,
        select: :from_selectors,
        order_by: [{:asc, :firstname}]
      )
  end
end
