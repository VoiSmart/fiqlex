defmodule FIQLEx.QueryBuilders.EctoQueryBuilder do
  @moduledoc ~S"""
  Builds Ecto queries from FIQL AST.

  Possible options for this query builder are:

  * `schema`: The schema name to use in the `FROM` statement
  * `initial_query`: An optional Ecto query used as a starting point for building the query. If not given `schema` is used.
  * `select`: `SELECT` statement to build (_see below_).
  * `only`: A list of atom/binary with the only fields to accept in the query (if `only` and `except` are both provided, `only` is used)
  * `except`: A list of atom/binary with the fields to reject in the query (if `only` and `except` are both provided, `only` is used)
  * `order_by`: A tuple list {direction, field} for order by to be added to the query. Direction is :asc or :desc, field is an atom/binary
  * `limit`: A limit for the query
  * `case_sensitive`: Boolean value (default to true) to set equals case sensitive or not
  * `transformer`: Function that takes a selector and its value as parameter and must return a tuple {new_selector, new_value} with the transformed values
  * `casting_assoc_fields`: Function that takes an association and a field as parameter and must return ecto type for that field


  ### Select option

  Possible values of the `select` option are:

  * `:all`: use `SELECT *` (default value)
  * `:from_selectors`: Searches for all selectors in the FIQL AST and use them as `SELECT` statement.
  For instance, for the following query: `age=ge=25;name==*Doe`, the `SELECT` statement will be `SELECT age, name`
  * `selectors`: You specify a list of atom/binary items you want to use in the `SELECT` statement.
  """
  use FIQLEx.QueryBuilder

  import Ecto.Query

  alias Timex.Parse.Duration.Parsers.ISO8601Parser

  @impl true
  def init(_ast, opts) do
    {schema_fields, schema_associations, schema_table} =
      case Keyword.get(opts, :schema) do
        nil ->
          {[], [], nil}

        schema ->
          schema_table = schema.__schema__(:source)
          schema_table = string_to_atom(schema_table)

          {Enum.map(schema.__schema__(:fields), fn field -> Atom.to_string(field) end),
           Enum.map(schema.__schema__(:associations), fn association ->
             Atom.to_string(association)
           end), schema_table}
      end

    new_opts =
      opts
      |> Keyword.put(:schema_fields, schema_fields)
      |> Keyword.put(:schema_associations, schema_associations)
      |> Keyword.put(:schema_table, schema_table)

    {"", new_opts}
  end

  @impl true
  def build(ast, {query, opts}) do
    schema = inital_query(opts)
    order_by = Keyword.get(opts, :order_by, [])

    select = get_select_option(ast, opts)

    limit = Keyword.get(opts, :limit)

    final_query =
      schema
      |> add_select(select)
      |> order_by(^add_order_by(order_by, opts))
      |> where(^query)
      |> add_limit(limit)

    {:ok, final_query}
  end

  @impl true
  def handle_or_expression(exp1, exp2, ast, {query, opts}) do
    with {:ok, {left, _opts}} <- handle_ast(exp1, ast, {query, opts}),
         {:ok, {right, _opts}} <- handle_ast(exp2, ast, {query, opts}) do
      {:ok, {dynamic([q], ^left or ^right), opts}}
    else
      {:error, err} -> {:error, err}
    end
  end

  @impl true
  def handle_and_expression(exp1, exp2, ast, {query, opts}) do
    with {:ok, {left, _opts}} <- handle_ast(exp1, ast, {query, opts}),
         {:ok, {right, _opts}} <- handle_ast(exp2, ast, {query, opts}) do
      {:ok, {dynamic([q], ^left and ^right), opts}}
    else
      {:error, err} -> {:error, err}
    end
  end

  @impl true
  def handle_expression(exp, ast, {query, opts}) do
    case handle_ast(exp, ast, {query, opts}) do
      {:ok, {constraint, _opts}} -> {:ok, {constraint, opts}}
      {:error, err} -> {:error, err}
    end
  end

  @impl true
  def handle_selector(selector_name, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], not is_nil(field(q, ^selector_name))), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  @impl true
  def handle_selector_and_value(selector_name, op, value, ast, {query, opts}) do
    {new_selector_name, new_value} =
      Keyword.get(opts, :transformer, &identity_transformer/2).(selector_name, value)

    do_handle_selector_and_value(new_selector_name, op, new_value, ast, {query, opts})
  end

  @impl true
  def handle_selector_and_value_with_comparison(selector_name, op, value, ast, {query, opts}) do
    {new_selector_name, new_value} =
      Keyword.get(opts, :transformer, &identity_transformer/2).(selector_name, value)

    do_handle_selector_and_value_with_comparison(
      new_selector_name,
      op,
      new_value,
      ast,
      {query, opts}
    )
  end

  defp binary_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        if is_case_insensitive(opts) and
             field_can_be_insensitive(opts, association, assoc_selector) do
          subquery_where =
            dynamic(
              [],
              fragment("lower(?)", field(as(^association), ^assoc_selector)) ==
                fragment(
                  "lower(?)",
                  type(^value, ^assoc_value_type(opts, association, assoc_selector))
                )
            )

          build_association_where(association, subquery_where, opts)
        else
          subquery_where =
            dynamic(
              [],
              field(as(^association), ^assoc_selector) ==
                type(^value, ^assoc_value_type(opts, association, assoc_selector))
            )

          build_association_where(association, subquery_where, opts)
        end

      false ->
        selector_name = string_to_atom(selector_name)
        schema_table = Keyword.get(opts, :schema_table)

        if is_case_insensitive(opts) and
             field_can_be_insensitive(opts, schema_table, selector_name) do
          dynamic(
            [q],
            fragment("lower(?)", field(q, ^selector_name)) == fragment("lower(?)", ^value)
          )
        else
          dynamic([q], field(q, ^selector_name) == ^value)
        end
    end
  end

  defp binary_like(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        if is_case_insensitive(opts) do
          value = String.replace(value, "*", "%", global: true)

          subquery_where =
            dynamic(
              [],
              ilike(field(as(^association), ^assoc_selector), ^value)
            )

          build_association_where(association, subquery_where, opts)
        else
          value = String.replace(value, "*", "%", global: true)

          subquery_where =
            dynamic(
              [],
              like(field(as(^association), ^assoc_selector), ^value)
            )

          build_association_where(association, subquery_where, opts)
        end

      false ->
        selector_name = string_to_atom(selector_name)

        if is_case_insensitive(opts) do
          value = String.replace(value, "*", "%", global: true)
          dynamic([q], ilike(field(q, ^selector_name), ^value))
        else
          value = String.replace(value, "*", "%", global: true)
          dynamic([q], like(field(q, ^selector_name), ^value))
        end
    end
  end

  defp binary_not_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        if is_case_insensitive(opts) and
             field_can_be_insensitive(opts, association, assoc_selector) do
          subquery_where =
            dynamic(
              [],
              fragment("lower(?)", field(as(^association), ^assoc_selector)) !=
                fragment(
                  "lower(?)",
                  type(^value, ^assoc_value_type(opts, association, assoc_selector))
                )
            )

          build_association_where(association, subquery_where, opts)
        else
          subquery_where =
            dynamic(
              [],
              field(as(^association), ^assoc_selector) !=
                type(^value, ^assoc_value_type(opts, association, assoc_selector))
            )

          build_association_where(association, subquery_where, opts)
        end

      false ->
        selector_name = string_to_atom(selector_name)

        schema_table = Keyword.get(opts, :schema_table)

        if is_case_insensitive(opts) and
             field_can_be_insensitive(opts, schema_table, selector_name) do
          dynamic(
            [q],
            fragment("lower(?)", field(q, ^selector_name)) != fragment("lower(?)", ^value)
          )
        else
          dynamic([q], field(q, ^selector_name) != ^value)
        end
    end
  end

  defp binary_not_like(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        if is_case_insensitive(opts) do
          value = String.replace(value, "*", "%", global: true)

          subquery_where =
            dynamic([], not ilike(field(as(^association), ^assoc_selector), ^value))

          build_association_where(association, subquery_where, opts)
        else
          value = String.replace(value, "*", "%", global: true)
          subquery_where = dynamic([], not like(field(as(^association), ^assoc_selector), ^value))
          build_association_where(association, subquery_where, opts)
        end

      false ->
        selector_name = string_to_atom(selector_name)

        if is_case_insensitive(opts) do
          value = String.replace(value, "*", "%", global: true)
          dynamic([q], not ilike(field(q, ^selector_name), ^value))
        else
          value = String.replace(value, "*", "%", global: true)
          dynamic([q], not like(field(q, ^selector_name), ^value))
        end
    end
  end

  defp list_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) in type(
              ^value,
              {:array, ^assoc_value_type(opts, association, assoc_selector)}
            )
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) in ^value)
    end
  end

  defp list_not_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) not in type(
              ^value,
              {:array, ^assoc_value_type(opts, association, assoc_selector)}
            )
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) not in ^value)
    end
  end

  defp boolean_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic([], field(as(^association), ^assoc_selector) == ^to_string(value))

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) == ^to_string(value))
    end
  end

  defp boolean_not_equal(selector_name, value, opts) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic([], field(as(^association), ^assoc_selector) != ^to_string(value))

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) != ^to_string(value))
    end
  end

  defp identity_transformer(selector, value), do: {selector, value}

  defp do_handle_selector_and_value(selector_name, :equal, value, _ast, {_query, opts})
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      if String.starts_with?(value, "*") || String.ends_with?(value, "*") do
        {:ok, {binary_like(selector_name, value, opts), opts}}
      else
        {:ok, {binary_equal(selector_name, value, opts), opts}}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, value, _ast, {_query, opts})
       when is_list(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {list_equal(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, true, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {boolean_equal(selector_name, true, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, false, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {boolean_equal(selector_name, false, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, value, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {binary_equal(selector_name, to_string(value), opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, value, _ast, {_query, opts})
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      if String.starts_with?(value, "*") || String.ends_with?(value, "*") do
        {:ok, {binary_not_like(selector_name, value, opts), opts}}
      else
        {:ok, {binary_not_equal(selector_name, value, opts), opts}}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, value, _ast, {_query, opts})
       when is_list(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {list_not_equal(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, true, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {boolean_not_equal(selector_name, true, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, false, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {boolean_not_equal(selector_name, false, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, value, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {binary_not_equal(selector_name, to_string(value), opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  def ge_filter(selector_name, value, opts) when is_number(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) >=
              type(^value, ^assoc_value_type(opts, association, assoc_selector))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) >= ^value)
    end
  end

  def ge_filter(selector_name, value, opts) when is_binary(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) >= fragment("?::date", ^to_string(value))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) >= fragment("?::date", ^to_string(value)))
    end
  end

  def gt_filter(selector_name, value, opts) when is_number(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) >
              type(^value, ^assoc_value_type(opts, association, assoc_selector))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) > ^value)
    end
  end

  def gt_filter(selector_name, value, opts) when is_binary(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) > fragment("?::date", ^to_string(value))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) > fragment("?::date", ^to_string(value)))
    end
  end

  def le_filter(selector_name, value, opts) when is_number(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) <=
              type(^value, ^assoc_value_type(opts, association, assoc_selector))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) <= ^value)
    end
  end

  def le_filter(selector_name, value, opts) when is_binary(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) <= fragment("?::date", ^to_string(value))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) <= fragment("?::date", ^to_string(value)))
    end
  end

  def lt_filter(selector_name, value, opts) when is_number(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) <
              type(^value, ^assoc_value_type(opts, association, assoc_selector))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) < ^value)
    end
  end

  def lt_filter(selector_name, value, opts) when is_binary(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            field(as(^association), ^assoc_selector) < fragment("?::date", ^to_string(value))
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], field(q, ^selector_name) < fragment("?::date", ^to_string(value)))
    end
  end

  def isnull_filter(selector_name, value, opts) when is_boolean(value) do
    case maybe_associations_selector?(selector_name) do
      true ->
        {association, assoc_selector} = get_association_selector_to_atom(selector_name)

        subquery_where =
          dynamic(
            [],
            is_nil(field(as(^association), ^assoc_selector)) == ^value
          )

        build_association_where(association, subquery_where, opts)

      false ->
        selector_name = string_to_atom(selector_name)

        dynamic([q], is_nil(field(q, ^selector_name)) == ^value)
    end
  end

  defp build_association_where(association, subquery_where, opts) do
    schema = Keyword.get(opts, :schema)

    [primary_key | _] = schema.__schema__(:primary_key)

    dynamic(
      [qu],
      field(qu, ^primary_key) in subquery(
        from(sc in schema)
        |> join(:inner, [sc], re in assoc(sc, ^association), as: ^association)
        |> where(^subquery_where)
        |> select([sc], field(sc, ^primary_key))
      )
    )
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "ge",
         value,
         _ast,
         {_query, opts}
       )
       when is_number(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {ge_filter(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "gt",
         value,
         _ast,
         {_query, opts}
       )
       when is_number(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {gt_filter(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "le",
         value,
         _ast,
         {_query, opts}
       )
       when is_number(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {le_filter(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "lt",
         value,
         _ast,
         {_query, opts}
       )
       when is_number(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {lt_filter(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "ge",
         value,
         _ast,
         {_query, opts}
       )
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok, {ge_filter(selector_name, date, opts), opts}}

        {:error, err} ->
          {:error, err}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "gt",
         value,
         _ast,
         {_query, opts}
       )
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok, {gt_filter(selector_name, date, opts), opts}}

        {:error, err} ->
          {:error, err}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "le",
         value,
         _ast,
         {_query, opts}
       )
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok, {le_filter(selector_name, date, opts), opts}}

        {:error, err} ->
          {:error, err}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "lt",
         value,
         _ast,
         {_query, opts}
       )
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok, {lt_filter(selector_name, date, opts), opts}}

        {:error, err} ->
          {:error, err}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "isnull",
         value,
         _ast,
         {_query, opts}
       )
       when is_boolean(value) do
    if is_selector_allowed?(selector_name, opts) do
      {:ok, {isnull_filter(selector_name, value, opts), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value_with_comparison(_selector_name, _op, value, _ast, _state)
       when is_number(value) do
    {:error, :unsupported_operator}
  end

  defp do_handle_selector_and_value_with_comparison(_selector_name, _op, _value, _ast, _state) do
    {:error, :invalid_value}
  end

  defp string_to_atom(value), do: String.to_existing_atom(value)

  defp add_select(schema, []), do: schema

  defp add_select(schema, select) do
    select(schema, ^select)
  end

  defp add_limit(schema, nil), do: schema

  defp add_limit(schema, limit) do
    limit(schema, ^limit)
  end

  defp add_order_by(order_by, opts) do
    order_by
    |> Enum.flat_map(fn {direction, field} ->
      case order_by_field_allowed?(field, opts) do
        true -> [{direction, dynamic([q], field(q, ^convert_selector(field)))}]
        false -> []
      end
    end)
    |> Keyword.new()
  end

  defp order_by_field_allowed?(field, opts) when is_atom(field) do
    order_by_field_allowed?(Atom.to_string(field), opts)
  end

  defp order_by_field_allowed?(field, opts) do
    field in get_schema_fields(opts)
  end

  defp is_selector_allowed?(selector, opts) do
    case maybe_associations_selector?(selector) do
      true -> is_association_selector_allowed(selector, opts)
      false -> is_single_selector_allowed(selector, opts)
    end
  end

  defp maybe_associations_selector?(selector) do
    String.contains?(selector, ".")
  end

  defp is_single_selector_allowed(selector, opts) do
    case selector in get_schema_fields(opts) do
      true ->
        selector = convert_selector(selector)
        get_select_only_option(selector, opts)

      false ->
        false
    end
  end

  defp is_association_selector_allowed(selector, opts) do
    {association, assoc_selector} = get_association_selector(selector)

    case association in get_schema_associations(opts) do
      true ->
        validate_association_field(assoc_selector)

      false ->
        false
    end
  end

  defp validate_association_field(assoc_selector) do
    _ = String.to_existing_atom(assoc_selector)
    true
  rescue
    _ -> false
  end

  defp get_select_only_option(selector, opts) do
    case Keyword.get(opts, :only, nil) do
      nil ->
        get_select_except_option(selector, opts)

      fields ->
        fields = Enum.map(fields, fn s -> convert_selector(s) end)
        Enum.member?(fields, selector)
    end
  end

  defp get_select_except_option(selector, opts) do
    case Keyword.get(opts, :except, nil) do
      nil ->
        true

      fields ->
        fields = Enum.map(fields, fn s -> convert_selector(s) end)
        not Enum.member?(fields, selector)
    end
  end

  defp is_case_insensitive(opts) do
    not Keyword.get(opts, :case_sensitive, true)
  end

  defp get_select_option(ast, opts) do
    case Keyword.get(opts, :select, :all) do
      :all ->
        []

      :from_selectors ->
        get_select_from_selectors(ast)

      selectors ->
        Enum.map(selectors, fn s -> convert_selector(s) end)
    end
  end

  defp get_select_from_selectors(ast) do
    ast
    |> get_selectors()
    |> Enum.reduce([], fn s, acc ->
      case maybe_associations_selector?(s) do
        true -> acc
        false -> Enum.concat(acc, [string_to_atom(s)])
      end
    end)
  end

  defp maybe_date_time_value(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _date, _} ->
        {:ok, value}

      {:error, _err} ->
        maybe_duration_value(value)
    end
  end

  defp maybe_duration_value(value) do
    case parse_duration(value) do
      {:ok, value, op} -> parse_duration_value(value, op)
      {:error, _} -> {:error, :invalid_comparison_value}
    end
  end

  defp parse_duration_value(value, op) do
    case ISO8601Parser.parse(value) do
      {:ok, duration} ->
        new_date = Timex.shift(Timex.now(), seconds: duration.seconds * op)
        {:ok, new_date |> DateTime.to_iso8601()}

      {:error, _error} ->
        {:error, :invalid_comparison_value}
    end
  end

  defp convert_selector(value) when is_atom(value), do: value
  defp convert_selector(value), do: string_to_atom(value)

  defp parse_duration("P" <> rest), do: {:ok, "P" <> rest, 1}

  defp parse_duration("-P" <> rest), do: {:ok, "P" <> rest, -1}

  defp parse_duration(value), do: {:error, value}

  defp get_schema_fields(opts), do: Keyword.get(opts, :schema_fields, [])

  defp get_schema_associations(opts), do: Keyword.get(opts, :schema_associations, [])

  defp get_association_selector(selector) do
    [association, selector] = String.split(selector, ".")
    {association, selector}
  end

  defp get_association_selector_to_atom(selector) do
    {association, selector} = get_association_selector(selector)
    {String.to_existing_atom(association), String.to_existing_atom(selector)}
  end

  defp inital_query(opts) do
    schema = Keyword.get(opts, :schema)
    initial_query = Keyword.get(opts, :initial_query)

    case initial_query do
      nil -> schema
      _ -> initial_query
    end
  end

  defp assoc_value_type(opts, association, assoc_selector) do
    Keyword.get(opts, :casting_assoc_fields, &default_casting_assoc_fields/2).(
      association,
      assoc_selector
    )
  end

  def default_casting_assoc_fields(_association, _assoc_selector) do
    :string
  end

  defp field_can_be_insensitive(opts, table, field) do
    case assoc_value_type(opts, table, field) do
      :string -> true
      _ -> false
    end
  end
end
