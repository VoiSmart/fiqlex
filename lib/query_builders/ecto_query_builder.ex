defmodule FIQLEx.QueryBuilders.EctoQueryBuilder do
  @moduledoc ~S"""
  Builds Ecto queries from FIQL AST.

  Possible options for this query builder are:

  * `schema`: The schema name to use in the `FROM` statement
  * `select`: `SELECT` statement to build (_see below_).
  * `only`: A list of atom/binary with the only fields to accept in the query (if `only` and `except` are both provided, `only` is used)
  * `except`: A list of atom/binary with the fields to reject in the query (if `only` and `except` are both provided, `only` is used)
  * `order_by`: A tuple list {direction, field} for order by to be added to the query. Direction is :asc or :desc, field is an atom
  * `limit`: A limit for the query
  * `case_sensitive`: Boolean value (default to true) to set equals case sensitive or not
  * `transformer`: Function that takes a selector and its value as parameter and must return a tuple {new_selector, new_value} with the transformed values


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
    schema_fields =
      case Keyword.get(opts, :schema) do
        nil -> []
        schema -> Enum.map(schema.__schema__(:fields), fn field -> Atom.to_string(field) end)
      end

    {"", Keyword.put(opts, :schema_fields, schema_fields)}
  end

  @impl true
  def build(ast, {query, opts}) do
    schema = Keyword.get(opts, :schema)
    order_by = Keyword.get(opts, :order_by, [])

    select = get_select_option(ast, opts)

    limit = Keyword.get(opts, :limit)

    final_query =
      schema
      |> add_select(select)
      |> order_by(^add_order_by(order_by))
      |> where(^query)
      |> add_limit(limit)

    {:ok, final_query}
  end

  def binary_equal(selector_name, value, opts) do
    selector_name = string_to_atom(selector_name)

    if is_case_insensitive(opts) do
      dynamic([q], fragment("lower(?)", field(q, ^selector_name)) == fragment("lower(?)", ^value))
    else
      dynamic([q], field(q, ^selector_name) == ^value)
    end
  end

  def binary_like(selector_name, value, opts) do
    selector_name = string_to_atom(selector_name)

    if is_case_insensitive(opts) do
      value = String.replace(escape_string(value), "*", "%", global: true)
      dynamic([q], ilike(field(q, ^selector_name), ^value))
    else
      value = String.replace(escape_string(value), "*", "%", global: true)
      dynamic([q], like(field(q, ^selector_name), ^value))
    end
  end

  def binary_not_equal(selector_name, value, opts) do
    selector_name = string_to_atom(selector_name)

    if is_case_insensitive(opts) do
      dynamic([q], fragment("lower(?)", field(q, ^selector_name)) != fragment("lower(?)", ^value))
    else
      dynamic([q], field(q, ^selector_name) != ^value)
    end
  end

  def binary_not_like(selector_name, value, opts) do
    selector_name = string_to_atom(selector_name)

    if is_case_insensitive(opts) do
      value = String.replace(escape_string(value), "*", "%", global: true)
      dynamic([q], not ilike(field(q, ^selector_name), ^value))
    else
      value = String.replace(escape_string(value), "*", "%", global: true)
      dynamic([q], not like(field(q, ^selector_name), ^value))
    end
  end

  def identity_transformer(selector, value), do: {selector, value}

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

  defp do_handle_selector_and_value(selector_name, :equal, value, _ast, {_query, opts})
       when is_binary(value) do
    if is_selector_allowed?(selector_name, opts) do
      if String.starts_with?(value, "*") || String.ends_with?(value, "*") do
        {:ok, {binary_like(selector_name, value, opts), opts}}
      else
        {:ok, {binary_equal(selector_name, escape_string(value), opts), opts}}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, value, _ast, {_query, opts})
       when is_list(value) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      values = value |> escape_list()

      {:ok, {dynamic([q], field(q, ^selector_name) in ^values), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, true, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) == ^to_string(true))}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :equal, false, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) == ^to_string(false)), opts}}
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
        {:ok, {binary_not_equal(selector_name, escape_string(value), opts), opts}}
      end
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, value, _ast, {_query, opts})
       when is_list(value) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      values = value |> escape_list()
      {:ok, {dynamic([q], field(q, ^selector_name) not in ^values), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, true, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) != ^to_string(true)), opts}}
    else
      {:error, :selector_not_allowed}
    end
  end

  defp do_handle_selector_and_value(selector_name, :not_equal, false, _ast, {_query, opts}) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) != ^to_string(false)), opts}}
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

  defp do_handle_selector_and_value_with_comparison(
         selector_name,
         "ge",
         value,
         _ast,
         {_query, opts}
       )
       when is_number(value) do
    if is_selector_allowed?(selector_name, opts) do
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) >= ^to_string(value)), opts}}
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
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) > ^to_string(value)), opts}}
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
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) <= ^to_string(value)), opts}}
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
      selector_name = string_to_atom(selector_name)
      {:ok, {dynamic([q], field(q, ^selector_name) < ^to_string(value)), opts}}
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
      selector_name = string_to_atom(selector_name)

      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok,
           {dynamic(
              [q],
              field(q, ^selector_name) >= fragment("?::date", ^to_string(escape_string(date)))
            ), opts}}

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
      selector_name = string_to_atom(selector_name)

      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok,
           {dynamic(
              [q],
              field(q, ^selector_name) > fragment("?::date", ^to_string(escape_string(date)))
            ), opts}}

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
      selector_name = string_to_atom(selector_name)

      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok,
           {dynamic(
              [q],
              field(q, ^selector_name) <= fragment("?::date", ^to_string(escape_string(date)))
            ), opts}}

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
      selector_name = string_to_atom(selector_name)

      case maybe_date_time_value(value) do
        {:ok, date} ->
          {:ok,
           {dynamic(
              [q],
              field(q, ^selector_name) < fragment("?::date", ^to_string(escape_string(date)))
            ), opts}}

        {:error, err} ->
          {:error, err}
      end
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

  defp escape_string(str) when is_binary(str),
    do: "'" <> String.replace(str, "'", "''", global: true) <> "'"

  defp escape_string(str), do: to_string(str)

  defp escape_list(list), do: Enum.map(list, &escape_string/1)

  defp string_to_atom(value), do: String.to_existing_atom(value)

  defp add_select(schema, []), do: schema

  defp add_select(schema, select) do
    select(schema, ^select)
  end

  defp add_limit(schema, nil), do: schema

  defp add_limit(schema, limit) do
    limit(schema, ^limit)
  end

  defp add_order_by(order_by) do
    order_by
    |> Enum.map(fn {direction, field} -> {direction, dynamic([q], field(q, ^field))} end)
    |> Keyword.new()
  end

  defp is_selector_allowed?(selector, opts) do
    schema_fields = Keyword.get(opts, :schema_fields)

    case selector in schema_fields do
      true ->
        selector = convert_selector(selector)
        get_select_only_option(selector, opts)

      false ->
        false
    end
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
        ast
        |> get_selectors()
        |> Enum.map(fn s -> string_to_atom(s) end)

      selectors ->
        Enum.map(selectors, fn s -> convert_selector(s) end)
    end
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
end
