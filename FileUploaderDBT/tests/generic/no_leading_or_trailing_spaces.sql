{% test no_leading_or_trailing_spaces(model, column_name) %}
 
    select *
    from {{ model }}
    where ltrim(rtrim({{ column_name }})) != {{ column_name }}
 
{% endtest %}