{% test no_less_than(model, column_name, lower_boundary) %}

    select *
    from {{ model }}
    where CAST({{ column_name }} AS INT)  < {{ lower_boundary }}

{% endtest %}