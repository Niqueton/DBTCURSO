{% test menor_que(model, column_name,a) %}

select {{ column_name }} from {{ model }}
where {{ column_name }}>{{ a }}

{% endtest %}