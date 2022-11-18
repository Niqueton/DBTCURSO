{% macro obtener_tipo_eventos() %}
{{ return(["checkout", "package_shipped", "add_to_cart","page_view"]) }}
{% endmacro %}

-- Una función clasica pero puede estar muy chetada