{% macro generate_external_location() %}
  {%- set schema = config.get('schema', 'marts') -%}
  {%- set model_name = this.name -%}
  data/{{ schema }}/{{ model_name }}
{% endmacro %}
