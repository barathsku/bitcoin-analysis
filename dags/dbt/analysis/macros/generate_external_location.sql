{% macro generate_external_location() %}
  {%- set subfolder = model.fqn[1] -%}
  {%- set model_name = this.name -%}
  {%- set base_path = var('data_path') -%}
  {{ base_path }}/{{ subfolder }}/{{ model_name }}.parquet
{%- endmacro %}
