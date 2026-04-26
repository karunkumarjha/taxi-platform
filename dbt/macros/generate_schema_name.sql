{#
    Override of dbt's default schema-name resolution.

    Default behaviour: when a model says `+schema: marts_build`, dbt produces
    `<target.schema>_marts_build` (e.g. STAGING_marts_build). That's wrong for
    our blue-green pattern — we want the model materialised in the literal
    `MARTS_BUILD` schema so the SWAP works.

    This macro returns the custom schema as-is when one is set, falling back
    to the target's schema otherwise.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
