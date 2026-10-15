{#
  NBA's live feeds express clock and minutes as ISO-8601 durations, e.g.
  'PT34M12.00S' (34:12) or 'PT05M00.00S'. Returns seconds as a double.
#}
{% macro iso_duration_to_seconds(column) %}
    (
        coalesce(try_cast(regexp_extract({{ column }}, 'PT(\d+)M', 1) as integer), 0) * 60
        + coalesce(try_cast(regexp_extract({{ column }}, '([\d.]+)S', 1) as double), 0)
    )
{% endmacro %}
