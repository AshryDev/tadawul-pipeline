{% macro generate_date_spine(start_date='2021-01-01', end_date='2024-12-31') %}
    WITH date_series AS (
        SELECT 
            DATE '{{ start_date }}' + INTERVAL '1' DAY * n as date
        FROM UNNEST(SEQUENCE(0, DATE_DIFF('day', DATE '{{ start_date }}', DATE '{{ end_date }}'))) AS t(n)
    )
    SELECT * FROM date_series
{% endmacro %}