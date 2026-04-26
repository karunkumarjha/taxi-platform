{#
    Custom generic test: trip_duration_sane
    ----------------------------------------
    Fails if any row in `model` has a `column_name` value outside [0, max_minutes].

    Why this test: trip duration is the single metric most likely to silently
    corrupt downstream revenue and demand answers if it's wrong. TLC has a
    documented history of clock-skew + timezone bugs that produce durations of
    hundreds of hours or negative numbers. The standard schema tests (not_null,
    accepted_values) don't catch "implausible but structurally valid" values —
    this one does.

    Usage:
      columns:
        - name: trip_duration_min
          tests:
            - trip_duration_sane:
                max_minutes: 720
                row_condition: "is_valid"     # optional — only evaluate on matching rows

    max_minutes defaults to 720 (12h), which is a conservative upper bound for
    a legitimate single taxi trip (JFK round-trip with heavy traffic fits well
    under that). Negative or zero durations also fail — they indicate a clock bug.
#}
{% test trip_duration_sane(model, column_name, max_minutes=720, row_condition='1 = 1') %}

select *
from {{ model }}
where {{ row_condition }}
  and (
        {{ column_name }} is null
     or {{ column_name }} <= 0
     or {{ column_name }} > {{ max_minutes }}
  )

{% endtest %}
