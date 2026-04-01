{% macro duree_avion_min(distance_km) %}
    ROUND((({{ distance_km }} / 800.0) + 0.5) * 60)
{% endmacro %}