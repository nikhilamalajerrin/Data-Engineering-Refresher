{% macro get_vessel_country(VesselName) -%}
(
  case trim(upper({{ dbt.safe_cast(VesselName, api.Column.translate_type("string")) }}))
    when 'BOOSTER 9000' then 'USA'
    when 'ZEEPAARD' then 'NETHERLANDS'
    when 'MILADY' then 'FRANCE'
    when 'TWINS' then 'USA'
    when 'RYAN JAMES' then 'USA'
    when 'MANANA' then 'SPAIN'
    when 'SY SIMA' then 'GERMANY'
    when 'LASAVO' then 'ITALY'
    when 'AFRICAN CONDOR' then 'SOUTH AFRICA'  -- ✅ Add this
    else 'UNKNOWN'
  end
)
{%- endmacro %}
