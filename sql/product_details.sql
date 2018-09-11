-- extract rows necessary to understand product composition from dm+d
WITH ingredients AS (
         SELECT dmd_vpi_1.vpid,
            count(*) AS count
           FROM dmd_vpi dmd_vpi_1
          GROUP BY dmd_vpi_1.vpid
        )
 SELECT DISTINCT dmd_product.bnf_code,
    dmd_product.name,
    dmd_product.vpid,
    dmd_lookup_form."desc" AS form,
    indicator_lookup."desc" AS form_indicator,
    dmd_vmp.udfs AS form_size,
    form_size_lookup."desc" AS form_units,
    unit_dose_lookup."desc" AS unit_of_measure,
    dmd_vpi.strnt_nmrtr_val AS numerator,
    numerator_measure_lookup."desc" AS numerator_unit_of_measure,
    dmd_vpi.strnt_dnmtr_val AS denominator,
    denominator_measure_lookup."desc" AS denominator_unit_of_measure,
    ingredients.count AS ingredient_count
   FROM dmd_product
     JOIN dmd_vmp ON dmd_vmp.vpid = dmd_product.vpid
     LEFT JOIN dmd_vpi ON dmd_vpi.vpid = dmd_product.vpid
     LEFT JOIN ingredients ON ingredients.vpid = dmd_product.vpid
     LEFT JOIN dmd_lookup_df_indicator indicator_lookup ON indicator_lookup.cd = dmd_vmp.df_indcd
     LEFT JOIN dmd_lookup_unit_of_measure form_size_lookup ON form_size_lookup.cd = dmd_vmp.udfs_uomcd
     LEFT JOIN dmd_lookup_unit_of_measure unit_dose_lookup ON unit_dose_lookup.cd = dmd_vmp.unit_dose_uomcd
     LEFT JOIN dmd_lookup_unit_of_measure numerator_measure_lookup ON numerator_measure_lookup.cd = dmd_vpi.strnt_nmrtr_uomcd
     LEFT JOIN dmd_lookup_unit_of_measure denominator_measure_lookup ON denominator_measure_lookup.cd = dmd_vpi.strnt_dnmtr_uomcd
     left join dmd_dform on dmd_dform.vpid = dmd_vmp.vpid inner join dmd_lookup_form on dmd_dform.formcd = dmd_lookup_form.cd
     LEFT JOIN dmd_ont ON dmd_product.vpid = dmd_ont.vpid
     LEFT JOIN dmd_lookup_ont_form_route ON dmd_lookup_ont_form_route.cd = dmd_ont.formcd
WHERE bnf_code IS NOT NULL;
