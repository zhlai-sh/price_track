-- datapull to execute in gcp BigQuery to pull historical transactions
SELECT st.id        AS transaction_id
     , st.event_id
     , st.ticket_id
     , e.event_date
     , st.date_added
     , st.quantity
     , st.total_cost
     , e.venue_config_id
     , sqs.sqs
FROM `stubhub-dataplatform-prd.stub_ecomm.stub_trans` st
JOIN `stubhub-dataplatform-prd.stub_data_platform.dw_events_dim` e
  ON st.event_id = e.event_id
JOIN `stubhub-dataplatform-prd.stub_data_platform.dw_genre_dim` g
  ON e.genre_id = g.genre_id
JOIN (SELECT s.venue_config_id
           , s.section_id
           , r.row_desc
           , s.coefficient AS section_coefficient
           , r.coefficient AS row_coefficient
           , s.coefficient + coalesce(r.coefficient,0) AS sqs
      FROM `stubhub-dataplatform-prd.p13n_data.section_coefficient` s
      LEFT JOIN (SELECT DISTINCT venue_config_id, section_id, row_desc, coefficient
                 FROM `stubhub-dataplatform-prd.p13n_data.row_coefficient`
                 WHERE coefficient_version = 32
                ) r
        ON s.venue_config_id = r.venue_config_id
       AND s.section_id = r.section_id
      WHERE s.coefficient_version = 32
     ) sqs
  ON e.venue_config_id = sqs.venue_config_id
 AND st.venue_config_sections_id = sqs.section_id
 AND st.row_desc = sqs.row_desc
WHERE (   (g.genre_cat_final = 'MLB' AND e.season = 2019)
       OR (g.genre_cat_final = 'NFL' AND e.season = 2018)
       OR (g.genre_cat_final = 'NBA' AND e.season = 2018)
       OR (g.genre_cat_final = 'NHL' AND e.season = 2018)
      )
  AND lower(e.game_type) IN ('regular','post-season')
  AND lower(e.event_cancelled_yn) = 'n'
  AND lower(e.event_status) IN ('active', 'completed')
  AND e.venue_config_id IS NOT NULL
  AND e.season_ticket_package_ind = 0
  AND e.mirror_event_ind = 0
  AND st.ticket_id IS NOT NULL