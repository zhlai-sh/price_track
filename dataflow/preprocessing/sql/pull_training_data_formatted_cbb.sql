-- datapull to execute in gcp BigQuery to pull historical transactions
SELECT spf.transaction_id
     , e.event_id
     , spf.ticket_id
     , sqs.seat_quality_score                                   AS sqs
     , log(spf.ticket_cost/spf.quantity)-sqs.seat_quality_score AS bvs
     ,     spf.ticket_cost/spf.quantity                         AS price
     , TIMESTAMP_DIFF(spf.src_created_dttm_sale, '1970-01-01', MILLISECOND) AS timestamp
FROM `stubhub-dataplatform-prd.stub_data_platform.dw_sales_pipeline_fact` spf
JOIN `stubhub-dataplatform-prd.stub_data_platform.dw_events_dim` e
  ON spf.event_dw_id = e.event_dw_id
JOIN `stubhub-dataplatform-prd.stub_data_platform.dw_genre_dim` g
  ON e.genre_id = g.genre_id
JOIN `stubhub-dataplatform-prd.stub_ecomm.stub_trans` st
  ON spf.transaction_id = st.id
JOIN `stubhub-dataplatform-prd.data_science_dev.SQS_Tier_32` sqs
  ON e.venue_config_id = sqs.venue_config_id
 AND st.venue_config_sections_id = sqs.section_id
 AND st.row_desc = sqs.row_desc
WHERE (g.genre_cat_final = 'College Basketball' AND e.season = 2018)  -- below, these are required data filtering logic:
  AND spf.transaction_id is not null                   -- data-integrity checks: since spf table on gcp do not enforce these fields as non-nullable
  AND spf.ticket_id is not null
  AND spf.src_created_dttm_sale is not null
  AND e.venue_config_id is not null
  AND lower(e.game_type) IN ('regular')                -- consider only regular events
  AND lower(e.event_cancelled_yn) = 'n'                -- consider non-cancelled events
  AND lower(e.event_status) IN ('active', 'completed') -- consider only active and completed events
  AND e.season_ticket_package_ind = 0                  -- drop season ticket packages
  AND e.mirror_event_ind = 0                           -- drop mirror events
  AND spf.quantity >= 2                                -- consider only transactions with min casepack of e.g. 2
  AND spf.src_created_dttm_sale < e.event_date_utc     -- require transactions to occur before event date
  AND sqs.seat_quality_score > -10                     -- drop nonsensical seat quality scores
  AND spf.ticket_cost/spf.quantity BETWEEN 1 and 1000000000 -- drop nonsensical prices
ORDER BY e.event_id, spf.src_created_dttm_sale ASC