# BQ

GA360 

1) select parse_date('%Y%m%d', date) as date,
          hits.eventInfo.eventAction,
          cd.value,
          cd.index,
          concat(clientId, "-", visitNumber, "-", date) as sessionId,
          device.deviceCategory
   from 'xxxxxx.xxxx.ga_sessions_*', unnest(hits) hits, unnest(hits.customDimension) cd
   where _table_suffix between "20220101" and "20220202"
   
2) case when totals.newvisits IS NOT NULL then 'New Visitor' else 'returning' end as user_type
3) min(if(hits.isInteraction IS NOT NULL, hits.hitnumber, 0) over (partition by fullVisitorId, visitStartTime) as first_interaction
4) min(hits.hitNumber) over (partition by fullVisitorId, visitStartTime) as first_hit
5) case when visitnumber != 1 and max(totals.newVisits) over (partition by fullVisitorId) IS NULL then TRUE end as old_user

GA4

1) select timestamp(datetime(timestamp_micros(event_timestamp), "America/Denver")) as event_timestamp,
          case when regexp_contains(traffic_source, medium, r'^(?!)directmail') 
               and  regexp_contains(traffic_source, source, r'^(?!)qr') 
               then 'DirectMail'
               else null
               end as channel,
          (select max(value.string_value) from unnest(event_params) where key = 'retailer_name') as retailer_name,
          (select max(value.string_value) from unnest(event_params) where key = 'traffic_type') as traffic_type,
          case when (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') = 1 then 'new_visitor
          case when (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') != 1 then 'returning_visitor
          else null
          end as user_type,
          (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
          case when (select 1 from t.event_params where key = 'engagement_time_msec' and event_params.value.int_value > 0) = 1 then 1 else null end as active_user,
          (select max(value.int_value) from unnest(event_params) where key = 'session_engaged') engaged_session,
          current_timestamp() last_updated_on
          
          
          

    
