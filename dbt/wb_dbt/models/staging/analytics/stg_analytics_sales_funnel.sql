{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}

with latest_raw as (

    select distinct on (client_id, wb_account_id, source_system, dataset_name, source_file)
        id as raw_payload_id,
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{ source('quarantine', 'v_raw_payloads_schema_passed') }}
    where dataset_name = 'analytics_sales_funnel'
    order by
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
        p.client_id,
        p.wb_account_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        x.ordinality::integer as record_index,
        x.raw_record
    from latest_raw p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload #> '{data,products}') = 'array' then p.payload #> '{data,products}'
            when jsonb_typeof(p.payload #> '{data,products}') = 'object' then jsonb_build_array(p.payload #> '{data,products}')
            else '[]'::jsonb
        end
    ) with ordinality as x(raw_record, ordinality)

),

typed as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_record,
        nullif(raw_record #>> '{product,brandName}', '') as product_brand_name,
        staging.try_numeric(raw_record #>> '{product,feedbackRating}') as product_feedback_rating,
        staging.try_bigint(raw_record #>> '{product,nmId}') as product_nm_id,
        staging.try_numeric(raw_record #>> '{product,productRating}') as product_product_rating,
        staging.try_bigint(raw_record #>> '{product,stocks,balanceSum}') as product_stocks_balance_sum,
        staging.try_bigint(raw_record #>> '{product,stocks,mp}') as product_stocks_mp,
        staging.try_bigint(raw_record #>> '{product,stocks,wb}') as product_stocks_wb,
        staging.try_bigint(raw_record #>> '{product,subjectId}') as product_subject_id,
        nullif(raw_record #>> '{product,subjectName}', '') as product_subject_name,
        raw_record #> '{product,tags}' as product_tags,
        nullif(raw_record #>> '{product,title}', '') as product_title,
        nullif(raw_record #>> '{product,vendorCode}', '') as product_vendor_code,
        staging.try_bigint(raw_record #>> '{statistic,comparison,addToWishlistDynamic}') as statistic_comparison_add_to_wishlist_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,avgOrdersCountPerDayDynamic}') as statistic_comparison_avg_orders_count_per_day_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,avgPriceDynamic}') as statistic_comparison_avg_price_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,buyoutCountDynamic}') as statistic_comparison_buyout_count_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,buyoutSumDynamic}') as statistic_comparison_buyout_sum_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,cancelCountDynamic}') as statistic_comparison_cancel_count_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,cancelSumDynamic}') as statistic_comparison_cancel_sum_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,cartCountDynamic}') as statistic_comparison_cart_count_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,conversions,addToCartPercent}') as statistic_comparison_conversions_add_to_cart_percent,
        staging.try_bigint(raw_record #>> '{statistic,comparison,conversions,buyoutPercent}') as statistic_comparison_conversions_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,comparison,conversions,cartToOrderPercent}') as statistic_comparison_conversions_cart_to_order_percent,
        staging.try_bigint(raw_record #>> '{statistic,comparison,localizationPercentDynamic}') as statistic_comparison_localization_percent_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,openCountDynamic}') as statistic_comparison_open_count_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,orderCountDynamic}') as statistic_comparison_order_count_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,orderSumDynamic}') as statistic_comparison_order_sum_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,shareOrderPercentDynamic}') as statistic_comparison_share_order_percent_dynamic,
        staging.try_bigint(raw_record #>> '{statistic,comparison,timeToReadyDynamic,days}') as statistic_comparison_time_to_ready_dynamic_days,
        staging.try_bigint(raw_record #>> '{statistic,comparison,timeToReadyDynamic,hours}') as statistic_comparison_time_to_ready_dynamic_hours,
        staging.try_bigint(raw_record #>> '{statistic,comparison,timeToReadyDynamic,mins}') as statistic_comparison_time_to_ready_dynamic_mins,
        staging.try_numeric(raw_record #>> '{statistic,comparison,wbClubDynamic,avgOrderCountPerDay}') as statistic_comparison_wb_club_dynamic_avg_order_count_per_day,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,avgPrice}') as statistic_comparison_wb_club_dynamic_avg_price,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,buyoutCount}') as statistic_comparison_wb_club_dynamic_buyout_count,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,buyoutPercent}') as statistic_comparison_wb_club_dynamic_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,buyoutSum}') as statistic_comparison_wb_club_dynamic_buyout_sum,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,cancelCount}') as statistic_comparison_wb_club_dynamic_cancel_count,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,cancelSum}') as statistic_comparison_wb_club_dynamic_cancel_sum,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,orderCount}') as statistic_comparison_wb_club_dynamic_order_count,
        staging.try_bigint(raw_record #>> '{statistic,comparison,wbClubDynamic,orderSum}') as statistic_comparison_wb_club_dynamic_order_sum,
        staging.try_bigint(raw_record #>> '{statistic,past,addToWishlist}') as statistic_past_add_to_wishlist,
        staging.try_numeric(raw_record #>> '{statistic,past,avgOrdersCountPerDay}') as statistic_past_avg_orders_count_per_day,
        staging.try_bigint(raw_record #>> '{statistic,past,avgPrice}') as statistic_past_avg_price,
        staging.try_bigint(raw_record #>> '{statistic,past,buyoutCount}') as statistic_past_buyout_count,
        staging.try_bigint(raw_record #>> '{statistic,past,buyoutSum}') as statistic_past_buyout_sum,
        staging.try_bigint(raw_record #>> '{statistic,past,cancelCount}') as statistic_past_cancel_count,
        staging.try_bigint(raw_record #>> '{statistic,past,cancelSum}') as statistic_past_cancel_sum,
        staging.try_bigint(raw_record #>> '{statistic,past,cartCount}') as statistic_past_cart_count,
        staging.try_bigint(raw_record #>> '{statistic,past,conversions,addToCartPercent}') as statistic_past_conversions_add_to_cart_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,conversions,buyoutPercent}') as statistic_past_conversions_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,conversions,cartToOrderPercent}') as statistic_past_conversions_cart_to_order_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,localizationPercent}') as statistic_past_localization_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,openCount}') as statistic_past_open_count,
        staging.try_bigint(raw_record #>> '{statistic,past,orderCount}') as statistic_past_order_count,
        staging.try_bigint(raw_record #>> '{statistic,past,orderSum}') as statistic_past_order_sum,
        staging.try_timestamptz(raw_record #>> '{statistic,past,period,end}') as statistic_past_period_end,
        staging.try_timestamptz(raw_record #>> '{statistic,past,period,start}') as statistic_past_period_start,
        staging.try_numeric(raw_record #>> '{statistic,past,shareOrderPercent}') as statistic_past_share_order_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,timeToReady,days}') as statistic_past_time_to_ready_days,
        staging.try_bigint(raw_record #>> '{statistic,past,timeToReady,hours}') as statistic_past_time_to_ready_hours,
        staging.try_bigint(raw_record #>> '{statistic,past,timeToReady,mins}') as statistic_past_time_to_ready_mins,
        staging.try_numeric(raw_record #>> '{statistic,past,wbClub,avgOrderCountPerDay}') as statistic_past_wb_club_avg_order_count_per_day,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,avgPrice}') as statistic_past_wb_club_avg_price,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,buyoutCount}') as statistic_past_wb_club_buyout_count,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,buyoutPercent}') as statistic_past_wb_club_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,buyoutSum}') as statistic_past_wb_club_buyout_sum,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,cancelCount}') as statistic_past_wb_club_cancel_count,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,cancelSum}') as statistic_past_wb_club_cancel_sum,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,orderCount}') as statistic_past_wb_club_order_count,
        staging.try_bigint(raw_record #>> '{statistic,past,wbClub,orderSum}') as statistic_past_wb_club_order_sum,
        staging.try_bigint(raw_record #>> '{statistic,selected,addToWishlist}') as statistic_selected_add_to_wishlist,
        staging.try_numeric(raw_record #>> '{statistic,selected,avgOrdersCountPerDay}') as statistic_selected_avg_orders_count_per_day,
        staging.try_bigint(raw_record #>> '{statistic,selected,avgPrice}') as statistic_selected_avg_price,
        staging.try_bigint(raw_record #>> '{statistic,selected,buyoutCount}') as statistic_selected_buyout_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,buyoutSum}') as statistic_selected_buyout_sum,
        staging.try_bigint(raw_record #>> '{statistic,selected,cancelCount}') as statistic_selected_cancel_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,cancelSum}') as statistic_selected_cancel_sum,
        staging.try_bigint(raw_record #>> '{statistic,selected,cartCount}') as statistic_selected_cart_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,conversions,addToCartPercent}') as statistic_selected_conversions_add_to_cart_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,conversions,buyoutPercent}') as statistic_selected_conversions_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,conversions,cartToOrderPercent}') as statistic_selected_conversions_cart_to_order_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,localizationPercent}') as statistic_selected_localization_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,openCount}') as statistic_selected_open_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,orderCount}') as statistic_selected_order_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,orderSum}') as statistic_selected_order_sum,
        staging.try_timestamptz(raw_record #>> '{statistic,selected,period,end}') as statistic_selected_period_end,
        staging.try_timestamptz(raw_record #>> '{statistic,selected,period,start}') as statistic_selected_period_start,
        staging.try_numeric(raw_record #>> '{statistic,selected,shareOrderPercent}') as statistic_selected_share_order_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,timeToReady,days}') as statistic_selected_time_to_ready_days,
        staging.try_bigint(raw_record #>> '{statistic,selected,timeToReady,hours}') as statistic_selected_time_to_ready_hours,
        staging.try_bigint(raw_record #>> '{statistic,selected,timeToReady,mins}') as statistic_selected_time_to_ready_mins,
        staging.try_numeric(raw_record #>> '{statistic,selected,wbClub,avgOrderCountPerDay}') as statistic_selected_wb_club_avg_order_count_per_day,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,avgPrice}') as statistic_selected_wb_club_avg_price,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,buyoutCount}') as statistic_selected_wb_club_buyout_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,buyoutPercent}') as statistic_selected_wb_club_buyout_percent,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,buyoutSum}') as statistic_selected_wb_club_buyout_sum,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,cancelCount}') as statistic_selected_wb_club_cancel_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,cancelSum}') as statistic_selected_wb_club_cancel_sum,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,orderCount}') as statistic_selected_wb_club_order_count,
        staging.try_bigint(raw_record #>> '{statistic,selected,wbClub,orderSum}') as statistic_selected_wb_club_order_sum
    from expanded

)

select *
from typed
