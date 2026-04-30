WITH All_Touchpoints AS (
    -- Step 1: Stack both Meta and Website data
    SELECT 
        Phone_Number, 
        Date AS Touch_Date, 
        'Meta Ads' AS Channel 
    FROM `makegro-analytics-hub.agency_performance.meta_leads`
    
    UNION ALL
    
    SELECT 
        Phone_Number, 
        Date AS Touch_Date, 
        'Website/Google' AS Channel 
    FROM `makegro-analytics-hub.agency_performance.website_clicks`
),

Ranked_Touchpoints AS (
    -- Step 2: Group by Phone Number and rank the dates from oldest to newest
    SELECT 
        Phone_Number,
        Channel,
        Touch_Date,
        ROW_NUMBER() OVER(PARTITION BY Phone_Number ORDER BY Touch_Date ASC) as Touch_Rank
    FROM All_Touchpoints
)

-- Step 3: Filter for ONLY the first touch and EXPOSE THE DATE for Looker Studio
SELECT 
    Touch_Date,                                    
    Channel AS True_First_Touch_Channel,
    COUNT(DISTINCT Phone_Number) AS Total_Unique_Leads_Acquired
FROM Ranked_Touchpoints
WHERE Touch_Rank = 1
GROUP BY Touch_Date, Channel                         
ORDER BY Total_Unique_Leads_Acquired DESC;
