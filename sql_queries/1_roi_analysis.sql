WITH Lead_Journey AS (
    SELECT 
        m.Lead_ID,
        m.Date AS Lead_Date,
        m.Campaign_Name,
        m.Actual_Daily_Spend,
        c.Final_Brand_Booked,
        c.Contract_Value_INR
    FROM `makegro-analytics-hub.agency_performance.meta_leads` m
    LEFT JOIN `makegro-analytics-hub.agency_performance.crm_sales` c
    ON m.Phone_Number = c.Phone_Number
)

SELECT 
    Lead_Date,
    Campaign_Name,
    COUNT(Lead_ID) AS Total_Leads_Generated,
    SUM(Actual_Daily_Spend) AS Total_Ad_Spend_INR,
    SUM(CASE WHEN Final_Brand_Booked = '24 Frames' THEN 1 ELSE 0 END) AS Closed_24F_Deals,
    SUM(CASE WHEN Final_Brand_Booked = 'Visual Stories' THEN 1 ELSE 0 END) AS Closed_VS_Deals,
    SUM(Contract_Value_INR) AS Total_Revenue_Generated
FROM Lead_Journey
GROUP BY Lead_Date, Campaign_Name;
