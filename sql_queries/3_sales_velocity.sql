WITH Sales_Cycle_Data AS (
    -- Step 1: Join leads to sales and calculate the raw days to close
    SELECT 
        m.Lead_ID,
        m.Date AS Lead_Generated_Date,
        c.Closing_Date,
        c.Final_Brand_Booked,
        c.Contract_Value_INR,
        c.Sales_Rep,
        -- Calculate the difference between lead generation and closing
        DATE_DIFF(CAST(c.Closing_Date AS DATE), CAST(m.Date AS DATE), DAY) AS Raw_Days_To_Close
    FROM 
        `makegro-analytics-hub.agency_performance.meta_leads` m
    INNER JOIN 
        `makegro-analytics-hub.agency_performance.crm_sales` c
    ON 
        m.Phone_Number = c.Phone_Number
    WHERE 
        c.Closing_Date IS NOT NULL
)

-- Step 2: Keep individual records and add the Audit Flags
SELECT 
    Lead_Generated_Date,  -- Essential for the Looker Calendar
    Closing_Date,
    Final_Brand_Booked,
    Sales_Rep,
    Contract_Value_INR,
    Raw_Days_To_Close,
    CASE 
        WHEN Raw_Days_To_Close < 0 THEN 'Error: Closed Before Lead'
        WHEN Raw_Days_To_Close > 365 THEN 'Error: Cycle > 1 Year'
        ELSE 'Valid'
    END AS Data_Quality_Flag
FROM 
    Sales_Cycle_Data;
