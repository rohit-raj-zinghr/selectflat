SELECT TOP 10
    re.ed_empcode AS EmpCode,
    re.ed_Salutation, 
    re.ed_firstname, 
    re.ed_MiddleName, 
    re.ed_lastname,
    re.ed_empid,
    re.ED_Status,
    se.ESM_EmpStatusDesc,
    gc_bool.IPCheckEnabled,
    gc_bool.LocationCheckEnabled,
    gc_bool.IPCheckEnabledOnMobile,
    gc_bool.PunchIn,
    gc_bool.PunchOut,

    -- Create a nested JSON object for shift details without escaping (unchanged)
    (
        SELECT ShiftID,
            JSON_QUERY(
                (
                    SELECT 
                        MIN(ro_inner.AttMode) AS AttMode,
                        MIN(ro_inner.DiffIN) AS DiffIN,
                        MIN(ro_inner.DiffOUT) AS DiffOUT,
                        MIN(ro_inner.TotalworkedMinutes) AS TotalworkedMinutes,
                        MIN(ro_inner.RegIN) AS RegIN,
                        MIN(ro_inner.RegOut) AS RegOut,
                        MIN(ro_inner.PreTime) AS PreTime,
                        MIN(ro_inner.PostTime) AS PostTime,
                        MIN(ro_inner.FromMin) AS FromMin,
                        MIN(ro_inner.ToMin) AS ToMin,
                        MIN(sht_inner.ShiftName) AS ShiftName,
                        MIN(ShiftBlocks.ShiftStart) AS ShiftStart,  
                        MAX(ShiftBlocks.ShiftEnd) AS ShiftEnd,      
                        MIN(sht_inner.InTime) AS InTime,
                        MAX(sht_inner.OutTime) AS OutTime,
                        MAX(sht_inner.TotalMinutes) AS TotalMinutes,
                        MIN(sht_inner.SwipesSeperatorParam) AS SwipesSeperatorParam,
                        MIN(CAST(sht_inner.ISWorkBtwnShifttime AS TINYINT)) AS ISWorkBtwnShifttime,
                        MIN(CAST(sht_inner.IsBreakApplicable AS TINYINT)) AS IsBreakApplicable,
                        MIN(CAST(sht_inner.IsNightShiftApplicable AS TINYINT)) AS IsNightShiftApplicable,
                        MIN(CAST(sht_inner.IsActive AS TINYINT)) AS IsActive,
                        MIN(sht_inner.AutoShift) AS AutoShift,
                        MIN(CAST(sht_inner.ShiftAllowance AS TINYINT)) AS ShiftAllowance,
                        -- Include shift date ranges as a nested array
                        (
                            SELECT 
                                MIN(Date) AS RangeStart,
                                MAX(Date) AS RangeEnd
                            FROM (
                                SELECT 
                                    sr.Date,
                                    sr.ShiftID,
                                    sr.ShiftGroup
                                FROM (
                                    SELECT 
                                        t.Date,
                                        t.ShiftID,
                                        t.ShiftChange,
                                        SUM(t.ShiftChange) OVER (PARTITION BY t.EmpCode, t.ShiftID ORDER BY t.Date ROWS UNBOUNDED PRECEDING) AS ShiftGroup
                                    FROM (
                                        SELECT 
                                            ro.EmpCode,
                                            ro.ShiftID,
                                            ro.Date,
                                            CASE 
                                                WHEN LAG(ro.ShiftID) OVER (PARTITION BY ro.EmpCode, ro.ShiftID ORDER BY ro.Date) IS NULL THEN 1 
                                                WHEN ro.Date > DATEADD(day, 1, LAG(ro.Date) OVER (PARTITION BY ro.EmpCode, ro.ShiftID ORDER BY ro.Date)) THEN 1
                                                ELSE 0
                                            END AS ShiftChange
                                        FROM (
                                            SELECT DISTINCT
                                                r.EmpCode,
                                                FIRST_VALUE(r.ShiftID) OVER (
                                                    PARTITION BY r.EmpCode, r.Date 
                                                    ORDER BY r.ShiftID DESC
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                ) AS ShiftID,
                                                r.Date
                                            FROM tna.Rostering r
                                            WHERE r.EmpCode = re.ed_empcode
                                        ) ro
                                    ) t
                                    WHERE t.ShiftID = s.ShiftID
                                ) sr
                            ) grouped
                            GROUP BY ShiftID, ShiftGroup
                            FOR JSON PATH
                        ) AS ShiftRanges
                    FROM tna.Rostering AS ro_inner
                    INNER JOIN tna.ShiftMst AS sht_inner
                        ON ro_inner.ShiftId = sht_inner.ShiftId
                    CROSS APPLY (
                        SELECT 
                            MIN(sb_inner.Date) AS ShiftStart,
                            MAX(sb_inner.Date) AS ShiftEnd
                        FROM (
                            SELECT 
                                Date, 
                                ShiftID,
                                ShiftGroup
                            FROM (
                                SELECT 
                                    Date,
                                    ShiftID,
                                    ShiftChange,
                                    SUM(ShiftChange) OVER (PARTITION BY EmpCode, ShiftID ORDER BY Date ROWS UNBOUNDED PRECEDING) AS ShiftGroup
                                FROM (
                                    SELECT 
                                        EmpCode,
                                        ShiftID,
                                        Date,
                                        CASE 
                                            WHEN LAG(ShiftID) OVER (PARTITION BY EmpCode, ShiftID ORDER BY Date) IS NULL THEN 1 
                                            WHEN Date > DATEADD(day, 1, LAG(Date) OVER (PARTITION BY EmpCode, ShiftID ORDER BY Date)) THEN 1
                                            ELSE 0
                                        END AS ShiftChange
                                    FROM tna.Rostering
                                    WHERE EmpCode = re.ed_empcode AND ShiftID = s.ShiftID
                                ) inner_shifts
                            ) grouped_shifts
                        ) sb_inner
                        GROUP BY ShiftGroup
                    ) AS ShiftBlocks
                    WHERE ro_inner.EmpCode = re.ed_empcode AND ro_inner.ShiftId = s.ShiftID
                    GROUP BY ro_inner.ShiftId
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                )
            ) AS ShiftDetails
        FROM (
            SELECT DISTINCT ShiftID
            FROM tna.Rostering
            WHERE EmpCode = re.ed_empcode
        ) s
        FOR JSON PATH
    ) AS ShiftDetails,

    -- Create a nested JSON object for location details with resolved overlaps
    (
        SELECT 
            loc.LocationID,
            JSON_QUERY(
                (
                    SELECT 
                        MIN(loc.georange) AS georange,
                        MAX(CAST(loc.rangeinkm AS INT)) AS rangeinkm,
                        MIN(gl_inner.Latitude) AS Latitude,
                        MIN(gl_inner.Longitude) AS Longitude,
                        MIN(gl_inner.LocationAlias) AS LocationAlias,
                        -- Nested DateRanges array
                        (
                            SELECT 
                                MIN(SingleDate) AS RangeStart,
                                MAX(SingleDate) AS RangeEnd
                            FROM (
                                SELECT 
                                    SingleDate,
                                    SUM(DateChange) OVER (PARTITION BY LocationID ORDER BY SingleDate ROWS UNBOUNDED PRECEDING) AS DateGroup
                                FROM (
                                    SELECT 
                                        SingleDate,
                                        LocationID,
                                        CASE 
                                            WHEN SingleDate > DATEADD(day, 1, LAG(SingleDate) OVER (PARTITION BY LocationID ORDER BY SingleDate)) THEN 1
                                            WHEN LAG(LocationID) OVER (PARTITION BY LocationID ORDER BY SingleDate) IS NULL THEN 1
                                            ELSE 0
                                        END AS DateChange
                                    FROM (
                                        -- Select distinct dates for this LocationID after resolving overlaps
                                        SELECT DISTINCT
                                            dates.SingleDate,
                                            resolved.LocationID
                                        FROM (
                                            -- Generate all dates for all LocationID values
                                            SELECT 
                                                DATEADD(day, n.number, gg_inner.FromDate) AS SingleDate,
                                                gg_inner.LocationID,
                                                gg_inner.EmployeeCode,
                                                gg_inner.georange,
                                                gg_inner.rangeinkm
                                            FROM GeoConfig.EmployeesLocationMapping gg_inner
                                            CROSS APPLY (
                                                SELECT TOP (DATEDIFF(day, gg_inner.FromDate, gg_inner.ToDate) + 1)
                                                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS number
                                                FROM master.dbo.spt_values
                                            ) n
                                            WHERE gg_inner.EmployeeCode = re.ed_empcode
                                        ) dates
                                        INNER JOIN (
                                            -- Resolve overlaps by selecting the highest LocationID for each date
                                            SELECT 
                                                dates_inner.SingleDate,
                                                MAX(dates_inner.LocationID) AS LocationID
                                            FROM (
                                                SELECT 
                                                    DATEADD(day, n_inner.number, gg_inner_inner.FromDate) AS SingleDate,
                                                    gg_inner_inner.LocationID,
                                                    gg_inner_inner.EmployeeCode
                                                FROM GeoConfig.EmployeesLocationMapping gg_inner_inner
                                                CROSS APPLY (
                                                    SELECT TOP (DATEDIFF(day, gg_inner_inner.FromDate, gg_inner_inner.ToDate) + 1)
                                                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS number
                                                    FROM master.dbo.spt_values
                                                ) n_inner
                                                WHERE gg_inner_inner.EmployeeCode = re.ed_empcode
                                            ) dates_inner
                                            GROUP BY dates_inner.SingleDate
                                        ) resolved ON dates.SingleDate = resolved.SingleDate
                                        WHERE resolved.LocationID = loc.LocationID
                                    ) distinct_dates
                                ) date_changes
                            ) grouped_dates
                            GROUP BY LocationID, DateGroup
                            FOR JSON PATH
                        ) AS DateRanges
                    FROM GeoConfig.EmployeesLocationMapping loc
                    INNER JOIN GeoConfig.GeoConfigurationLocationMst gl_inner
                        ON loc.LocationID = gl_inner.ID
                    WHERE loc.LocationID = loc.LocationID
                    AND loc.EmployeeCode = re.ed_empcode
                    GROUP BY loc.LocationID
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                )
            ) AS LocationDetails
        FROM (
            SELECT DISTINCT LocationID
            FROM GeoConfig.EmployeesLocationMapping
            WHERE EmployeeCode = re.ed_empcode
        ) loc
        FOR JSON PATH
    ) AS LocationDetails,

    -- IP Range JSON (unchanged)
    (
        SELECT 
            geoip.IPFrom,
            geoip.IPTo
        FROM GeoConfig.GeoConfigurationIPMaster geoip  
        WHERE geoip.GeoConfigurationID IN 
        (
            SELECT DISTINCT gl_sub.ID
            FROM GeoConfig.GeoConfigurationLocationMst gl_sub
            INNER JOIN GeoConfig.EmployeesLocationMapping gg_sub
                ON gl_sub.ID = gg_sub.LocationID
            WHERE gg_sub.EmployeeCode = re.ed_empcode
        )
        FOR JSON PATH
    ) AS IPRange

FROM reqrec_employeedetails AS re
INNER JOIN dbo.SETUP_EMPLOYEESTATUSMST AS se 
    ON re.ED_Status = se.ESM_EmpStatusID

-- Compute boolean flags (unchanged)
CROSS APPLY (
    SELECT 
        CASE WHEN MAX(CAST(gl.IPCheckEnabled AS INT)) = 1 THEN 'true' ELSE 'false' END AS IPCheckEnabled,
        CASE WHEN MAX(CAST(gl.LocationCheckEnabled AS INT)) = 1 THEN 'true' ELSE 'false' END AS LocationCheckEnabled,
        CASE WHEN MAX(CAST(gl.IPCheckEnabledOnMobile AS INT)) = 1 THEN 'true' ELSE 'false' END AS IPCheckEnabledOnMobile,
        CASE WHEN MAX(CAST(el.PunchIn AS INT)) = 1 THEN 'true' ELSE 'false' END AS PunchIn,
        CASE WHEN MAX(CAST(el.PunchOut AS INT)) = 1 THEN 'true' ELSE 'false' END AS PunchOut
    FROM GeoConfig.GeoConfigurationLocationMst gl
    INNER JOIN GeoConfig.EmployeesLocationMapping el 
        ON gl.ID = el.LocationId
    WHERE el.EmployeeCode = re.ed_empcode
) AS gc_bool

WHERE EXISTS (
    SELECT 1
    FROM tna.Rostering AS ro
    WHERE ro.EmpCode = re.ed_empcode
)
ORDER BY re.ed_empcode;
