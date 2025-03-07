SELECT top 100 
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

    -- Create a nested JSON object for shift details
    (
        SELECT ShiftID,
            (
                SELECT 
                    MIN(sht_inner.ShiftName) AS ShiftName,
                    MIN(sht_inner.InTime) AS InTime,
                    MAX(sht_inner.OutTime) AS OutTime,
                    MAX(sht_inner.TotalMinutes) AS TotalMinutes,
                    --MIN(sht_inner.SwipesSeperatorParam) AS SwipesSeperatorParam,
                    MIN(CAST(sht_inner.ISWorkBtwnShifttime AS TINYINT)) AS ISWorkBtwnShifttime,
                    MIN(CAST(sht_inner.IsBreakApplicable AS TINYINT)) AS IsBreakApplicable,
                    MIN(CAST(sht_inner.IsNightShiftApplicable AS TINYINT)) AS IsNightShiftApplicable,
                    MIN(CAST(sht_inner.IsActive AS TINYINT)) AS IsActive,
                    MIN(sht_inner.AutoShift) AS AutoShift,
                    MIN(CAST(sht_inner.ShiftAllowance AS TINYINT)) AS ShiftAllowance,
                     --Include shift date ranges as a nested array
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
                                -- This gets unique dates with assigned shifts and groups them
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
                                        -- Assign each date to a single shift
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
                                WHERE t.ShiftID = shifts.ShiftID -- Filter for the current shift
                            ) sr
                        ) grouped
                        GROUP BY ShiftID, ShiftGroup
                        FOR JSON PATH
                    ) AS ShiftRanges
                FROM tna.ShiftMst AS sht_inner
                WHERE sht_inner.ShiftId = shifts.ShiftID
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ) AS ShiftDetails
        FROM (
            -- Get distinct shift IDs for this employee
            SELECT DISTINCT ShiftID
            FROM tna.Rostering
            WHERE EmpCode = re.ed_empcode
        ) shifts
        FOR JSON PATH
    ) AS ShiftDetails,

    -- Location details JSON (unchanged)
    (
        SELECT 
            gg.LocationID,
            MIN(gg.georange) AS georange,
            MAX(CAST(gg.rangeinkm AS INT)) AS rangeinkm,
            MIN(gl.Latitude) AS Latitude,
            MIN(gl.Longitude) AS Longitude,
            MIN(gg.FromDate) AS FromDate,
            MIN(gg.ToDate) AS ToDate,
            MIN(gl.LocationAlias) AS LocationAlias
        FROM tna.Rostering AS ro_loc
        INNER JOIN GeoConfig.EmployeesLocationMapping AS gg 
            ON ro_loc.EmpCode = gg.EmployeeCode
        INNER JOIN GeoConfig.GeoConfigurationLocationMst gl
            ON gg.LocationID = gl.ID
        WHERE ro_loc.EmpCode = re.ed_empcode
        GROUP BY gg.LocationID
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
