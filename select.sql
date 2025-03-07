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
 
    -- Shift details JSON with Break details merged
    (
        SELECT
            ro_inner.ShiftID,
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
            MIN(sbrk.BreakInTime) AS BreakInTime,  
            MAX(sbrk.BreakOutTime) AS BreakOutTime,
            MAX(sbrk.TotalBreakMinutes) AS TotalBreakMinutes,
            MIN(CAST(sbrk.Applicable AS TINYINT)) AS Applicable
        FROM tna.Rostering AS ro_inner
        INNER JOIN tna.ShiftMst AS sht_inner
            ON ro_inner.ShiftId = sht_inner.ShiftId
        inner JOIN tna.ShiftBreak AS sbrk  
            ON sht_inner.ShiftId = sbrk.ShiftId
        INNER JOIN (
            SELECT
                ShiftID,
                MIN(ShiftStart) AS ShiftStart,
                MAX(ShiftEnd) AS ShiftEnd
            FROM (
                SELECT
                    ro.ShiftID,
                    ro.Date AS ShiftStart,
                    LEAD(ro.Date) OVER (PARTITION BY ro.EmpCode ORDER BY ro.Date) AS ShiftEnd,
                    LAG(ro.ShiftID) OVER (PARTITION BY ro.EmpCode ORDER BY ro.Date) AS PrevShiftID
                FROM tna.Rostering ro
                WHERE ro.EmpCode = re.ed_empcode
            ) AS ShiftBlocksInner
            WHERE PrevShiftID IS NULL OR PrevShiftID <> ShiftID
            GROUP BY ShiftID
        ) AS ShiftBlocks ON ro_inner.ShiftID = ShiftBlocks.ShiftID
        WHERE ro_inner.EmpCode = re.ed_empcode
        GROUP BY ro_inner.ShiftID
        FOR JSON PATH
    ) AS ShiftDetails,
 
    -- Location details JSON
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
 
    -- IP Range JSON
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
 
-- Compute boolean flags
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
) AS gc_bool
 
WHERE EXISTS (
    SELECT 1
    FROM tna.Rostering AS ro
    WHERE ro.EmpCode = re.ed_empcode
)
ORDER BY re.ed_empcode;
