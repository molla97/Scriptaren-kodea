


#Function call
    #loadScript -scriptName "DBQuery.ps1" -scriptFolder "std\"
    . D:\SysAppl\Scripts\std\DBQuery.ps1

    #loadScript -scriptName "MailSender.ps1" -scriptFolder "std\"
    . D:\SysAppl\Scripts\std\MailSender.ps1

#Global variable
    $global:colorHTMLInProgress = "#005fb0"
    $global:colorHTMLSuccess = "#00B050"
    $global:colorHTMLWarning = "#FFAC56"
    $global:colorHTMLError = "#FB9895"
    $global:sqlInstance_ARCServeDB = "arcserve_db"

function ARCServe-getJobs #Get jobs, subjobs and linked sessions
{
    #Parameters
        param([string]$paramSqlHost = "",
            [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_ARCServeDB,
            [Parameter(Mandatory=$false)][string]$paramHostName = $null,
            [Parameter(Mandatory=$false)][string]$paramStatus = $null,
            [Parameter(Mandatory=$false)][string]$paramType = $null,
            [Parameter(Mandatory=$false)][boolean]$paramEndDate = $false,
            [Parameter(Mandatory=$false)][string]$paramSinceDays = $null,
            [Parameter(Mandatory=$false)][DateTime[]]$paramBetween,
            [Parameter(Mandatory=$false)][DateTime]$paramBefore,
            [Parameter(Mandatory=$false)][DateTime]$paramAfter
            )

    #Parameters Variables

    #Variables
        $returnArray = @{}
        $message = ""

        $sqlWhereJobsDate = ""       #SQL Where Date
        $sqlWhereStatus = ""         #SQL Where Status
        $sqlWhereType = ""           #SQL Where Type
        $getDateStartOrEnd = ""
        $getJobsDateDescription = "" #Date criteria description
        
        $jobs = $null                #SQL Dataset Result
            [hashtable]$jobsArray = @{}
            [hashtable]$jobArray = @{}
            $job = $null
            $subJobsList = $null
        $jobsLimit = 100             #Limit job reporting up to xxx

        $sessions = $null            #SQL Dataset Result
            [hashtable]$sessionsArray = @{}
            [hashtable]$sessionArray = @{}
            $session = $null

       
        #Init some variable
            if ($paramEndDate -eq $true) { $getDateStartOrEnd = "endtime" }
            else { $getDateStartOrEnd = "starttime" }

            if (-not [string]::IsNullOrEmpty($paramStatus)) { $sqlWhereStatus = "AND status IN ($paramStatus)" }
            if (-not [string]::IsNullOrEmpty($paramType)) { $sqlWhereType = "AND type IN ($paramType)" }

            if ($paramSinceDays.ToString() -ne "") { $sqlWhereJobsDate = "AND DATEDIFF(day, $getDateStartOrEnd, GETDATE()) < $paramSinceDays"; $getJobsDateDescription = "since last $paramSinceDays day(s)" }
            elseif ($paramBetween -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd between '" + $paramBetween[0] + "' and '" + $paramBetween[1] + "'"; $getJobsDateDescription = "between " + $paramBetween[0] + "' and '" + $paramBetween[1] }
            elseif ($paramBefore -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd <= '" + (Get-Date $paramBefore -format s) + "'"; $getJobsDateDescription = "before " + (Get-Date $paramBefore -format s) }
            elseif ($paramAfter -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd >= '" + (Get-Date $paramAfter -format s) + "'"; $getJobsDateDescription = "since " + (Get-Date $paramAfter -format s) }
            #else {$sqlWhereJobsDate = "DATEDIFF(day, $getDateStartOrEnd, GETDATE()) < 1"; $getJobsDateDescription = "since last day" }

        #Get only Master jobs (no sub jobs)
        $sqlQuery = "SELECT 
                    id AS 'jobId',
                    RTRIM(jhostname) AS 'jobHostName',
                    RTRIM(owner) AS 'jobOwner',
                    RTRIM(comment) AS 'jobComment',
                    status AS 'jobStatus',
                    type  AS 'jobType',
                    jobexectype AS 'jobExecType',
                    throughputMBPerMin AS 'jobBandwidthMB',
                    
                    starttime AS 'jobDateStart',
                    endtime AS 'jobDateStop',
                    CASE
                        WHEN tbl_job.starttime > tbl_job.endtime OR tbl_job.status = 0 THEN
                           NULL
                        ELSE
                            CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_job.starttime, tbl_job.endtime), 0), 8)
                    END AS 'jobWallTime'

                FROM asdb.dbo.asjob tbl_job
                INNER JOIN asdb.dbo.asjobmap tbl_jobMap ON tbl_jobMap.jobid = tbl_job.id
                WHERE tbl_jobMap.masterslaveflag = tbl_jobMap.jobid
                AND tbl_job.jhostname = '$paramHostName'
                $sqlWhereJobsDate
                $sqlWhereStatus
                $sqlWhereType
                ORDER BY starttime DESC
                "
        $jobs = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery

    if ($jobs.Tables[0].Rows.Count -gt $jobsLimit) {
        #Prepare Message if jobs return limit has been reached
            $message = "<h1>" + $jobs.Tables[0].Rows.Count + " jobs has been found $getJobsDateDescription.</h1>"
            $message += "<h2>Display limit is set to $jobsLimit. Please generate another report with more precise search criteria.</h2>"
    }
    else {
        #Prepare Message if jobs found
            if ($jobs.Tables[0].Rows.Count -gt 0) {
                if ($paramEndDate -eq $true) { $message = "<h2>$paramHostName - " + $jobs.Tables[0].Rows.Count + " Master job(s) ended $getJobsDateDescription </h2>" }
                else { $message = "<h2>$paramHostName - " + $jobs.Tables[0].Rows.Count + " Master job(s) In Progress $getJobsDateDescription </h2>" }
                
                $message += "<p style='font-style: italic;'><b>Filtering condition are:</b> WHERE $sqlWhereJobsDate AND tbl_job.jhostname = $paramHostName $sqlWhereStatus $sqlWhereType</p>"
                #Write-Host ($jobs.Tables[0].Rows.values | sort-object -Property jobStatusDescription | Get-Unique)
                #$message += "<h2>" + (($jobs.Tables[0].Rows.Value.jobStatusDescription | sort-object | Get-Unique) -join ", ") + "</h2>"
            }
            else {
        #Prepare Message if no jobs found
                if ($paramStatus -eq 0) { $message = "<h2>$paramHostName - No job(s) In Progress $getJobsDateDescription</h2>" }
                else { $message = "<h2>$paramHostName - No job(s) ended $getJobsDateDescription</h2>" }
            }

        #Iterate each jobs
        foreach ($job in $jobs.Tables[0].Rows)
        {
            $jobArray = @{}
            $subJobsList = $null
            $sessionsArray = @{}
            $sessions = $null
            
            $jobArray.add("jobHostName", $job.jobHostName)
            $jobArray.add("jobId", $job.jobId)
            $jobArray.add("jobType", $job.jobType)
            switch ($job.jobType) {
                1 { $jobArray.add("jobTypeDescription", "Backup"); break }                     #1 THEN 'Backup'
                3 { $jobArray.add("jobTypeDescription", "Backup (GFS Rotation)"); break }      #3 THEN 'Backup (GFS Rotation)'
                5 { $jobArray.add("jobTypeDescription", "Backup (GFS Makeup)"); break }        #5 THEN 'Backup (GFS Makeup)'
                7 { $jobArray.add("jobTypeDescription", "Restore"); break }                    #7 THEN 'Restore'
                8 { $jobArray.add("jobTypeDescription", "Merge"); break }                      #8 THEN 'Merge'
                12 { $jobArray.add("jobTypeDescription", "Count"); break }                     #12 THEN 'Count'
                18 { $jobArray.add("jobTypeDescription", "Prune Database"); break }            #18 THEN 'Prune Database'
                20 { $jobArray.add("jobTypeDescription", "Backup (Makeup)"); break }           #20 THEN 'Backup (Makeup)'
                21 { $jobArray.add("jobTypeDescription", "Backup (Rotation)"); break }         #21 THEN 'Backup (Rotation)'
                45 { $jobArray.add("jobTypeDescription", "Device Management"); break }         #45 THEN 'Device Management'
                46 { $jobArray.add("jobTypeDescription", "Generic Job"); break }               #46 THEN 'Generic Job'
                default { $jobArray.add("jobTypeDescription", "Undefined job type"); break }           #tbl_job.type
            }
            switch ($job.jobExecType) {
                1 { $jobArray.add("jobTypeExecution", "Automatically (scheduled)"); break }
                4 { $jobArray.add("jobTypeExecution", "Manually"); break }
                default { $jobArray.add("jobTypeExecution", "Manually"); break } 
            }
            $jobArray.add("jobDescription", $job.jobComment)
            if ([string]::IsNullOrWhiteSpace($job.jobOwner)) {$jobArray.add("jobOwner", "ARCserve")} else {$jobArray.add("jobOwner", $job.jobOwner)}
            $jobArray.add("jobDateStart", $job.jobDateStart)
            $jobArray.add("jobDateStop", $job.jobDateStop)
            $jobArray.add("jobWallTime", $job.jobWallTime)
            $jobArray.add("jobBandwidthMB", $job.jobBandwidthMB)

            $jobArray.add("jobStatusOrigin", $job.jobStatus)
            switch ($job.jobStatus) {
                0 { $jobArray.add("jobStatus",4);
                    $jobArray.add("jobStatusDescription", "In Progress");
                    $jobArray.add("jobStatusColor", $colorHTMLInProgress); break }
                1 { $jobArray.add("jobStatus",0);
                    $jobArray.add("jobStatusDescription", "Finished");
                    $jobArray.add("jobStatusColor", $colorHTMLSuccess); break }
                2 { $jobArray.add("jobStatus",3);
                    $jobArray.add("jobStatusDescription", "Cancelled");
                    $jobArray.add("jobStatusColor", $colorHTMLWarning); break }
                3 { $jobArray.add("jobStatus",2);
                    $jobArray.add("jobStatusDescription", "Failed");
                    $jobArray.add("jobStatusColor", $colorHTMLError); break }
                4 { $jobArray.add("jobStatus",1);
                    $jobArray.add("jobStatusDescription", "Warning"); #Arcserve consider this Status code for "Incomplete".
                    $jobArray.add("jobStatusColor", $colorHTMLWarning); break }
                default { $jobArray.add("jobStatus",99); $jobArray.add("jobStatusDescription", "Other : "+$job.jobStatus); break }
            }

        #Get job logs
            $logs = (ARCServe-GetJobLogs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -jobId $job.jobId -maxSeverity 16).Tables[0].Rows
            $jobArray.add("logs", $logs)
 
        #Get subjobs
            $subJobsList = ((ARCServe-GetSubJobs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -masterJobId $job.jobId).Tables[0].Rows | Select -ExpandProperty jobid)
            $jobArray.add("jobCountSubjob", $subJobsList.Length)
            $subJobsList =  $subJobsList -join ", "
            
        #Get linked sessions
            $SqlQuery = "SELECT

                tbl_path.path AS 'jobSourceHost',
                tbl_pathw.path AS 'jobSourcePath',
       
                tbl_tapeSes.id AS 'sessionId',
                tbl_tapeSes.jobid AS 'jobId',
                tbl_tapeSes.status AS 'sessionStatus',
                tbl_job.comment AS 'jobDescription',

                CASE 
                    WHEN tbl_tapeSes.tapeid IS NULL THEN
                        NULL
                    ELSE
                        CONCAT(RTRIM(tbl_tape.tapename), ',', '<br>', 'Serial ', RTRIM(tbl_tape.serialnum), ' (ID: ', tbl_tapeSes.tapeid, ')')
                END AS 'sessionTapeName',
        
                tbl_tapeSes.starttime AS 'sessionDateStart',
                tbl_tapeSes.endtime AS 'sessionDateStop',
                CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_tapeSes.starttime, tbl_tapeSes.endtime), 0), 8) AS 'sessionElapsedTime',

                tbl_tapeSes.totalkb AS 'sessionBackupedDatasKB',
                tbl_tapeSes.sizeOnTapeKB AS 'sessionBackupedSizeOnTapeKB',
                tbl_tapeSes.totalfiles AS 'sessionBackupedFiles',
                tbl_tapeSes.totalmissed AS 'sessionMissedFiles',
                tbl_tapeSes.ThroughputMBPerMin AS 'sessionBandwithMB'
            
                FROM asdb.dbo.astpses tbl_tapeSes
                LEFT JOIN asdb.dbo.asjob tbl_job ON tbl_tapeSes.jobid = tbl_job.id
                LEFT JOIN aspath.dbo.aspathname tbl_path ON tbl_tapeSes.srchostid = tbl_path.id
                LEFT JOIN aspath.dbo.aspathnamew tbl_pathw ON tbl_tapeSes.srcpathid = tbl_pathw.id
                LEFT JOIN asdb.dbo.ashost tbl_host ON tbl_tapeSes.srchostid = tbl_host.rhostid
                LEFT JOIN asdb.dbo.astape tbl_tape ON tbl_tapeSes.tapeid = tbl_tape.id

                WHERE tbl_tapeSes.jobid = " + $job.jobId + "
                OR tbl_tapeSes.jobid IN (" + $subJobsList + ") 
            
                ORDER BY ( ( POWER(2.0, 32) + tbl_tapeSes.totalkb ) % POWER(2.0, 32) ) / 1024 ASC, tbl_path.path DESC, tbl_pathw.path DESC" #Sorting must be reverted (reversed when printed)
                #ORDER BY tbl_tapeSes.totalkb DESC, tbl_path.path DESC, tbl_pathw.path DESC"
            $sessions = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery
            
        #Iterate linked sessions
            foreach ($session in $sessions.Tables[0].Rows)
            {
                $sessionArray = @{}

                $sessionArray.add("jobStatusOrigin", $session.sessionStatus)

                switch ($session.sessionStatus) {
                    0 { $sessionArray.add("jobStatus",4);
                        $sessionArray.add("jobStatusDescription", "In Progress");
                        $sessionArray.add("jobStatusColor", $colorHTMLInProgress);
                        $sessionArray.add("ip", 1); break }
                    1 { $sessionArray.add("jobStatus",0);
                        $sessionArray.add("jobStatusDescription", "Finished");
                        $sessionArray.add("jobStatusColor", $colorHTMLSuccess);
                        $sessionArray.add("success", 1); break }
                    2 { $sessionArray.add("jobStatus",3);
                        $sessionArray.add("jobStatusDescription", "Cancelled");
                        $sessionArray.add("jobStatusColor", $colorHTMLWarning);
                        $sessionArray.add("warning", 1); break }
                    3 { $sessionArray.add("jobStatus",2);
                        $sessionArray.add("jobStatusDescription", "Failed");
                        $sessionArray.add("jobStatusColor", $colorHTMLError);
                        $sessionArray.add("error", 1); break }
                    4 { $sessionArray.add("jobStatus",1);
                        $sessionArray.add("jobStatusDescription", "Warning"); #Arcserve consider this Status code for "Incomplete".
                        $sessionArray.add("jobStatusColor", $colorHTMLWarning);
                        $sessionArray.add("warning", 1); break }
                    default { $sessionArray.add("jobStatus",99); $sessionArray.add("jobStatusDescription", "Other : "+$session.sessionStatus); break }
                }
                
                $sessionArray.add("sessionId", $session.sessionId)
                $sessionArray.add("jobId", $session.jobId)
                $sessionArray.add("jobDescription", $session.jobDescription)
                $sessionArray.add("jobSourceHost", $session.jobSourceHost.Replace('\', '').Split(" ")[0])
                if (-not [string]::IsNullOrWhiteSpace($session.jobSourcePath)) {$sessionArray.add("jobSourcePath", $session.jobSourcePath.Replace('@', '<br>'))}
                $sessionArray.add("sessionDateStart", $session.sessionDateStart)
                $sessionArray.add("sessionDateStop", $session.sessionDateStop)
                $sessionArray.add("sessionElapsedTime", $session.sessionElapsedTime)
                $sessionArray.add("sessionTapeName", $session.sessionTapeName)
                $sessionArray.add("sessionBackupedDatasMB", (([math]::pow(2, 32) + $session.sessionBackupedDatasKB) % [math]::pow(2, 32)/1024))
                if ($session.sessionBackupedDatasKB -gt 0) {$sessionArray.add("sessionCompressionRatio", ($session.sessionBackupedSizeOnTapeKB/$session.sessionBackupedDatasKB))}
                $sessionArray.add("sessionBackupedFiles", $session.sessionBackupedFiles)
                $sessionArray.add("sessionMissedFiles", $session.sessionMissedFiles)
                $sessionArray.add("sessionBandwithMB", $session.sessionBandwithMB)
                $sessionArray.add("sessionDescription", $session.sessionTapeName)

                $sessionsArray.add($sessionsArray.Count, $sessionArray)
            }

            $jobArray.add("globalBackupedFile", ($sessionsArray.Values.sessionBackupedFiles | Measure-Object -Sum).Sum)
            $jobArray.add("globalMissedFile", ($sessionsArray.Values.sessionMissedFiles | Measure-Object -Sum).Sum)
            $jobArray.add("globalBackupedDataMB", ($sessionsArray.Values.sessionBackupedDatasMB | Measure-Object -Sum).Sum)
            $jobArray.add("globalcountIp", ($sessionsArray.Values.ip | Measure-Object -Sum).Sum)
            $jobArray.add("globalcountSuccess", ($sessionsArray.Values.success | Measure-Object -Sum).Sum)
            $jobArray.add("globalcountWarning", ($sessionsArray.Values.warning | Measure-Object -Sum).Sum)
            $jobArray.add("globalcountError", ($sessionsArray.Values.error | Measure-Object -Sum).Sum)

            #Add sessions to job table
                $jobArray.add("sessions", $sessionsArray)

            #Add job table to jobs table
                $jobsArray.add($jobsArray.Count, $jobArray)
        } #End for each jobs enumeration

    } #End if jobs quantity exceed fixed jobs limit

    #Return values init
        $returnArray.add("hostName", $paramHostname)
        $returnArray.add("message", $message)
        $returnArray.add("jobs", $jobsArray)

    Return $returnArray
}

function ARCServe-GetSubJobs
{
    #Parameters
        param([string]$paramSqlHost = "",
            [string]$paramSqlInstance = $sqlInstance_ARCServeDB,
            [int]$masterJobId = 0)
    
    $sqlQuery = "SELECT jobid FROM asdb.dbo.asjobmap
                WHERE masterslaveflag = $masterJobId AND jobid != $masterJobId
                ORDER BY jobid ASC"
    
    Return Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlQuery $sqlQuery
}

function ARCServe-GetJobLogs
{
    #Parameters
        param([string]$paramSqlHost = "",
            [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_ARCServeDB,
            [int]$jobId = 0,
            [Parameter(Mandatory=$false)][int]$maxSeverity)
    
    $sqlQuery = "SELECT logtime, msgtext, msgtypeid, severity, CONCAT(FORMAT(logtime, 'dd/MM/yy HH:mm:ss'), ' - ', msgtext) AS fullLog FROM aslog.dbo.aslogw
                WHERE jobid = $jobId AND severity <= $maxSeverity
                ORDER BY logtime ASC"
    
    Return Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlQuery $sqlQuery
}

