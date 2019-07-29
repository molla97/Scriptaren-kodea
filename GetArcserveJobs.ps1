########################################
# Name: Get ARCServe jobs
# Current version : 2.2
# Current Release date : 03/06/2018
# Version History : 2.1 -> New version (2.2) Some updates (sorting by MB backuped, title size, new mail subject, Brand parameter, LYO and RUE PROD)
# Version History : 2.0 of 03/03/2018 -> First Prod version. New version (2.1) Title and text message change
# Version History : 1.1 of 15/01/2018 -> First Beta version. New version (2.0) has been fully transformed
# Author : Ibermática (Vincent Seres)
#
# Usage: This script contain some functions to connects ARCServe DBs and and returns the list of Jobs
#    ARCServe-getJobs : Return a list of jobs, subjobs and sessions (with logs and all important infos) base on some filters (status, type and date)
#        - Return Value : Return a System.Data.DataSet Object
#    ARCServe-GetErrorJobsID : Return a etor of unsuccessful backups jobs ID / End date (Failed, Cancelled, Incomplete).
# 
# 
# Execution: Must be executed with user having rights on ARCServe Database.
########################################
   
function loadScript
{
    #Parameters
        param([Parameter(Mandatory=$true)][string]$scriptName = "",
            [Parameter(Mandatory=$false)][string]$scriptFolder = "")
 
    $rootFolder = "Scripts\"

    if ([string]::IsNullOrEmpty($scriptFolder)) {
        $scriptFolder = (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)
    }
    else {
        $cmd = $PSScriptRoot
        #$cmd = (split-path -parent $MyInvocation.MyCommand.Definition)
        $scriptFolder = $cmd.ToString().Substring(0, $cmd.ToString().lastIndexOf($rootFolder)) + $rootFolder + $scriptFolder
    }
    
    try {
        Write-Host -NoNewline "Loading $scriptFolder$scriptName ..."
        . ($scriptFolder+$scriptName)
        Write-Host "Loaded."
    }
    catch {
        
        $ErrorMessage = $_.Exception.Message
        $FailedItem = $_.Exception.ItemName

        Write-Host "Error"
        Write-Host $ErrorMessageProgram
        Write-Host $FailedItem
        Write-Host "Program will exit." 
        Break
    }
}

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

function ARCServe-getCurrentJobsStatus #Get jobs last status
{
    #Parameters
        param([string]$paramSqlHost = "",
        [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_ARCServeDB)

    $sqlQuery = "SELECT
                    tbl_job.jhostname AS 'Hostname',

                    CASE 
                        WHEN tbl_job.status = 0 THEN 'In Progres'
                        WHEN tbl_job.status = 1 THEN 'Finished'
                        WHEN tbl_job.status = 2 THEN 'Cancelled'
                        WHEN tbl_job.status = 3 THEN 'Failed'
                        WHEN tbl_job.status = 4 THEN 'Incomplete'
                    END AS 'Status',
                    tbl_job.status AS 'StatusID',
                    
                    tbl_job.id  AS 'JobID',
                    tbl_job.comment  AS 'Comment',
                    tbl_job.endtime  AS 'EndTime'

                FROM ( SELECT *, ROW_NUMBER() OVER (
				                PARTITION BY no
				                ORDER BY starttime DESC ) AS [RowNumber]
	                FROM asdb.dbo.asjob tbl_jobs
                    INNER JOIN asdb.dbo.asjobmap tbl_jobMap ON tbl_jobMap.jobid = tbl_jobs.id
                    WHERE tbl_jobMap.masterslaveflag = tbl_jobMap.jobid
                ) AS tbl_job
                WHERE tbl_job.[RowNumber] = 1
                AND tbl_job.EndTime > GETDATE()-7"
    
    Return Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlQuery $sqlQuery

}

function ARCServe-checkCentreon
{
    #Parameters
        param([string]$paramServer = "")

    $jobsStatus = ARCServe-getCurrentJobsStatus -paramSqlHost $paramServer

    $centronExitCode = 3
    $centreonMessage = ""

    if($jobsStatus)
    {
        foreach ($jobStatus in $jobsStatus.Tables[0].Rows)
        {
            if ($jobStatus.StatusID -le 1)  {
                $centreonMessage += "Job " + $jobStatus.JobID.ToString() + " = OK`n`r"
                $centronExitCode = 0
            }
            if ($jobStatus.StatusID -eq 2)  {
                $centreonMessage += "Job " + $jobStatus.JobID.ToString() + " = Cancelled`n`r"
                if ($centronExitCode -lt 1) { $centronExitCode = 1 }
            }
            elseif ($jobStatus.StatusID -eq 3) {
                $centreonMessage += "Job " + $jobStatus.JobID.ToString() + " = Error during: " + $jobStatus.Comment.Trim() + " finished on: " + $jobStatus.EndTime + "`n`r"
                if ($centronExitCode -lt 2) { $centronExitCode = 2 }
 
            }
            elseif ($jobStatus.StatusID -eq 4) {
                $centreonMessage += "Job " + $jobStatus.JobID.ToString() + " = Warning during: "  + $jobStatus.Comment.Trim() + " finished on: " + $jobStatus.EndTime + "`n`r"
                if ($centronExitCode -lt 1) { $centronExitCode = 1 }
            }        
        }
    }
    else
    {
        $centronExitCode = 3
        $centreonMessage = "Error getting Script status. Please contact script developper"
    }

    echo $centreonMessage
    Exit $centronExitCode
}

function ARCServe-sendMailJobsSummary
{
    #Parameters
        param([string]$emailFrom = "", [string]$emailTo = "", [hashtable]$jobsContainer = $null)
       
        if ([string]::IsNullOrEmpty($emailFrom)) {
            Write-Host "ERROR on ARCServe-sendMailJobsSummary script : 'emailFrom' parameter is Empty."
            Exit
        }
        if ([string]::IsNullOrEmpty($emailTo)) {
            Write-Host "ERROR on ARCServe-sendMailJobsSummary script : 'emailTo' parameter is Empty."
            Exit
        }
        if ($jobsContainer -eq $null) {
            Write-Host "ERROR on ARCServe-sendMailJobsSummary script : 'jobsContainer' parameter is Empty."
            Exit
        }
        
        if ([string]::IsNullOrEmpty($jobsContainer.branch)) {
            $jobsContainer.branch = $jobsContainer.hostName
        }

        #Variable 
            $sumIP = 0
            $sumSuccess = 0
            $sumWarning = 0
            $sumError = 0
            $sumTotal = 0

            $subject = ""
            $body = ""
            $jobs = $null
            $job = $null
            $hostNames = New-Object System.Collections.ArrayList

        foreach ($jobs in $jobsContainer.GetEnumerator() | Sort-Object -Property Name) #-descending 
        {
            #Append header (title or error message)
                $body += $jobs.Value.message
                $hostNames.add($jobs.Value.hostName) > $null

            #Append Mail body
                if ($jobs.Value.jobs.Count -gt 0)
                {
                    foreach ($job in $jobs.Value.jobs.GetEnumerator() | Sort-Object -Property Name) #-descending 
                    {
                        $body += ARCServe-formatJobSummary -job $job.Value
                        
                        switch ($job.Value.jobStatus) {
                            0 { $sumIP += 1; break }
                            1 { $sumSuccess += 1; break }
                            2 { $sumWarning += 1; break }
                            3 { $sumError += 1; break }
                            4 { $sumWarning += 1; break }
                        }
                    }
                }
        }

        $sumTotal += $sumIP + $sumSuccess + $sumWarning + $sumError

        Write-Host "sumTotal: " $sumTotal
        Write-Host "sumIP: " $sumIP
        Write-Host "sumSuccess: " $sumSuccess
        Write-Host "sumWarning: " $sumWarning
        Write-Host "sumError: " $sumError

        #Append footer
            $body += '<table style="width:100%">
                        <tbody>
                            <tr style="color:#626365; vertical-align:middle; font-family:Tahoma; font-size:12px;">
		                        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">ARCServe Backup 17.5</td>
		                    </tr>
                        </tbody>
                    </table>'

        #Prepare mail subject
            $subject = "[BKP " + $jobsContainer.branch + "]"
            if ($sumError -gt 0) { $subject += " Error on $sumError jobs" }
            elseif ($sumWarning -gt 0) { $subject += " Warning on $sumWarning jobs" }
            else { $subject += " All Success" }
            $subject += " - " + ($sumSuccess + $sumIP) + "/$sumTotal Jobs OK on " + ($hostNames | Get-Unique)
       
        #Send amil
            Mail-SendInternalBrandtMail -emailFrom $emailFrom -emailTo $emailTo -subject $subject -body $body
}

function ARCServe-sendMailReportBackup
{
    param([string]$paramSqlHost = "",
        [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_ARCServeDB,
        [Parameter(Mandatory=$false)][string]$paramHostName = $null,
        [Parameter(Mandatory=$false)][string]$paramBranch = $null,
        [string]$paramEmailFrom = "",
        [string]$paramEmailTo = "")
       
        if ([string]::IsNullOrEmpty($paramEmailFrom)) {
            Write-Host "ERROR on ARCServe-sendMailReportBackup script : 'paramEmailFrom' parameter is Empty."
            Exit
        }
        if ([string]::IsNullOrEmpty($paramEmailTo)) {
            Write-Host "ERROR on ARCServe-sendMailReportBackup script : 'paramEmailTo' parameter is Empty."
            Exit
        }

    #Variable
        $previousValues = $null

        $jobsInProgress = $null
        $jobsFinished = $null
        $jobsContainer = @{}

        $scriptName = (Get-Item $PSCommandPath ).Basename #$MyInvocation.ScriptName
        $scriptCommandName = $MyInvocation.MyCommand.Name #(Get-Item $PSCommandPath ).Name
        $scriptAbsPath = (Get-Item $PSCommandPath ).DirectoryName #$PSScriptRoot #$MyInvocation.MyCommand.Path #Get-Location 

        $datPath = "$scriptAbsPath\$scriptName\$paramSqlHost-$scriptCommandName.dat"

    #Check path and file and create them as needed
        if (!(Test-Path "$scriptAbsPath\$scriptName\")) { New-Item -path $scriptAbsPath -name "$scriptName\" -type "directory" }
        if (!(Test-Path $datPath)) { New-Item -path "$scriptAbsPath\$scriptName" -name "$paramSqlHost-$scriptCommandName.dat" -type "file" }

    #Get last execution values
        $previousValues = Get-Content $datPath | ConvertFrom-StringData
        if ($previousValues.LastScriptExecution -eq $null) { $previousValues = @{}; $previousValues.add("LastScriptExecution", (Get-Date).AddDays(-1)) }

    #Retrieve backups jobs (Jobs In Progress [Status = 0] and Jobs Ended after last execution date)
        $jobsInProgress = ARCServe-getJobs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -paramHostName $paramHostName -paramStatus '0' -paramType '1, 3, 5, 20, 21'
        $jobsContainer.add($jobsContainer.Count, $jobsInProgress)

        $jobsFinished = ARCServe-getJobs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -paramHostName $paramHostName -paramEndDate $true -paramAfter $previousValues.LastScriptExecution -paramType '1, 3, 5, 20, 21'
        $jobsContainer.add($jobsContainer.Count, $jobsFinished) 
        
        $jobsContainer.add("branch", $paramBranch)

    #Update DAT file with current execution values
        "LastScriptExecution=" + (Get-Date) + "`r`n" > $datPath

    #if ($jobs.jobs.Count -gt 0) {
        ARCServe-sendMailJobsSummary -emailFrom $paramEmailFrom -emailTo $paramEmailTo -jobsContainer $jobsContainer
    #}
}

function ARCServe-sendMailIncidentBackup
{
    param([string]$paramSqlHost = "",
        [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_ARCServeDB,
        [Parameter(Mandatory=$false)][string]$paramHostName = $null,
        [string]$paramEmailFrom = "",
        [string]$paramEmailTo = "")
       
        if ([string]::IsNullOrEmpty($paramEmailFrom)) {
            Write-Host "ERROR on ARCServe-sendMailReportBackup script : 'paramEmailFrom' parameter is Empty."
            Exit
        }
        if ([string]::IsNullOrEmpty($paramEmailTo)) {
            Write-Host "ERROR on ARCServe-sendMailReportBackup script : 'paramEmailTo' parameter is Empty."
            Exit
        }

    #Variable
        $log = ""
        $previousValues = $null
 
        $jobs = $null
        $jobsContainer = @{}

        $currentDate = Get-Date -Format u
        $scriptName =  (Get-Item $PSCommandPath ).Basename #$MyInvocation.ScriptName
        $scriptFullName = $scriptName + (Get-Item $PSCommandPath ).Extension
        $scriptCommandName = $MyInvocation.MyCommand.Name #(Get-Item $PSCommandPath ).Name
        $scriptAbsPath = (Get-Item $PSCommandPath ).DirectoryName #$PSScriptRoot #$MyInvocation.MyCommand.Path #Get-Location 

        $datPath = "$scriptAbsPath\$scriptName.dat"
        $logPath = "$scriptAbsPath\$scriptName.log"

    #Check path and file and create them as needed
        if (!(Test-Path "$scriptAbsPath\$scriptName\")) { New-Item -path $scriptAbsPath -name "$scriptName\" -type "directory" }
        if (!(Test-Path $datPath)) { New-Item -path "$scriptAbsPath\$scriptName" -name "$scriptCommandName.dat" -type "file" }
        if (!(Test-Path $logPath)) { New-Item -path "$scriptAbsPath\$scriptName" -name "$scriptCommandName.log" -type "file" }

    #Get last execution vales
        $previousValues = Get-Content $datPath | ConvertFrom-StringData

    #Retrieve backups jobs with problem (2 = Cancelled, 3 = Failed, 4 = Incomplete)
        $jobs = ARCServe-getJobs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -paramHostName BTFFSPLYO01 -paramEndDate $true -paramAfter $previousValues.LastScriptExecution -paramStatus '2, 3, 4' -paramType '1, 3, 5, 20, 21'
        $jobsContainer.Add($jobsContainer.Count, $jobs)

    #Update DAT file with current execution values
        "LastScriptExecution=" + (Get-Date) + "`r`n" > $datPath
        #"LastFailedJobExecution=" + $consideredData.lastFailedJobExecution >> $datPath

    #Send Mail to Ticket system
        if ($jobs.jobs.Count -gt 0) {
            ARCServe-sendMailJobsSummary -emailFrom $paramEmailFrom -emailTo $paramEmailTo -jobsContainer $jobsContainer
        }

    #Log    
        $log = "/**** Script executed on $env:ComputerName by $env:UserDomain\$env:UserName at $currentDate ****/`r`n"
        $log += "Datetime = $currentDate `r`n"
        $log += "Path = $scriptAbsPath`r`n"
        $log += "Script = $scriptFullName`r`n"
        $log += "Command = $scriptCommandName`r`n"
        
        $log += "Start script checking ARCServer fails on server $ARCServeHost. Previous script execution done on 01/01/1900 at 00:00`r`n"
        if ($sqlData.jobErrorQty -gt 0) { $log += $sqlData.jobErrorQty.ToString() + " Error(s) found. A mail will be sent to GLPI in order to open an ticket containing all these errors`r`n" }
        else { $log += "No error found on last bakups jobs. No tiket opened to GLPI.`r`n" }
        $log += "Next script execution will only consider errors occurs after the current last job. If no errors detected no mails will be sent to GLPI and no incident will be created.`r`n"
        $log += "Script ends successfully`r`n"
        $log += "/************************************/`r`n"
        $log += $data
        $log += "`r`n"
        
        #Tools-logger -filename $logPath -log $log
}

function ARCServe-formatJobSummary
{
 #Parameters
    param([hashtable]$job)

    if ([string]::IsNullOrEmpty($paramEmailTo)) {
        Write-Host "ERROR on ARCServe-formatJobSummary script : 'job' parameter is Empty."
        Return ""
        Exit
    }

    [cultureinfo]::CurrentCulture = 'en-US'
    
    $jobDateStart = if ($job.jobDateStart -eq $null) {$null} else {$job.jobDateStart.ToString('dddd dd MMMM yyyy \a\t HH:mm:ss')}
    $jobDateStop = if ($job.jobDateStop -eq $null -or $job.jobStatus -eq 0) {$null} else {$job.jobDateStop.ToString('dddd dd MMMM yyyy \a\t HH:mm:ss')}
    $jobTimeStop = if ($job.jobDateStop -eq $null -or $job.jobStatus -eq 0) {$null} else {$job.jobDateStop.ToString('\a\t HH:mm:ss')}
    $jobBandwidthMB = if ([string]::IsNullOrWhiteSpace($job.jobBandwidthMB)) {"0 MB/min"} else {$job.jobBandwidthMB.ToString('###,###,##0.# MB/min')}
    $globalBackupedDataMB = if ([string]::IsNullOrWhiteSpace($job.globalBackupedDataMB)) {"0 MB"} else {$job.globalBackupedDataMB.ToString('###,###,##0.# MB')}
    $globalCompressionRatio = if ($job.globalCompressionRatio -eq $null) {"0"} else {$job.globalCompressionRatio.ToString('#0.##')}
    $globalBackupedFile = if ($job.globalBackupedFile -eq $null) {"0"} else {$job.globalBackupedFile.ToString('#,###,###,###')}
    $globalMissedFile = if ($job.globalMissedFile -eq $null) {"0"} else {$job.globalMissedFile.ToString('#,###,###,###')}

    $tableSummary = '
        <table cellspacing="0" cellpadding="0" width="100%" border="0" style="border-collapse:collapse">
	        <tbody>
		        <tr>
			        <td style="border:none; padding:0px; font-family:Tahoma; font-size:12px">
				        <table cellspacing="0" cellpadding="0" width="100%" border="0" style="border-collapse:collapse">
					        <tbody>
                                <tr style="height:55px; vertical-align:top; font-family:Tahoma; font-size:12px;">
							        <td style="width:72%; border:none; background-color:' + $job.jobStatusColor + '; color:White; font-weight:bold; font-size:16px; padding:10px 0 10px 15px;">
								        ' + $job.jobHostName + ' - Job ID : ' + $job.jobId + ' - ' + $job.jobDescription + '
								        <div class="jobDescription" style="margin-top:5px; font-size:12px;">' + $job.jobTypeDescription + ' Started ' + $job.jobTypeExecution + ' by ' + $job.jobOwner + '</div>
							        </td>
							        <td style="border:none; background-color:' + $job.jobStatusColor + '; color:White; font-weight:bold; font-size:16px; padding:10px 0 10px 15px; font-family:Tahoma">
								        ' + $job.jobStatusDescription +' ' + $jobTimeStop + '
								        <div class="jobDescription" style="margin-top:5px; font-size:12px">' + ($job.globalcountIp + $job.globalcountSuccess + $job.globalcountWarning + $job.globalCountError) + ' session(s) - ' + $job.jobCountSubjob + ' Sub Job(s)</div>
							        </td>
						        </tr>
                                <tr>
							        <td colspan="2" style="border:none; padding:0px; font-family:Tahoma; font-size:12px">
								        <table width="100%" cellspacing="0" cellpadding="0" class="inner" border="0" style="margin:0px; border-collapse:collapse">
									        <tbody>
                                                <tr style="height:17px">
											        <td colspan="9" class="sessionDetails" style="border-style:solid; border-color:#a7a9ac; border-width:1px 1px 1px 1px; height:35px; background-color:#f3f4f4; font-size:16px; vertical-align:middle; padding:5px 0 0 15px; color:#626365; font-family:Tahoma">
											            <span style="width:42%; display:inline-block;">Start: <b>' + $jobDateStart + '</b></span>
                                                        <span>-</span>
                                                        <span style="width:42%; display:inline-block; text-align:right;">Stop: <b>' + $jobDateStop + '</b></span>
                                                    </td>
										        </tr>'
    
    #High severity error messages
    foreach ($log in $job.logs)
    {
        if ($log.msgtypeid -ge 8) {
                            $tableSummary +=    '<tr style="border-style:solid; border-color:#a7a9ac; border-width:0px 1px 0px 1px; border-top: 1px dashed; background-color:#ffb794;  vertical-align:middle; font-family:Tahoma; font-size:10px">
											        <td colspan="1" style="margin: 2px; padding:5px 0 5px 15px;">' + $log.logtime + '</td>
                                                    <td colspan="8" style="margin: 2px; padding:5px 0 5px 15px;">' + $log.msgtext + '</td>
										        </tr>'
        }
    }

    if ($job.sessions.Count -gt 0) { 
                            $tableSummary +=    '<tr style="height:17px; vertical-align:middle; font-family:Tahoma; font-size:12px">
                                                    <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLInProgress + '"><b>In Progress</b></td>
											        <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLInProgress + '">'+ $job.globalcountIp +'</td>											        
                                                    <td nowrap="" style="width:85px; padding:2px 3px 2px 3px ;border:1px solid #a7a9ac; color:' + $colorHTMLSuccess + '"><b>Success</b></td>
											        <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLSuccess + '">'+ $job.globalcountSuccess +'</td>
											        <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLWarning + '"><b>Warning</b></td>
											        <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLWarning + '">'+ $job.globalcountWarning +'</td>
											        <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLError + '"><b>Error</b></td>
											        <td colspan="2" nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac; color:' + $colorHTMLError + '">'+ $job.globalCountError +'</td>
                                                </tr>
										        <tr style="height:17px; vertical-align:middle; font-family:Tahoma; font-size:12px">
                                                    <td nowrap="" style="width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Duration</b></td>
											        <td nowrap="" style="max-width:85px; padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $job.jobWallTime +'</td>		
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Total Datas</b></td>
											        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $globalBackupedDataMB +'</td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Total Files</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $globalBackupedFile +'</td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Missed Files</b></td>
											        <td colspan="2" nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $globalMissedFile +'</td>
										        </tr>
										        <tr style="height:17px; vertical-align:middle; font-family:Tahoma; font-size:12px">
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Bandwidth</b></td>
											        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $jobBandwidthMB +'</td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b>Compression</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $globalCompressionRatio +' </td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b> </b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">'+ $job.null +'</td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><b> </b></td>
											        <td colspan="2" nowrap="" style="padding:2px 3px 2px 3px;border:1px solid #a7a9ac;">'+ $job.null +'</td>
										        </tr>
                                                <tr style="height:17px">
											        <td colspan="9" nowrap="" style="height:35px; background-color:#f3f4f4; font-size:16px; vertical-align:middle; padding:5px 0 0 15px; color:#626365; font-family:Tahoma; border:1px solid #a7a9ac">
											            Log(s) and Session(s) Details
                                                    </td>
										        </tr>'
    #High severity normal messages
    foreach ($log in $job.logs)
    {
        if ($log.msgtypeid -lt 256) {
                            $tableSummary +=    '<tr style="border-style:solid; border-color:#a7a9ac; border-width:0px 1px 0px 1px; border-bottom: 1px dashed; background-color:#c2ff94;  vertical-align:middle; font-family:Tahoma; font-size:10px">
                                                        <td colspan="1" style="margin: 2px; padding:5px 0 5px 15px;">' + $log.logtime + '</td>
                                                        <td colspan="8" style="margin: 2px; padding:5px 0 5px 15px;">' + $log.msgtext.Replace("`n", "<br>")+ '</td>
										         </tr>'
        }
    }
                            $tableSummary +=    '<tr class="processObjectsHeader" style="height:23px; background-color:#e3e3e3; vertical-align:middle; border:1px solid #a7a9ac; font-family:Tahoma; font-size:12px">
											        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none;"><b>Name</b></td>
											        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none;"><b>Source</b></td>											            
                                                    <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none; text-align:center;"><b>Status</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none;"><b>Start time</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none;"><b>End time</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none; text-align:center;"><b>Backuped</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none; width:1%; text-align:center;"><b>Files</b></td>
											        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none; text-align:center;"><b>Duration</b></td>
											        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac; border-top:none;"><b>Details</b></td>
										        </tr>'
                                                

        foreach ($session in $job.sessions.Values) #| sort -Property Name
        {
            $sessionDateStart = if ($session.sessionDateStart -eq $null) {$null} else {$session.sessionDateStart.ToString('dd/MM/yyyy<br>HH:mm:ss')}
            if ($session.ip -eq 1) {$sessionDateStop = ""} else {
                $sessionDateStop = if ($session.sessionDateStop -eq $null) {$null} else {$session.sessionDateStop.ToString('dd/MM/yyyy<br>HH:mm:ss')}
            }
            $sessionBackupedFiles = if ($session.sessionBackupedFiles -eq $null) {$null} else {$session.sessionBackupedFiles.ToString('#,###,###,##0')}
            $sessionBackupedDatasMB = if ($session.sessionBackupedDatasMB -eq $null) {$null} else {$session.sessionBackupedDatasMB.ToString('###,###,##0.# MB')}
            
            $sessionDescription = if ($session.jobDescription -eq $null) {$null} else {$session.jobDescription.Trim() + "<br>"}
            $sessionDescription += if ($session.sessionDescription -eq $null) {$null} else {"On tape: " + $session.sessionDescription}

            $tableSummary += '<tr style="height:17px; vertical-align:middle; font-family:Tahoma; font-size:10px">
						        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">' + $session.jobSourceHost + '</td>
						        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">' + $session.jobSourcePath + '</td>
                                <td nowrap="" style="padding:2px 3px 2px 3px; text-align: center; border:1px solid #a7a9ac; background-color:' + $session.jobStatusColor + 'a1;">' + $session.jobStatusDescription + '</td>
                                <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">' + $sessionDateStart + '</td>
						        <td nowrap="" style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;">' + $sessionDateStop + '</td>
						        <td nowrap="" style="padding:2px 3px 2px 3px; text-align: center; border:1px solid #a7a9ac;">' + $sessionBackupedDatasMB + '</td>
						        <td nowrap="" style="padding:2px 3px 2px 3px; text-align: center; border:1px solid #a7a9ac;">' + $sessionBackupedFiles + '</td>
						        <td nowrap="" style="padding:2px 3px 2px 3px; text-align: center; border:1px solid #a7a9ac;">' + $session.sessionElapsedTime + ' </td>
						        <td style="padding:2px 3px 2px 3px; border:1px solid #a7a9ac;"><span class="small_label" style="font-size:10px">' + $sessionDescription + '</span></td>
					        </tr>'
                                #<td nowrap="" style="padding:2px 3px 2px 3px; text-align: center; border:1px solid #a7a9ac;"><span style="color:' + $session.jobStatusColor + '">' + $session.jobStatusDescription + '</span></td>
        }
    }

    $tableSummary +=                   '</tbody>
								    </table>
							    </td>
						    </tr>
					    </tbody>
				    </table>
			    </td>
		    </tr>
	    </tbody>
    </table>
    &nbsp;
    &nbsp;'

    return $tableSummary
}

#function ARCServe-GetAllJobs
#{
#    #Parameters
#        param([string]$ARCServeHost = "", [string]$instance = "",
#            [Parameter(Mandatory=$false)][string]$sinceDays,
#            [Parameter(Mandatory=$false)][DateTime[]]$between,
#            [Parameter(Mandatory=$false)][DateTime]$before,
#            [Parameter(Mandatory=$false)][DateTime]$after,
#
#            [Parameter(Mandatory=$false)][Boolean]$jobBackup,
#            [Parameter(Mandatory=$false)][Boolean]$jobRestore,
#            [Parameter(Mandatory=$false)][Boolean]$jobOhter
#            )
#        $whereDate = ""
#
#        if ($sinceDays.ToString() -ne "") { $whereDate = "DATEDIFF(day, tbl_job.endtime, GETDATE()) < $sinceDays AND " }
#        elseif ($between -ne $null) {$whereDate = "tbl_job.endtime between '" + $between[0] + "' and '" + $between[1] + "' AND " }
#        elseif ($before -ne $null) {$whereDate = "tbl_job.endtime <= '" + (Get-Date $before -format s) + "' AND " }
#        elseif ($after -ne $null) {$whereDate = "tbl_job.endtime >= '" + (Get-Date $after -format s) + "' AND " }
#        else {$whereDate = "DATEDIFF(day, tbl_job.endtime, GETDATE()) < 1 AND" }
#
#    $sqlQuery = "SELECT
#
#        CASE tbl_job.status   
#             WHEN 0 THEN 'In Progress' 
#             WHEN 1 THEN 'Finished' 
#             WHEN 2 THEN 'Cancelled' 
#             WHEN 3 THEN 'Failed' 
#             WHEN 4 THEN 'Incomplete'
#             ELSE CONVERT(VARCHAR, tbl_job.status)
#        END AS 'Job Status',
#
#        CONCAT(tbl_job.jhostname, '(', tbl_job.id, ')') AS 'Hostname (Job ID)',
#
#        CONCAT(
#        CASE tbl_job.type  
#            WHEN 1 THEN 'Backup'
#            WHEN 3 THEN 'Backup (GFS Rotation)'
#            WHEN 5 THEN 'Backup (GFS Makeup)'
#            WHEN 7 THEN 'Restore'
#	        WHEN 12 THEN 'Count'
#	        WHEN 18 THEN 'Prune Database'
#	        WHEN 20 THEN 'Backup (Makeup)'
#	        WHEN 21 THEN 'Backup (Rotation)'
#	        WHEN 45 THEN 'Device Management'
#	        WHEN 46 THEN 'Generic Job'
#            ELSE CONVERT(VARCHAR, tbl_job.type)
#        END,
#        'CHAR10',
#        tbl_job.comment) AS 'Job Type',
#
#        tbl_path.path AS 'Source Host',
#        tbl_pathw.path AS 'Source Path',
#
#        CASE
#            WHEN tbl_job.starttime > tbl_job.endtime OR tbl_job.status = 0 THEN
#                CONCAT('Operation in progress', ' (From ', FORMAT(tbl_job.starttime, 'dd/MM/yy HH:mm:ss'), ' - To ?', ')')
#            ELSE
#                CONCAT('XXX',
#                'From ', FORMAT(tbl_job.starttime, 'dd/MM/yy HH:mm:ss'), ' - To ', FORMAT(tbl_job.endtime, 'dd/MM/yyyy HH:mm:ss'),
#                'YYY',
#                CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_job.starttime, tbl_job.endtime), 0), 8), 
#                'ZZZ')
#            END AS 'Job Elapsed Time',
#
#        CASE 
#            WHEN tbl_tapeSes.tapeid IS NULL THEN
#                NULL
#            ELSE
#                CONCAT(RTRIM(tbl_tape.tapename), ',', 'CHAR10', 'Serial ', RTRIM(tbl_tape.serialnum), ' (ID: ', tbl_tapeSes.tapeid, ')')
#            END AS 'Tape Name',
#
#        CASE
#            WHEN tbl_tapeSes.starttime > tbl_tapeSes.endtime OR tbl_tapeSes.status = 0 THEN
#                CONCAT('Operation in progress', ' (From ', FORMAT(tbl_tapeSes.starttime, 'dd/MM/yy HH:mm:ss'), ' - To ?', ')')
#            ELSE
#                CONCAT(
#                CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_tapeSes.starttime, tbl_tapeSes.endtime), 0), 8),
#                ' (From ', FORMAT(tbl_tapeSes.starttime, 'dd/MM/yy HH:mm:ss'), ' - To ', FORMAT(tbl_tapeSes.endtime, 'dd/MM/yyyy HH:mm:ss'), ')')
#            END AS 'Session Elapsed Time',
#
#
#        CASE 
#            WHEN tbl_tapeSes.tapeid IS NULL THEN
#                NULL
#             ELSE
#                CONCAT(
#                FORMAT(((POWER(2.0, 32) + tbl_tapeSes.totalkb) % POWER(2.0, 32))/1024.0, '###,###,##0.# MB'),
#                'CHAR10', FORMAT(tbl_tapeSes.totalfiles, '#,###,###,###'), ' File(s)')
#             END AS 'Backuped Data',
#
#        FORMAT(tbl_tapeSes.ThroughputMBPerMin, '###,###,### MB.min-1') AS 'Bandwith',
#        FORMAT(tbl_tapeSes.sizeOnTapeKB/CONVERT(FLOAT, tbl_tapeSes.totalkb), '#0.##') AS 'Compression ratio'
#
#        FROM asdb.dbo.asjob tbl_job 
#        LEFT JOIN asdb.dbo.astpses tbl_tapeSes ON tbl_job.id = tbl_tapeSes.jobid
#        LEFT JOIN aspath.dbo.aspathname tbl_path ON tbl_tapeSes.srchostid = tbl_path.id
#        LEFT JOIN aspath.dbo.aspathnamew tbl_pathw ON tbl_tapeSes.srcpathid = tbl_pathw.id
#        LEFT JOIN asdb.dbo.ashost tbl_host ON tbl_tapeSes.srchostid = tbl_host.rhostid
#        LEFT JOIN asdb.dbo.astape tbl_tape ON tbl_tapeSes.tapeid = tbl_tape.id
#
#        WHERE $whereDate
#        AND tbl_job.status IN (2, 3, 4)
#        AND tbl_job.type IN (1, 2, 3, 4, 5)
#
#        ORDER BY tbl_job.id DESC, tbl_pathw.path, tbl_path.path
#    ";
#
#    Return Sql-QueryDB -sqlHost $ARCServeHost -sqlInstance $instance -sqlQuery $sqlQuery
#}
#
#function ARCServe-GetErrorJobsID
#{
#    #Parameters
#        param([string]$ARCServeHost = "", [string]$instance = "",
#            [Parameter(Mandatory=$false)][string]$sinceDays,
#            [Parameter(Mandatory=$false)][DateTime[]]$between,
#            [Parameter(Mandatory=$false)][DateTime]$before,
#            [Parameter(Mandatory=$false)][DateTime]$after)
#
#    $whereDate = ""
#
#    if ($sinceDays.ToString() -ne "") { $whereDate = "DATEDIFF(day, tbl_job.endtime, GETDATE()) < $sinceDays AND " }
#    elseif ($between -ne $null) {$whereDate = "tbl_job.endtime between '" + $between[0] + "' and '" + $between[1] + "' AND " }
#    elseif ($before -ne $null) {$whereDate = "tbl_job.endtime <= '" + (Get-Date $before -format s) + "' AND " }
#    elseif ($after -ne $null) {$whereDate = "tbl_job.endtime >= '" + (Get-Date $after -format s) + "' AND " }
#    else {$whereDate = "DATEDIFF(day, tbl_job.endtime, GETDATE()) < 1 AND" }
#
#    $sqlQuery = "SELECT
#
#        tbl_job.id, FORMAT(tbl_job.endtime, 'dd/MM/yyyy HH:mm:ss') endtime
#
#        FROM asdb.dbo.asjob tbl_job 
#       
#        WHERE $whereDate
#        tbl_job.status IN (2, 3, 4)
#        AND tbl_job.type IN (1, 2, 3, 4, 5)
#
#        ORDER BY tbl_job.id DESC
#    ";
#
#    Return Sql-QueryDB -sqlHost $ARCServeHost -sqlInstance $instance -sqlQuery $sqlQuery
#}