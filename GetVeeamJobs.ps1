########################################
# Name: Get Veeam jobs
# Current version : 1.0
# Current Release date : 18/06/2019
# Author : Ibermática (Ion Molla)
#
# Usage: This script contain some functions to connects Veeam DBs and and returns the list of Jobs
#    Veeam-getJobs : Return a list of jobs, subjobs and sessions (with logs and all important infos) base on some filters (status, type and date)
#        - Return Value : Return a System.Data.DataSet Object
#     
# 
# Execution: Must be executed with user having rights on Veeam Database.
########################################




################################################
#EJEMPLO
################################################
 
<#
    $paramSqlHost = "SISDTC0051"
    $paramSqlInstance = "VEEAMSQL2012"
    $paramSqlDBName = "VeeamBackup"
    $paramHostName = "SISDTC0051"
    $paramEndDate= "true"
    $paramAfter= "26/06/2019 00:00:00"


      if ($paramEndDate -eq $true) { $getDateStartOrEnd = "end_time" }
            else { $getDateStartOrEnd = "creation_time" }
           
            if ($paramBefore -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd <= '" + (Get-Date $paramBefore -format s) + "'"}; 
            if ($paramAfter -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd >= '" + (Get-Date $paramAfter -format s) + "'"}; 

$sqlQuery = "SELECT 
                    T2.id AS 'jobId',
			        job_name AS 'jobName',
			        T1.description AS 'jobDescription',
			        result AS 'jobResult',
			        job_type AS 'jobType',
			        modified_by AS 'jobOwner',
			        creation_time AS 'jobCreationTime',
			        end_time AS 'jobEndTime',
			        CASE
				        WHEN creation_time > end_time THEN
					        NULL
				        ELSE
					        CONVERT (VARCHAR, DATEADD(minute, DATEDIFF(minute, creation_time, end_time), 0), 8) END AS 'jobWallTime'
	
		        FROM [VeeamBackup].[dbo].[BJobs] T1 
		        INNER JOIN [VeeamBackup].[dbo].[Backup.Model.JobSessions] T2 ON T1.id=T2.job_id 
		        WHERE job_name LIKE '%00%-%' 
                $sqlWhereJobsDate
                ORDER BY creation_time DESC"

$jobs = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery

  foreach ($job in $jobs.Tables[0].Rows)
 { 
 
     $SqlQuery = "SELECT
		        tbl_BaTaskSes.object_name AS 'jobSourceHost',
		        tbl_BaTaskSes.id AS 'sessionId',
		        tbl_JobSes.id AS 'jobId',
		        tbl_BaTaskSes.status AS 'sessionStatus',
		        tbl_JobSes.job_name AS 'jobName',
		
		        tbl_JobSes.creation_time AS 'sessionDateStart',
		        tbl_JobSes.end_time AS 'sessionDateStop',
			        CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_JobSes.creation_time, tbl_JobSes.end_time), 0), 8) AS 'sessionElapsedTime',
		
		
		        tbl_BaJobSes.processed_objects AS 'sessionBackupedFiles',
		        tbl_JobSes.result AS 'sessionResult',
		        tbl_BaJobSes.processed_size AS 'sessionBackupedSizeKB',
		        tbl_BaJobSes.is_retry AS 'SessionJobIsRetry',
		        tbl_BaJobSes.avg_speed AS 'sessionAverageSpeed'

		         FROM [VeeamBackup].[dbo].[Backup.Model.JobSessions] tbl_JobSes
                        LEFT JOIN [VeeamBackup].[dbo].[Backup.Model.BackupJobSessions] tbl_BaJobSes ON tbl_JobSes.id = tbl_BaJobSes.id
                        LEFT JOIN [VeeamBackup].[dbo].[Backup.Model.BackupTaskSessions] tbl_BaTaskSes ON tbl_JobSes.id = tbl_BaTaskSes.session_id
		        WHERE  tbl_JobSes.id = '" + $job.jobId + "' AND job_name LIKE '%00%-%'
		        ORDER BY tbl_JobSes.creation_time DESC"

            $sessions = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery



    
    foreach ($session in $sessions.Tables[0].Rows)
    {
       echo ($session.jobname)
       echo ($session.sessionDateStop)
       echo ($session.jobSourceHost)
       
    }
  
   
    #echo ($job.jobname)
    #echo ($job.jobEndTime)
 }
    
 #>
   
################################################
#EJEMPLO
################################################


#Function call
 . D:\DEV-ION\Script\std\DBQuery.ps1


    $paramSqlHost = "SISDTC0051"
    $paramSqlInstance = "VEEAMSQL2012"
    $paramSqlDBName = "VeeamBackup"
    $paramHostName = "SISDTC0051"

    #Global variable
    $global:sqlInstance_Veeam = "VEEAMSQL2012"

    function Veeam-getJobs #Get jobs, logs and linked sessions
{
    #Parameters
        param([string]$paramSqlHost = "",
            [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_Veeam,
            [Parameter(Mandatory=$false)][string]$paramHostName = $null,
            [Parameter(Mandatory=$false)][boolean]$paramEndDate = $false,
            [Parameter(Mandatory=$false)][DateTime]$paramBefore,
            [Parameter(Mandatory=$false)][DateTime]$paramAfter
             )

    ######### Variables ###########

    $jobsLimit = 3000 #Limit job reporting up to xxx
    $message = ""
    $jobs = $null                #SQL Dataset Result
            [hashtable]$jobsArray = @{}
            [hashtable]$jobArray = @{}
            $job = $null
       
            
    $sessions = $null            #SQL Dataset Result
                [hashtable]$sessionsArray = @{}
                [hashtable]$sessionArray = @{}
                $session = $null
    
    $returnArray = @{}
    $sqlWhereJobsDate = ""       #SQL Where Date
    $getDateStartOrEnd = ""
    


      #Init some variable
            if ($paramEndDate -eq $true) { $getDateStartOrEnd = "end_time" }
            else { $getDateStartOrEnd = "creation_time" }
           
            if ($paramBefore -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd <= '" + (Get-Date $paramBefore -format s) + "'"}; 
            if ($paramAfter -ne $null) {$sqlWhereJobsDate = "AND $getDateStartOrEnd >= '" + (Get-Date $paramAfter -format s) + "'"}; 
          

        #Get JOBS 
       
        $sqlQuery = "SELECT 
			        T2.id AS 'jobId',
			        job_name AS 'jobName',
			        T1.description AS 'jobDescription',
			        result AS 'jobResult',
                    CAST(REPLACE(CAST(reason as nvarchar(MAX)), '''','') AS NText) AS 'jobResultReason',
			        job_type AS 'jobType',
			        modified_by AS 'jobOwner',
			        creation_time AS 'jobCreationTime',
			        end_time AS 'jobEndTime',
			        CASE
				        WHEN creation_time > end_time THEN
					        NULL
				        ELSE
					        CONVERT (VARCHAR, DATEADD(minute, DATEDIFF(minute, creation_time, end_time), 0), 8) END AS 'jobWallTime'
	
		        FROM [VeeamBackup].[dbo].[BJobs] T1 
		        INNER JOIN [VeeamBackup].[dbo].[Backup.Model.JobSessions] T2 ON T1.id=T2.job_id 
                $sqlWhereJobsDate
                ORDER BY creation_time DESC"

        $jobs = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery

    #Check

    if ($jobs.Tables[0].Rows.Count -gt $jobsLimit) {
        #Prepare Message if jobs return limit has been reached
            $message =  "" + $jobs.Tables[0].Rows.Count + " jobs has been found."
            $message += " Display limit is set to $jobsLimit. Please generate another report with more precise search criteria."
            
    }
   
    else {
        #Prepare Message if jobs found
            if ($jobs.Tables[0].Rows.Count -gt 0) {
                  $message = "" + $jobs.Tables[0].Rows.Count + " jobs has been found."
                  
            }
            else {
        #Prepare Message if no jobs found
                 $message = " Jobs has not been found."
                 
            }

        #Iterate each jobs
        foreach ($job in $jobs.Tables[0].Rows)
        {
            $jobArray = @{}
            $sessionsArray = @{}
            $sessions = $null
            
       
            $jobArray.add("jobId", $job.jobId)
            $jobArray.add("jobName", $job.jobName)
            $jobArray.add("jobType", $job.jobType)
            $jobArray.add("jobDescription", $job.jobDescription)
            $jobArray.add("jobResultOrigin",$job.jobResult)

            switch ($job.jobResult) {
                0 { $jobArray.add("jobResult",0); 
                    $jobArray.add("jobResultDescription","Finished");break }
                1 { $jobArray.add("jobResult",1);
                    $jobArray.add("jobResultDescription","Warning"); break }
                2 { $jobArray.add("jobResult",2);
                    $jobArray.add("jobResultDescription","Failed"); break }
                default { $jobArray.add("jobStatus",99); $jobArray.add("jobResultDescription","Other :"+$job.jobResult); break }
                }

            $jobArray.add("jobResultReason", $job.jobResultReason) #Jobs log
            $jobArray.add("jobCreationTime", $job.jobCreationTime)
            $jobArray.add("jobEndTime", $job.jobEndTime)
            $jobArray.add("jobWallTime", $job.jobWallTime)
            $jobArray.add("jobOwner", $job.jobOwner)

    #Get SESSIONS 
  
    $SqlQuery = "SELECT
		        tbl_BaTaskSes.object_name AS 'jobSourceHost',
		        tbl_BaTaskSes.id AS 'sessionId',
		        tbl_JobSes.id AS 'jobId',
		        tbl_BaTaskSes.status AS 'sessionStatus',
                CAST(REPLACE(CAST(tbl_BaTaskSes.reason as nvarchar(MAX)),'''','') AS NText) AS 'sessionStatusReason',
		        tbl_JobSes.job_name AS 'jobName',
		
		        tbl_BaTaskSes.creation_time AS 'sessionDateStart',
		        tbl_BaTaskSes.end_time AS 'sessionDateStop',
                    CONVERT(VARCHAR, DATEADD(second, DATEDIFF(second, tbl_BaTaskSes.creation_time, tbl_BaTaskSes.end_time), 0), 8) AS 'sessionElapsedTime',
		
		        tbl_BaTaskSes.processed_objects AS 'sessionBackupedFiles',
		        tbl_JobSes.result AS 'sessionResult',
		        tbl_BaTaskSes.total_size AS 'sessionBackupedSizeB',
		        tbl_BaTaskSes.avg_speed AS 'sessionAverageSpeed'

		         FROM [VeeamBackup].[dbo].[Backup.Model.JobSessions] tbl_JobSes
                        LEFT JOIN [VeeamBackup].[dbo].[Backup.Model.BackupJobSessions] tbl_BaJobSes ON tbl_JobSes.id = tbl_BaJobSes.id
                        LEFT JOIN [VeeamBackup].[dbo].[Backup.Model.BackupTaskSessions] tbl_BaTaskSes ON tbl_JobSes.id = tbl_BaTaskSes.session_id
		        WHERE  tbl_JobSes.id = '" + $job.jobId + "' AND tbl_BaTaskSes.id IS NOT NULL
		        ORDER BY tbl_JobSes.creation_time DESC"

            $sessions = Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlDBName $sqlDBName -sqlQuery $sqlQuery
   
             #Iterate linked sessions
            foreach ($session in $sessions.Tables[0].Rows)
            {
                $sessionArray = @{}

                $sessionArray.add("sessionStatusOrigin", $session.sessionStatus)

                switch ($session.sessionStatus) {
                    0 { $sessionArray.add("sessionStatus",0); 
                        $sessionArray.add("SessionStatusDescription", "Finished"); break }
                    1 { $sessionArray.add("sessionStatus",1); 
                        $sessionArray.add("SessionStatusDescription", "Warning"); break }
                    2 { $sessionArray.add("sessionStatus",2); 
                        $sessionArray.add("SessionStatusDescription", "Failed"); break }
                    3 { $sessionArray.add("sessionStatus",1); 
                        $sessionArray.add("SessionStatusDescription", "Warning"); break }
                    default { $sessionArray.add("sessionStatus",99); $sessionArray.add("SessionStatusDescription","Other :"+$session.sessionStatus); break }
                }
                
                $sessionArray.add("sessionStatusReason", $session.sessionStatusReason)#Sessions log
                $sessionArray.add("sessionId", $session.sessionId)
                $sessionArray.add("jobId", $session.jobId)
                $sessionArray.add("jobName", $session.jobName)
                $sessionArray.add("jobSourceHost", $session.jobSourceHost)
               
                $sessionArray.add("sessionDateStart", $session.sessionDateStart)
                $sessionArray.add("sessionDateStop", $session.sessionDateStop)
                $sessionArray.add("sessionElapsedTime", $session.sessionElapsedTime)

                $sessionArray.add("sessionBackupedSizeMB", ($session.sessionBackupedSizeB)/1024/1024)# B-->KB-->MB 
                $sessionArray.add("sessionBackupedFiles", $session.sessionBackupedFiles)
                $sessionArray.add("sessionResult", $session.sessionResult)
                $sessionArray.add("sessionAverageSpeed", $session.sessionAverageSpeed)
              

                $sessionsArray.add($sessionsArray.Count, $sessionArray)
            }
            #Add sessions to job table
                $jobArray.add("sessions", $sessionsArray)
#>
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

<#
function Veeam-GetJobLogs
{
    #Parameters
        param([string]$paramSqlHost = "",
            [Parameter(Mandatory=$false)][string]$paramSqlInstance = $sqlInstance_VeeamDB,
            [int]$jobId = 0,
            [Parameter(Mandatory=$false)][int]$maxSeverity)
    
    $sqlQuery = "SELECT
                     session_id,
                     event_time,
                     severity,
                     text,
                     CONCAT(FORMAT(event_time, 'dd/MM/yy HH:mm:ss'), ' - ', text) AS fullLog 
                     FROM [VeeamBackupReporting].[dbo].[Enterprise.SessionEvents] 
                     WHERE severity <= $maxSeverity and session_id=$jobId
                     ORDER BY event_time DESC"

       Return  Sql-QueryDB -sqlHost $paramSqlHost -sqlInstance $paramSqlInstance -sqlQuery $sqlQuery
}


$logs=(Veeam-GetJobLogs -paramSqlHost $paramSqlHost -paramSqlInstance $paramSqlInstance -maxSeverity 1)

 foreach ($log in $logs.Tables[0].Rows)
        {
            echo ($log.session_id) 
            #echo ($log.event_time)
            echo ($log.severity)
            echo ($log.text)
            echo ($log.fullLog)
       
        }

#>
