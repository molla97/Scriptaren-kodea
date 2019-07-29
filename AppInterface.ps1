########################################
# Name: Application Interface
# Current version : 1.0
# Current Release date : 12/06/2019
# Version History : 1.0 of 12/06/2019 -> First Beta version. Veeam and Arcserve Backup extraction.
# Author : Ibermática (Ion Molla)
#
# Usage: This script contain some functions to connects Backup application database and extract specific information (like Jobs, sessions, logs...) and feed GLPI Backup Report
#    
#    BackupReport-fullExtraction : Loop on all specified backup host to extract and insert data
#        - Return Value : none 
#    BackupReport-insertDataVeeam :  Insert Veeam backup job data into bkpReport_* GLPI table
#        - Return Value : none
#    BackupReport-insertDataArcserveBackup : Insert Arcserve backup job data into bkpReport_* GLPI table
#        - Return Value : none
#    Veeam-extractForBackupReport : Connect to Veeam specific host, retrieve and format some data to prepara injection on report table
#        - Return Value : none
#    ARCServeBackup-extractForBackupReport : Connect to Arcserve specific host, retrieve and format some data to prepara injection on report table
#        - Return Value : none
# 
# Execution: Must be executed with user having rights on Database. We recommand specific created user on  AD : s-bkpReporter
########################################

#Function call
    . D:\DEV-ION\Script\std\DBQuery.ps1
    . D:\DEV-ION\Script\backup\Veeam\GetVeeamJobs.ps1
    . D:\DEV-ION\Script\backup\ARCServe\GetArcserveJobs.ps1

#Global variable


function BackupReport-fullExtraction
{
    $backupHosts = (MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery " SELECT * FROM ibservice.bkpReport_hosts;")
    $data = (MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery " SELECT * FROM  ibservice.bkpReport_job;")

    foreach ($backupHost in $backupHosts.Tables[0].Rows)
    {
        if($backupHost.bkphost_enabled -eq $true)
        {
            if ($data.Tables[0].Rows.bkpjob_id.count -eq 0)
            {    
                echo ("DB empty")
                $dte = Get-Date
                $getJobAfterDate= $dte.AddDays(-365)
            }
            else
            {
                echo ("DB Information")
                $dte = Get-Date
                $getJobAfterDate= $dte.AddDays(-1)
            }

            if($backupHost.bkphost_app -eq "arcserve backup") 
            {
                  
                $Arcservejobs = ARCServeBackup-extractForBackupReport -paramHostname $backupHost.bkphost_hostname -getJobAfterDate $getJobAfterDate

                foreach ($Arcservejob in $Arcservejobs.jobs.Values)
                {
                    BackupReport-insertDataArcserveBackup -job $Arcservejob -app $backupHost.bkphost_app
                } 
                   
                echo ($backupHost.bkphost_hostname) 
                echo ("########### Arcserve jobs successfully copied ###########")
            }

            if($backupHost.bkphost_app -eq "veeam") 
            {
                
                $Veeamjobs = Veeam-extractForBackupReport -paramHostname $backupHost.bkphost_hostname -getJobAfterDate $getJobAfterDate
            
            
                foreach ($Veeamjob in $Veeamjobs.jobs.Values)
                {
                    BackupReport-insertDataVeeam -job $Veeamjob -app $backupHost.bkphost_app -Hostname $backupHost.bkphost_hostname
                }

                echo ($backupHost.bkphost_hostname) 
                echo ("########### Veeam jobs successfully copied ###########")
            }  
        }            
    }      
}      

function BackupReport-insertDataVeeam
{
    #Parameters
        param($job, [string]$app,[string]$Hostname)

    [cultureinfo]::CurrentCulture = 'en-US'
    

    if ($job.jobId)
    {
        #Import job into DB Backup Report
        $jobDateStart = if ($job.jobCreationTime -eq $null) {$null} else {$job.jobCreationTime.ToString('yyyy-MM-dd HH:mm:ss')}
        $jobDateStop = if ($job.jobEndTime -eq $null) {$null} else {$job.jobEndTime.ToString('yyyy-MM-dd HH:mm:ss')}
        $jobTimeStop = if ($job.jobEndTime -eq $null) {$null} else {$job.jobEndTime.ToString('HH:mm:ss')}
    

        #Insert veeam JOBS data into GLPI table
        $sqlQuery = "INSERT INTO ibservice.bkpReport_job (bkpjob_software, bkpjob_jobID,bkpjob_hostName, bkpjob_description, bkpjob_type,  bkpjob_owner,
                                            bkpjob_statusOrigin, bkpjob_status, bkpjob_statusDescription, bkpjob_dateStart, bkpjob_dateStop, bkpjob_elapsedTime)
                                           
        VALUES ('$app', '" + $job.jobId + "','$Hostname','" + $job.jobName + "','" + $job.jobType + "','" + $job.jobOwner + "','" +
                    $job.jobResultOrigin + "','" + $job.jobResult + "','" + $job.jobResultDescription + "', '$jobDateStart', '$jobDateStop','" + $job.jobWallTime + "');
                    SELECT LAST_INSERT_ID();"

        $sqlResult = (MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery)
               
#<#
        if ($sqlResult)
        {
            $bkpjob_id = $sqlResult.Tables[0].Rows[0][0]      

            #Import job log(s) into DB Backup Report
            if (! [string]::IsNullOrEmpty($job.jobResultReason))
            {
                $sqlQuery = "INSERT INTO ibservice.bkpReport_log (bkplog_bkpjob_id, bkplog_logTime, bkplog_msgtext)
                VALUES (" + $bkpjob_id + ", '" + $jobDateStart + "', '" + $job.jobResultReason + "');"
                
                #echo $sqlQuery
                $JobsLogs = MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery
            }
 
            #Import job session(s) into DB Backup Report  
            if ($job.sessions.Count -gt 0)
            {                                 

            #Session details
                foreach ($session in $job.sessions.Values) #| sort -Property Name
                {
                    $sessionDateStart = if ($session.sessionDateStart -eq $null) {$null} else {$session.sessionDateStart.ToString('yyyy-MM-dd HH:mm:ss')}
                    $sessionDateStop = if ($session.sessionDateStop -eq $null) {$null} else {$session.sessionDateStop.ToString('yyyy-MM-dd HH:mm:ss')}
                    
                    
                    $sessionDescription = if ($session.jobName -eq $null) {$null} else {$session.jobName.Trim() + "<br>"}
 
                    #Insert veeam SESSIONS data into GLPI table

                    $sqlQuery = "INSERT INTO bkpReport_ses (bkpses_bkpjob_id, bkpses_sourceHost, bkpses_description, bkpses_statusOrigin, bkpses_status, bkpses_statusDescription,  
                                                            bkpses_dateStart, bkpses_dateStop, bkpses_elapsedTime, bkpses_backupedDataMB, bkpses_backupedFile)
                                 VALUES (" + $bkpjob_id + ",'" + $session.jobSourceHost + "','" + $sessionDescription + "', '" + $session.sessionStatusOrigin + "', '" + $session.sessionStatus + "', '" +
                                         $session.SessionStatusDescription + "', '$sessionDateStart', '$sessionDateStop', '" + $session.sessionElapsedTime + "', '" +
                                         $session.sessionBackupedSizeMB + "', '" + $session.sessionBackupedFiles + "');"
    
                    $resultVeeam = MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery

                    #Import session log(s) into DB Backup Report
                    if (! [string]::IsNullOrEmpty($session.sessionStatusReason))
                    {
                        $sqlQuery = "INSERT INTO ibservice.bkpReport_log (bkplog_bkpjob_id, bkplog_logTime, bkplog_msgtext)
                        VALUES (" + $bkpjob_id + ", '" + $sessionDateStart + "', '" + $session.sessionStatusReason + "');"

                        $SessionsLogs = MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery
                    }
                   
                }
            }

         }
       
    }
}
  

function BackupReport-insertDataArcserveBackup
{
    #Parameters
        param($job, [string]$app)

    [cultureinfo]::CurrentCulture = 'en-US'
    

    if ($job.jobId)
    {
        #Import job into DB Backup Report
        $jobDateStart = if ($job.jobDateStart -eq $null) {$null} else {$job.jobDateStart.ToString('yyyy-MM-dd HH:mm:ss')}
        $jobDateStop = if ($job.jobDateStop -eq $null) {$null} else {$job.jobDateStop.ToString('yyyy-MM-dd HH:mm:ss')}
        $jobTimeStop = if ($job.jobDateStop -eq $null) {$null} else {$job.jobDateStop.ToString('HH:mm:ss')}

    
        $sqlQuery = "INSERT INTO ibservice.bkpReport_job (bkpjob_software, bkpjob_jobID, bkpjob_hostName, bkpjob_description, bkpjob_type, bkpjob_typeDescription, bkpjob_typeExecution, bkpjob_owner,
                                            bkpjob_statusOrigin, bkpjob_status, bkpjob_statusDescription, bkpjob_statusColor,
                                            bkpjob_dateStart, bkpjob_dateStop, bkpjob_elapsedTime,
                                            bkpjob_jobCountSubjob,  bkpjob_globalcountSuccess,  bkpjob_globalcountWarning, bkpjob_globalCountError, 
                                            bkpjob_bandwidthMBmin,  bkpjob_backupedDataMB,  bkpjob_backupedFile, 
                                            bkpjob_compressionRatio, bkpjob_missedFile)
        VALUES ('" + $app + "', " + $job.jobId + ", '" + $job.jobHostName + "', '" + $job.jobDescription + "', '" + $job.jobType  + "', '" + $job.jobTypeDescription  + "', '" + $job.jobTypeExecution  + "', '" + $job.jobOwner  + "', '" +
                    $job.jobStatusOrigin + "', '" + $job.jobStatus + "', '" + $job.jobStatusDescription + "', '" + $job.jobStatusColor + "', '" +
                    $jobDateStart + "', '" + $jobDateStop + "', '" + $job.jobWallTime	+ "', " +
                    $job.jobCountSubjob + ", " + $job.globalcountSuccess + ", " + $job.globalcountWarning + ", " + $job.globalCountError + ", " +
                    $job.jobBandwidthMB + ", " + $job.globalBackupedDataMB + ", " + $job.globalBackupedFile + ", '" +
                    $job.globalCompressionRatio + "', " + $job.globalMissedFile + ");
                    SELECT LAST_INSERT_ID();"
                    
        $sqlResult = (MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery)          

        if ($sqlResult)
        {
            $bkpjob_id = $sqlResult.Tables[0].Rows[0][0]

            #Import job log(s) into DB Backup Report
            #Log
                foreach ($log in $job.logs)
                {
                    $sqlQuery = "INSERT INTO ibservice.bkpReport_log (bkplog_bkpjob_id, bkplog_logTime, bkplog_msgtypeid, bkplog_severity, bkplog_msgtext)
                    VALUES (" + $bkpjob_id + ", '" + $log.logtime.ToString('yyyy-MM-dd HH:mm:ss') + "', " + $log.msgtypeid + ", " + $log.severity + ", '" + $log.msgtext + "');"
                
                   $JobLog= MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery
                }
    
            #Import job session(s) into DB Backup Report  
            if ($job.sessions.Count -gt 0)
            {                                 

            #Session details
                foreach ($session in $job.sessions.Values)
                {
                    $sessionDateStart = if ($session.sessionDateStart -eq $null) {$null} else {$session.sessionDateStart.ToString('yyyy-MM-dd HH:mm:ss')}
                    if ($session.ip -eq 1) {$sessionDateStop = ""} else {
                        $sessionDateStop = if ($session.sessionDateStop -eq $null) {$null} else {$session.sessionDateStop.ToString('yyyy-MM-dd HH:mm:ss')}
                    }
                    
                    $sessionDescription = if ($session.jobDescription -eq $null) {$null} else {$session.jobDescription.Trim() + "<br>"}
                    $sessionDescription += if ($session.sessionDescription -eq $null) {$null} else {"On tape: " + $session.sessionDescription}
    
    
                    $sqlQuery = "INSERT INTO bkpReport_ses (bkpses_bkpjob_id, bkpses_sourceHost, bkpses_sourcePath, bkpses_description, 
                            bkpses_statusOrigin, bkpses_status, bkpses_statusDescription, bkpses_statusColor, 
                            bkpses_dateStart, bkpses_dateStop, bkpses_elapsedTime, 
                            bkpses_backupedDataMB, bkpses_backupedFile, bkpses_missedFile)
                    VALUES (" + $bkpjob_id + ", '" + $session.jobSourceHost + "', '"+ $session.jobSourcePath + "', '" + $sessionDescription + "', '"+
                                $session.jobStatusOrigin + "', '" + $session.jobStatus + "', '" + $session.jobStatusDescription + "', '" + $session.jobStatusColor + "', '" +
                                $sessionDateStart + "', '" + $sessionDateStop + "', '" + $session.sessionElapsedTime + "', " +
                                $session.sessionBackupedDatasMB + ", " + $session.sessionBackupedFiles + ", " + $session.sessionMissedFiles + ");"
    
                  $resultArcserve=MySql-QueryDB -sqlHost "glpi.intranet.local" -sqlDBName "ibservice" -sqlQuery $sqlQuery
                }
            }
        }
    }
}


function Veeam-extractForBackupReport
{
 #Parameters
    param([string]$paramHostname, [DateTime]$getJobAfterDate)

    #$getJobAfterDate= [DateTime]::ParseExact('01/03/2019 00:00:00','dd/MM/yyyy HH:mm:ss',[CultureInfo]::InvariantCulture)

    #Return only the backup jobs after "getJobAfterDate"
    return Veeam-getJobs -paramSqlHost $paramHostname -paramHostName $paramHostname -paramEndDate $true -paramAfter $getJobAfterDate 
}


function ARCServeBackup-extractForBackupReport
{
    #Parameters
        param([string]$paramHostname, [DateTime]$getJobAfterDate)

   #$getJobAfterDate= [DateTime]::ParseExact('01/03/2019 00:00:00','dd/MM/yyyy HH:mm:ss',[CultureInfo]::InvariantCulture)

    #Return only the finished backup jobs (type 1, 3, 5, 20, 21) after "getJobAfterDate"
    return ARCServe-getJobs -paramSqlHost $paramHostname -paramHostName $paramHostname -paramEndDate $true -paramAfter $getJobAfterDate -paramType '1, 3, 5, 20, 21'
}  


