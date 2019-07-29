

function Sql-QueryDB
{
    #Parameters
        param([string]$sqlHost = $null, [string]$sqlInstance = $null, [string]$sqlDBName = $null, [string]$sqlQuery = "")

    if ([string]::IsNullOrEmpty($sqlHost)) {
        Write-Host "ERROR on DBQuery script : sqlHost parameter is Null or Empty. Current value is '$sqlServer'"
    	Return $null
    }

    if (! $sqlInstance) {
        Write-Host "ERROR on DBQuery script : sqlInstance parameter is Null. Current value is '$sqlServer'"
	    Return $null
    }

    if ([string]::IsNullOrEmpty($sqlQuery)) {
        Write-Host "ERROR on DBQuery script : SqlQuery parameter is Null or Empty. Current value is '$sqlQuery'"
	    Return $null    
    }

    #Write-Host $sqlQuery

    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $sqlConnection.ConnectionString = "Server = $sqlHost\$sqlInstance; Database = $sqlDBName; Integrated Security = True; Connect Timeout=30; ApplicationIntent=ReadOnly; Pooling=true"

    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $sqlCmd.CommandText = $sqlQuery
    $sqlCmd.Connection = $sqlConnection

    $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $sqlAdapter.SelectCommand = $sqlCmd

    $dataSet = New-Object System.Data.DataSet
    $sqlAdapter.Fill($dataSet)
    
    #if ($PSCmdlet.MyInvocation.BoundParameter["debug"].IsPresent) { Write-Output $DataSet }

    #Restart parameters
        $sqlQuery = $sqlDBName = $sqlHost = $sqlInstance = ""
        $sqlAdapter.Dispose()

    Return $dataSet
}

function MySql-QueryDB
{
    Param(
        [Parameter(
        Mandatory = $true,
        ParameterSetName = '',
        ValueFromPipeline = $true)]
        [string]$sqlQuery,

        [Parameter(
        Mandatory = $true,
        ParameterSetName = '',
        ValueFromPipeline = $true)]
        [string]$sqlHost,
      
        [Parameter(
        Mandatory = $true,
        ParameterSetName = '',
        ValueFromPipeline = $true)]
        [string]$sqlDBName
    )

    $Error.clear()

    $sqlUser = '********'
    $sqlPwd = '*******'
    $ConnectionString = "server="+$sqlHost + ";port=3306;uid="+$sqlUser + ";pwd="+$sqlPwd + ";database="+$sqlDBName + ";SslMode=none; Convert Zero Datetime=True"

    #$scriptFolder = (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)
    #Write-Host $scriptFolder

    Try {
        #Load MySQL Data DLL directly
            [void][system.reflection.Assembly]::LoadFrom("D:\SysAppl\Scripts\std\MySql.Data.dll")
        
        #Load MySQL Data DLL with MySQL .NET Connector installed
            #[void][System.Reflection.Assembly]::LoadWithPartialName("MySql.Data")

        $Connection = New-Object MySql.Data.MySqlClient.MySqlConnection
        $Connection.ConnectionString = $ConnectionString
        $Connection.Open()

        $Command = New-Object MySql.Data.MySqlClient.MySqlCommand($sqlQuery, $Connection)
        $DataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($Command)
        $DataSet = New-Object System.Data.DataSet
        $RecordCount = $dataAdapter.Fill($dataSet, "data")
        #Return $DataSet.Tables[0] #| ConvertTo-Html -Fragment
        Return $DataSet
    }

    Catch {
      Write-Host "ERROR : Unable to run query : $query `n$Error[0]"
    }

    Finally {
      $Connection.Close()
    }
}