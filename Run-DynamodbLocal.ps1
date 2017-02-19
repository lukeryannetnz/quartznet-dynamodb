
<# .SYNOPSIS
     Downloads and runs the local dynamo db application.
.DESCRIPTION
     Only downloads if it doesn't exist.
	 Creates the .\dynamodb_local directory if it doesn't exist. Downloads to here.
	 Assumes you have a JVM installed.
#>
if(!(Test-Path ".\dynamodb_local\dynamodb_local_latest.zip"))
{
	mkdir -force "dynamodb_local"
	write-host "Downloading DynamoDB binaries"
	Invoke-WebRequest "https://s3-ap-southeast-1.amazonaws.com/dynamodb-local-singapore/dynamodb_local_latest.zip" -OutFile ".\dynamodb_local\dynamodb_local_latest.zip"

	Expand-Archive  ".\dynamodb_local\dynamodb_local_latest.zip" ".\dynamodb_local"
}

java.exe --% -Djava.library.path=./dynamodb_local/DynamoDBLocal_lib -jar ./dynamodb_local/DynamoDBLocal.jar -inMemory