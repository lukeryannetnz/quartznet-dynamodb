# Runs nuget package restore for mono. Assumes mono is on the path and installed via MonoDevelop/Xamarin Studio.

nuget install ./src/QuartzNET-DynamoDB/packages.config -OutputDirectory ./src/packages
nuget install ./src/QuartzNET-DynamoDB.Tests/packages.config -OutputDirectory ./src/packages