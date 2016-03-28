# quartznet-dynamo
Amazon DynamoDB jobstore for Quartz.NET

![Travis build status](https://travis-ci.org/lukeryannetnz/quartznet-dynamodb.svg?branch=master) [Builds by Travis-CI](https://travis-ci.org/lukeryannetnz/quartznet-dynamodb)

##Overview

Quartz.NET Quartz.NET is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

Aiming to provide a fully functioning Amazon DynamoDB JobStore for Quartz.NET using the AWS SDK Dynamo client. Inspired by Nanoko's Quartz.NET-MongoDB project. It is an adaptation of the original "RAMJobStore".

##Current State
Feb-2016: Work in progress.

##Building locally

###Compiling
####Windows/Visual Studio
If you're using windows/visual studio 2015 src/QuartzNET-DynamoDB.sln is what you're after. The solution file has nuget package restore on build so you should be good to go.

####OSX/Linux
If you're using linux/osx this library compiles under mono. Our build infrastructure (travis-ci) uses a debian. Check out http://www.mono-project.com/. You'll want to install mono and make sure it's in your path as a minimum.
There are some shell scripts to help you with common tasks if you're that way inclined:
`./mono-nugetpackagerestore.sh` Restores nuget packages by invoking nuget.exe via mono
`./mono-compile.sh` Compiles the solution in release configuration using mono xbuild.

###Testing
The solution uses xunit tests extensively. These are broken into two groups:
####Unit tests
These are small, target one class, where possible test state not behaviour and run entirely in memory. These are marked with the xunit trait Category=Unit.
####Integration tests
These are more complex tests that test behavior. The initial integration tests were ported directly from the Quartz-NET RAMJobStoreTests. These require an instance of DynamoDB to test against. Amazon provide a java stand-alone version of dynamo, see [Running DynamoDB on your computer](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html). These are marked with the xunit trait Category=Integration.
