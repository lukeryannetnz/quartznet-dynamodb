# quartznet-dynamo
Amazon DynamoDB jobstore for Quartz.NET

[![Build status](https://ci.appveyor.com/api/projects/status/mgrgaj6ox3yhmrgg?svg=true)](https://ci.appveyor.com/project/lukeryannetnz/quartznet-dynamodb) [Builds by AppVeyor](https://ci.appveyor.com/project/lukeryannetnz/quartznet-dynamodb)

## Overview

Quartz.NET Quartz.NET is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

This library provides a fully functioning Amazon DynamoDB JobStore for Quartz.NET using the AWS SDK Dynamo client. Inspired by Nanoko's Quartz.NET-MongoDB project. It is an adaptation of the original "RAMJobStore".

## Current State
* Jul-2016: Work in progress. Functioning! Completing functionality and polishing.
* Oct-2016: @ddhi004 joins project to work towards goal of publishing as complete nuget package. :-)
* Feb-2017: Published on nuget.org as beta package.
* Feb-2017: V1.0 on nuget.org!
* Mar-2017: Tweaks from hardening in production.

## Usage

### Install the package
Add the [nuget package](http://www.nuget.org/packages/QuartzNet-DynamoDB/).

### Configure the job store
Configure quartz.net to use the dynamodb job store.  
Code:  
`var properties = new NameValueCollection();`  <br/>
`properties[StdSchedulerFactory.PropertyJobStoreType] = typeof(Quartz.DynamoDB.JobStore).AssemblyQualifiedName;`  <br/>
`var schedulerFactory = new StdSchedulerFactory(properties);`  <br/>
`var scheduler = schedulerFactory.GetScheduler();`  <br/>
Configuration:  <br/>
`<configSections>`   <br/>
`	<section name="quartz" type="System.Configuration.NameValueSectionHandler, System, Version=1.0.5000.0,Culture=neutral,   PublicKeyToken=b77a5c561934e089" />`  <br/>
`</configSections>`  <br/>
`<quartz>`  <br/>
`	<add key="quartz.jobStore.type" value="Quartz.DynamoDB.JobStore, QuartzNET-DynamoDB" />`  <br/>
`</quartz>`  <br/>
Read more about configuring job stores in the [quartz.net documentation](https://www.quartz-scheduler.net/documentation/quartz-2.x/tutorial/job-stores.html).

### Configure the AWS SDK
There are a few ways to do this;
* Access key, secret key and region in the configuration file  <br/>
`<add key="AWSAccessKey" value="*******" />`  <br/>
`<add key="AWSSecretKey" value="*************" />`   <br/>
`<add key="AWSRegion" value="us-west-2" />`  <br/>
* A profile specified in the configuration file (or named default)
* Access key, secret key and region stored in environment variables
* An EC2 instance profile.

For more details see the [AWS documentation.](http://docs.aws.amazon.com/sdk-for-net/v2/developer-guide/net-dg-config-creds.html)

### DynamoDB Tables
The QuartzNet DynamoDB job store uses the following table names by default:
* QuartzScheduler.Calendar
* QuartzScheduler.Job
* QuartzScheduler.JobGroup
* QuartzScheduler.Scheduler
* QuartzScheduler.Trigger
* QuartzScheduler.TriggerGroup

If you run multiple scheduler instances, the instanceName will be used as the table prefix instead of "QuartzScheduler". You can set this in configuration:  <br/>
`<configSections>`  <br/>
`	<section name="quartz" type="System.Configuration.NameValueSectionHandler, System, Version=1.0.5000.0,Culture=neutral, PublicKeyToken=b77a5c561934e089" />`  <br/>
`</configSections>`  <br/>
`<quartz>`  <br/>
`		<add key="quartz.scheduler.instanceName" value="[YOURNAMEHERE]" />`  <br/>
`</quartz>`  <br/>

### Creating Tables
When the DynamoDB job store is initialised it will check if the required tables exist, if they do not it will attempt to create them and poll for them to be active before continuing. If you are running in a console, you'll see an output like the following:  <br/>
`21:29:36:169 [INFO]  Quartz.Core.QuartzScheduler - Quartz Scheduler v.2.5.0.0 created.`  <br/>
`Table QuartzScheduler.Job doesn't exist.`  <br/>
`Creating table QuartzScheduler.Job.`  <br/>
`Waiting for Table QuartzScheduler.Job to become active.`  <br/>
`Table QuartzScheduler.JobGroup doesn't exist.`  <br/>
`Creating table QuartzScheduler.JobGroup.`  <br/>
`Waiting for Table QuartzScheduler.JobGroup to become active.`  <br/>
`Table QuartzScheduler.Trigger doesn't exist.`  <br/>
`Creating table QuartzScheduler.Trigger.`  <br/>
`Waiting for Table QuartzScheduler.Trigger to become active.`  <br/>
`Table QuartzScheduler.TriggerGroup doesn't exist.`  <br/>
`Creating table QuartzScheduler.TriggerGroup.`  <br/>
`Waiting for Table QuartzScheduler.TriggerGroup to become active.`  <br/>
`Table QuartzScheduler.Scheduler doesn't exist.`  <br/>
`Creating table QuartzScheduler.Scheduler.`  <br/>
`Waiting for Table QuartzScheduler.Scheduler to become active.`  <br/>
`Table QuartzScheduler.Calendar doesn't exist.`  <br/>
`Creating table QuartzScheduler.Calendar.`  <br/>
`Waiting for Table QuartzScheduler.Calendar to become active.`  <br/>
Alternatively you may create the tables yourself before starting the scheduler. 

### Dynamo Capacity
The job store will create the tables with 1 unit of read and 1 unit of write capacity.
You may adjust the capacity of the tables, the only time the job store sets these values is when it creates the tables. 

## Contributing

Please see [contributing.md](/CONTRIBUTING.md).
