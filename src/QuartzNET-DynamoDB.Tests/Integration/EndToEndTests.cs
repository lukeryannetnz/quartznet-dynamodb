namespace Quartz.DynamoDB.Tests.Integration
{
    using System;
    using System.Collections.Specialized;
    using System.Threading;

    using Quartz.Impl;
    using Quartz.Job;

    using Xunit;

    /// <summary>
    /// These are end to end acceptance tests that use quartz to exercise the
    /// JobStore.
    /// </summary>
    public class EndToEndTests
    {
        /// <summary>
        /// Test to try to reproduce and verify issue 65, NextFireTimeUtc is not updated.
        /// <see href="https://github.com/lukeryannetnz/quartznet-dynamodb/issues/65"/>
        /// </summary>
        [Fact]
        public void Bug65()
        {
            var scheduler = CreateDynamoBackedScheduler();

            IJobDetail job = CreateNoOpJob();

            // Trigger the job to run now, and then repeat every 10 seconds
            ITrigger trigger = CreateOneSecondTrigger();

            scheduler.ScheduleJob(job, trigger);

            var nextFireTime = scheduler.GetTrigger(trigger.Key).GetNextFireTimeUtc();
            Console.WriteLine("NextFireTime: {0}", nextFireTime);

            scheduler.Start();

            // Give the job time to execute in the quartz thread. Ewww.
            Thread.Sleep(2000);

            var updatedNextFireTime = scheduler.GetTrigger(trigger.Key).GetNextFireTimeUtc();
            Console.WriteLine("UpdatedNextFireTime: {0}", nextFireTime);

            Assert.True(updatedNextFireTime > nextFireTime);
        }

        private static IJobDetail CreateNoOpJob()
        {
            var randomName = Guid.NewGuid().ToString();

            // define the job and tie it to our HelloJob class
            var job = JobBuilder.Create<NoOpJob>()
                .WithIdentity("TestJob" + randomName, "TestJobGroup")
                .Build();

            return job;
        }

        private static ITrigger CreateOneSecondTrigger()
        {
            var randomName = Guid.NewGuid().ToString();

            var trigger = TriggerBuilder.Create()
                .WithIdentity("OneSecondTrigger" + randomName, "SimpleTriggersGroup")
                .StartNow()
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(1)
                    .RepeatForever())
                .Build();

            return trigger;
        }

        private static IScheduler CreateDynamoBackedScheduler()
        {
            var properties = new NameValueCollection
            {
                [StdSchedulerFactory.PropertyJobStoreType] =
                                     typeof(DynamoDB.JobStore).AssemblyQualifiedName
            };
            var schedulerFactory = new StdSchedulerFactory(properties);

            return schedulerFactory.GetScheduler();
        }
    }
}
