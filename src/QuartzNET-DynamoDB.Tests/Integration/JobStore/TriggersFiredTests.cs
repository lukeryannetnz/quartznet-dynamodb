using System;
using Xunit;
using Quartz.Impl;
using Quartz.Job;
using Quartz.Spi;
using Quartz.Simpl;
using Quartz.Impl.Triggers;
using System.Collections.Generic;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests for the JobStore when triggers are fired.
    /// </summary>
    public class JobStoreTriggersFiredTests : JobStoreIntegrationTest
    {
        public JobStoreTriggersFiredTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that a single trigger can be fired successfully and the trigger and job keys are returned correctly
        /// when it is.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void SingleTriggerFiredSuccessfully()
        {
            string jobName = Guid.NewGuid().ToString();
            string jobGroup = Guid.NewGuid().ToString();
            string triggerName = Guid.NewGuid().ToString();
            string triggerGroup = Guid.NewGuid().ToString();
            DateTimeOffset d = DateTime.UtcNow;

            JobDetailImpl job = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            IOperableTrigger trigger = new SimpleTriggerImpl(triggerName, triggerGroup, job.Name, job.Group, d, null, 2, TimeSpan.FromSeconds(5));
            trigger.ComputeFirstFireTimeUtc(null);

            _sut.StoreJobAndTrigger(job, trigger);

            var acquired = _sut.AcquireNextTriggers(d.AddSeconds(9), 100, TimeSpan.FromSeconds(5));
            Assert.True(acquired.Count > 0);

            var result = _sut.TriggersFired(new List<IOperableTrigger>() { trigger });

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            Assert.Equal(triggerName, result[0].TriggerFiredBundle.Trigger.Key.Name);
            Assert.Equal(triggerGroup, result[0].TriggerFiredBundle.Trigger.Key.Group);
            Assert.Equal(jobName, result[0].TriggerFiredBundle.JobDetail.Key.Name);
            Assert.Equal(jobGroup, result[0].TriggerFiredBundle.JobDetail.Key.Group);
        }

        /// <summary>
        /// Tests that a single Cron trigger can be fired successfully and the trigger and job keys are returned correctly
        /// when it is.
        /// <see href="https://github.com/lukeryannetnz/quartznet-dynamodb/issues/61"/>
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void CronTriggerFiredSuccessfully()
        {
            string jobName = Guid.NewGuid().ToString();
            string jobGroup = Guid.NewGuid().ToString();
            string triggerName = Guid.NewGuid().ToString();
            string triggerGroup = Guid.NewGuid().ToString();

            // http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/tutorial-lesson-06.html
            string cronExpression = "0/5 * * * * ?";

            DateTimeOffset d = DateTime.UtcNow;

            JobDetailImpl job = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            IOperableTrigger trigger = new CronTriggerImpl(triggerName, triggerGroup, job.Name, job.Group, cronExpression);

            trigger.ComputeFirstFireTimeUtc(null);

            _sut.StoreJobAndTrigger(job, trigger);

            var acquired = _sut.AcquireNextTriggers(d.AddSeconds(9), 100, TimeSpan.FromSeconds(5));
            Assert.True(acquired.Count > 0);

            var result = _sut.TriggersFired(new List<IOperableTrigger>() { trigger });

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            Assert.Equal(triggerName, result[0].TriggerFiredBundle.Trigger.Key.Name);
            Assert.Equal(triggerGroup, result[0].TriggerFiredBundle.Trigger.Key.Group);
            Assert.Equal(jobName, result[0].TriggerFiredBundle.JobDetail.Key.Name);
            Assert.Equal(jobGroup, result[0].TriggerFiredBundle.JobDetail.Key.Group);
        }


        /// <summary>
        /// Tests that a single Daily Time Interval trigger can be fired successfully and the trigger and job keys are returned correctly
        /// when it is.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void DailyTimeIntervalTriggerFiredSuccessfully()
        {
            string jobName = Guid.NewGuid().ToString();
            string jobGroup = Guid.NewGuid().ToString();
            string triggerName = Guid.NewGuid().ToString();
            string triggerGroup = Guid.NewGuid().ToString();

            DateTimeOffset d = DateTime.UtcNow;

            JobDetailImpl job = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            DateTime nowUtc = DateTime.UtcNow;
            IOperableTrigger trigger = new DailyTimeIntervalTriggerImpl(triggerName, triggerGroup, job.Name, job.Group, new DateTimeOffset(DateTime.UtcNow), null, Quartz.TimeOfDay.HourMinuteAndSecondOfDay(0, 0, 0), Quartz.TimeOfDay.HourAndMinuteOfDay(23, 59), IntervalUnit.Second, 1);

            trigger.ComputeFirstFireTimeUtc(null);

            _sut.StoreJobAndTrigger(job, trigger);

            var acquired = _sut.AcquireNextTriggers(d.AddSeconds(9), 100, TimeSpan.FromSeconds(5));
            Assert.True(acquired.Count > 0);

            var result = _sut.TriggersFired(new List<IOperableTrigger>() { trigger });

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            Assert.Equal(triggerName, result[0].TriggerFiredBundle.Trigger.Key.Name);
            Assert.Equal(triggerGroup, result[0].TriggerFiredBundle.Trigger.Key.Group);
            Assert.Equal(jobName, result[0].TriggerFiredBundle.JobDetail.Key.Name);
            Assert.Equal(jobGroup, result[0].TriggerFiredBundle.JobDetail.Key.Group);
        }

        /// <summary>
        /// Tests that a single Calendar Interval trigger can be fired successfully and the trigger and job keys are returned correctly
        /// when it is.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void CalendarIntervalTriggerFiredSuccessfully()
        {
            string jobName = Guid.NewGuid().ToString();
            string jobGroup = Guid.NewGuid().ToString();
            string triggerName = Guid.NewGuid().ToString();
            string triggerGroup = Guid.NewGuid().ToString();

            DateTimeOffset d = DateTime.UtcNow;

            JobDetailImpl job = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            DateTime nowUtc = DateTime.UtcNow;
            IOperableTrigger trigger = new CalendarIntervalTriggerImpl(triggerName, triggerGroup, jobName, jobGroup, d, null, IntervalUnit.Second, 1);
            trigger.ComputeFirstFireTimeUtc(null);

            _sut.StoreJobAndTrigger(job, trigger);

            var acquired = _sut.AcquireNextTriggers(d.AddSeconds(9), 100, TimeSpan.FromSeconds(5));
            Assert.True(acquired.Count > 0);

            var result = _sut.TriggersFired(new List<IOperableTrigger>() { trigger });

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            Assert.Equal(triggerName, result[0].TriggerFiredBundle.Trigger.Key.Name);
            Assert.Equal(triggerGroup, result[0].TriggerFiredBundle.Trigger.Key.Group);
            Assert.Equal(jobName, result[0].TriggerFiredBundle.JobDetail.Key.Name);
            Assert.Equal(jobGroup, result[0].TriggerFiredBundle.JobDetail.Key.Group);
        }
    }
}

