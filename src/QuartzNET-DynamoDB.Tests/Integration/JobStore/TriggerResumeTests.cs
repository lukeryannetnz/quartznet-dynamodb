using System;
using Xunit;
using Quartz.Simpl;
using Quartz.Spi;
using Quartz.Impl.Triggers;
using Quartz.Impl;
using Quartz.Job;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to Resuming Triggers and Trigger Groups.
    /// </summary>
    public class TriggerResumeTests
    {
        IJobStore _sut;

        public TriggerResumeTests()
        {
            _sut = new Quartz.DynamoDB.JobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when Resume triggers is called for a paused trigger group, 
        /// the triggers in that group are resumed.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeTriggersExistingTriggerGroup()
        {
            var triggerGroup = Guid.NewGuid().ToString();

            // Create a random job, store it.
            string jobName = Guid.NewGuid().ToString();
            var detail = new JobDetailImpl(jobName, "JobGroup", typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group.
            IOperableTrigger tr = new SimpleTriggerImpl("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
            _sut.StoreTrigger(tr, false);

            // Pause the triggers and ensure they are paused.
            _sut.PauseTriggers(Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));
            var triggerState = _sut.GetTriggerState(tr.Key);
            Assert.Equal("Paused", triggerState.ToString());

            _sut.ResumeTriggers(Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            // Check the trigger has been resumed
            triggerState = _sut.GetTriggerState(tr.Key);
            Assert.Equal("Normal", triggerState.ToString());
        }
    }
}

