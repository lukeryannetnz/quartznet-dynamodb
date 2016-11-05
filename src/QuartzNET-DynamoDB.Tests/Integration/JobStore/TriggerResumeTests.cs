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

        /// <summary>
        /// Tests that when ResumeAll is called, the triggers in all trigger groups are resumed.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeAll()
        {
            // Create a random job, store it.
            var detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group.
            var tr1 = TestTriggerFactory.CreateTestTrigger(detail.Name, detail.Group);
            _sut.StoreTrigger(tr1, false);

            // Create another trigger for the job, in another trigger group.
            var tr2 = TestTriggerFactory.CreateTestTrigger(detail.Name, detail.Group);
            _sut.StoreTrigger(tr2, false);

            // Pause all triggers and check they have been paused
            _sut.PauseAll();
            var triggerState1 = _sut.GetTriggerState(tr1.Key);
            Assert.Equal("Paused", triggerState1.ToString());
            var triggerState2 = _sut.GetTriggerState(tr2.Key);
            Assert.Equal("Paused", triggerState2.ToString());
            
            _sut.ResumeAll();

            // Ensure all triggers have been resumed
            triggerState1 = _sut.GetTriggerState(tr1.Key);
            Assert.Equal("Normal", triggerState1.ToString());

            triggerState2 = _sut.GetTriggerState(tr2.Key);
            Assert.Equal("Normal", triggerState2.ToString());
        }
    }
}

