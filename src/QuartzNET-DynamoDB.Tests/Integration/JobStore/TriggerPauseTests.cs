using System;
using Xunit;
using Quartz.Simpl;
using Quartz.Spi;
using System.Linq;
using Quartz.Impl.Triggers;
using Quartz.Impl;
using Quartz.Job;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to the Pausing of Triggers and Trigger Groups.
    /// </summary>
    public class TriggerPauseTests : JobStoreIntegrationTest
    {
        public TriggerPauseTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when Pause triggers is called with a group matcher equalling a trigger group, one trigger group is paused.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersOneGroupEquals()
        {
            string triggerGroup = Guid.NewGuid().ToString();
            var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            Assert.Equal(1, result.Count);
            Assert.Equal(triggerGroup, result.Single());
        }

        /// <summary>
        /// Tests that when Pause triggers is called with a group matcher starts with and no groups match, then 0 should be returned.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersStartsWithNoMatches()
        {
            string triggerGroup = Guid.NewGuid().ToString();

            var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupStartsWith(triggerGroup.Substring(0, 8)));
            Assert.Equal(0, result.Count);
        }

        /// <summary>
        /// Tests that when Pause triggers is called with a group matcher starts with and one groups matches, 
        /// then that group should be paused,
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersStartsWithOneMatch()
        {
            // Create a random job, store it.
            string jobName = Guid.NewGuid().ToString();
            JobDetailImpl detail = new JobDetailImpl(jobName, "JobGroup", typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group.
            IOperableTrigger tr = TestTriggerFactory.CreateTestTrigger(jobName);
            var triggerGroup = tr.Key.Group;
            _sut.StoreTrigger(tr, false);

            var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupStartsWith(triggerGroup.Substring(0, 8)));
            Assert.Equal(1, result.Count);
        }

        /// <summary>
        /// Tests that when Pause triggers is called for a trigger group that doesn't exist, that trigger group is stored
        /// paused so that future triggers added against it are paused. 
        /// This feels odd, but simulates the behaviour of the mongodb job store.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersNewTriggerGroup()
        {
            // Pause triggers for the (new) trigger group
            string triggerGroup = Guid.NewGuid().ToString();
            _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            // Create a random job, store it.
            string jobName = Guid.NewGuid().ToString();
            JobDetailImpl detail = new JobDetailImpl(jobName, "JobGroup", typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group that is paused.
            IOperableTrigger tr = new SimpleTriggerImpl("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
            _sut.StoreTrigger(tr, false);

            var triggerState = _sut.GetTriggerState(tr.Key);
            Assert.Equal("Paused", triggerState.ToString());
        }

        /// <summary>
        /// Tests that when Pause triggers is called for a trigger group that exists and has a trigger in it, 
        /// the triggers in that group should be paused.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersExistingTriggerGroup()
        {
            string triggerGroup = Guid.NewGuid().ToString();

            // Create a random job, store it.
            string jobName = Guid.NewGuid().ToString();
            JobDetailImpl detail = new JobDetailImpl(jobName, "JobGroup", typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group.
            IOperableTrigger tr = new SimpleTriggerImpl("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
            _sut.StoreTrigger(tr, false);

            // Trigger should be waiting to be picked up.
            var triggerState = _sut.GetTriggerState(tr.Key);
            Assert.Equal("Normal", triggerState.ToString());

            _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            triggerState = _sut.GetTriggerState(tr.Key);
            Assert.Equal("Paused", triggerState.ToString());
        }

        /// <summary>
        /// Tests that when PauseAll is called, the triggers in all trigger groups are paused.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseAll()
        {
            var triggerGroup1 = Guid.NewGuid().ToString();
            var triggerGroup2 = Guid.NewGuid().ToString();

            // Create a random job, store it.
            var jobName = Guid.NewGuid().ToString();
            var detail = new JobDetailImpl(jobName, "JobGroup", typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            // Create a trigger for the job, in the trigger group.
            var tr1 = new SimpleTriggerImpl("test1", triggerGroup1, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
            _sut.StoreTrigger(tr1, false);

            // Create another trigger for the job, in another trigger group.
            var tr2 = new SimpleTriggerImpl("test2", triggerGroup2, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
            _sut.StoreTrigger(tr2, false);

            _sut.PauseAll();

            // Ensure all triggers in all trigger groups have been paused
            var triggerState1 = _sut.GetTriggerState(tr1.Key);
            Assert.Equal("Paused", triggerState1.ToString());

            var triggerState2 = _sut.GetTriggerState(tr2.Key);
            Assert.Equal("Paused", triggerState2.ToString());
        }

        /// <summary>
        /// Tests that when Pause triggers is called IsTriggerGroup paused returns true.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseTriggersIsTriggerPaused()
        {
            string triggerGroup = Guid.NewGuid().ToString();

            var paused = _sut.IsTriggerGroupPaused(triggerGroup);
            Assert.Equal(false, paused);

            _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            paused = _sut.IsTriggerGroupPaused(triggerGroup);
            Assert.Equal(true, paused);
        }
    }
}

