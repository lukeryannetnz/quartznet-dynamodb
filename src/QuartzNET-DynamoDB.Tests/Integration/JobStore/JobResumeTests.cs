using System;
using System.Linq;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to the Resumption of Jobs and Job Groups.
    /// </summary>
    public class JobResumeTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;

        public JobResumeTests()
        {
            _sut = TestJobStoreFactory.CreateTestJobStore();
            var signaler = new RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when a paused job with one trigger is resumed, the trigger state is set back to Normal.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobResumesTrigger()
        {
            var job = TestJobFactory.CreateTestJob();
            var trigger = TestTriggerFactory.CreateTestTrigger(job.Name, job.Group);

            _sut.StoreJobAndTrigger(job, trigger);
            _sut.PauseJob(job.Key);

            var state = _sut.GetTriggerState(trigger.Key);
            Assert.Equal(TriggerState.Paused, state);

            _sut.ResumeJob(job.Key);

            state = _sut.GetTriggerState(trigger.Key);
            Assert.Equal(TriggerState.Normal, state);
        }

        /// <summary>
        /// Tests that when Resume Jobs is called with a group matcher equalling a job group, one job group is returned.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobsOneGroupEquals()
        {
            var jobGroup = Guid.NewGuid().ToString();

            var result = _sut.ResumeJobs(Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));

            Assert.Equal(1, result.Count);
            Assert.Equal(jobGroup, result.Single());
        }

        /// <summary>
        /// Tests that when Resume jobs is called with a group matcher starts with and no groups match, then 0 should be returned.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobsStartsWithNoMatches()
        {
            var jobGroup = Guid.NewGuid().ToString();

            var result = _sut.ResumeJobs(Impl.Matchers.GroupMatcher<JobKey>.GroupStartsWith(jobGroup.Substring(0, 8)));
            Assert.Equal(0, result.Count);
        }

        /// <summary>
        /// Tests that when Resume jobs is called with a group matcher starts with and one group matches, 
        /// then that group should be returned,
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobsStartsWithOneMatch()
        {
            // Create a random job, store it.
            var detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            var result = _sut.ResumeJobs(Impl.Matchers.GroupMatcher<JobKey>.GroupStartsWith(detail.Group.Substring(0, 8)));
            Assert.Equal(1, result.Count);
        }

        /// <summary>
        /// Tests that when Resume Jobs is called IsJobGroupPaused returns false.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobsGroups()
        {
            // Pause a job group and ensure its pasued
            var jobGroup = Guid.NewGuid().ToString();
            _sut.PauseJobs(Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));
            var paused = _sut.IsJobGroupPaused(jobGroup);
            Assert.Equal(true, paused);

            _sut.ResumeJobs(Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));

            // Check the job group is resumed
            paused = _sut.IsJobGroupPaused(jobGroup);
            Assert.Equal(false, paused);
        }

        #region IDisposable implementation

        bool _disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _sut.ClearAllSchedulingData();
                }

                _disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }

        #endregion
    }
}