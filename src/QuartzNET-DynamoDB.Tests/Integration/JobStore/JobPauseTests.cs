using System;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;
using System.Linq;
using Quartz.Impl;
using Quartz.Job;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to the Pausing of Jobs and Job Groups.
    /// </summary>
    public class JobPauseTests
    {
        IJobStore _sut;

        public JobPauseTests()
        {
            _sut = new Quartz.DynamoDB.JobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when Pause Jobs is called with a group matcher equalling a job group, one job group is paused.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseJobsOneGroupEquals()
        {
            string jobGroup = Guid.NewGuid().ToString();
            var result = _sut.PauseJobs(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));

            Assert.Equal(1, result.Count);
            Assert.Equal(jobGroup, result.Single());
        }

        /// <summary>
        /// Tests that when Pause jobs is called with a group matcher starts with and no groups match, then 0 should be returned.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseJobsStartsWithNoMatches()
        {
            string jobGroup = Guid.NewGuid().ToString();

            var result = _sut.PauseJobs(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupStartsWith(jobGroup.Substring(0, 8)));
            Assert.Equal(0, result.Count);
        }

        /// <summary>
        /// Tests that when Pause jobs is called with a group matcher starts with and one groups matches, 
        /// then that group should be paused,
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseJobsStartsWithOneMatch()
        {
            string jobGroup = Guid.NewGuid().ToString();
            // Create a random job, store it.
            string jobName = Guid.NewGuid().ToString();
            JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            _sut.StoreJob(detail, false);

            var result = _sut.PauseJobs(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupStartsWith(jobGroup.Substring(0, 8)));
            Assert.Equal(1, result.Count);
        }

        /// <summary>
        /// Tests that when Pause Jobs is called IsJobGroupPaused returns true.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void PauseJobsGroups()
        {
            string jobGroup = Guid.NewGuid().ToString();

            var paused = _sut.IsJobGroupPaused(jobGroup);
            Assert.Equal(false, paused);

            _sut.PauseJobs(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));

            paused = _sut.IsJobGroupPaused(jobGroup);
            Assert.Equal(true, paused);
        }
    }
}

