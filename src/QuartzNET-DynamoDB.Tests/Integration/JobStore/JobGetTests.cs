using System;
using System.Threading;
using Quartz.Impl;
using Quartz.Simpl;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to retrieving Jobs and Job Groups.
    /// </summary>
    public class JobGetTests : JobStoreIntegrationTest
    {
        public JobGetTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that after a job is added, the number of jobs increments.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void GetNumberOfJobsIncrementsWhenJobAdded()
        {
            var jobCount = _sut.GetNumberOfJobs();

            JobDetailImpl detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            // Dynamo describe table is eventually consistent so give it a little time. Flaky I know, but hey - what are you going to do?
            Thread.Sleep(5000);

            var newCount = _sut.GetNumberOfJobs();

            Assert.Equal(jobCount + 1, newCount);

        }
    }
}

