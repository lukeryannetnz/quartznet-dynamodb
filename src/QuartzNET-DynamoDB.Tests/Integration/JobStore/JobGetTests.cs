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
    public class JobGetTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;
        private readonly DynamoClientFactory _testFactory;

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

        #region IDisposable implementation

        bool _disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _testFactory.CleanUpDynamo();

                    if (_sut != null)
                    {
                        _sut.Dispose();
                    }
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

