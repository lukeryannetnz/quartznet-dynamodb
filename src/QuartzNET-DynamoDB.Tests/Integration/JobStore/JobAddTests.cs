using System;
using System.Collections.Generic;
using Quartz.DynamoDB.Tests.Integration;
using Quartz.Impl;
using Quartz.Job;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to the Addition of Jobs and Job Groups.
    /// </summary>
    public class JobAddTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;
        private readonly DynamoClientFactory _testFactory;

        public JobAddTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when CheckExists is called for a job that exists, it returns true.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void CheckExistsWhenJobExsits()
        {
            JobDetailImpl detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            bool result = _sut.CheckExists(detail.Key);
            Assert.True(result);
        }

        /// <summary>
        /// Tests that after storing a new job, that job can be retrieved
        /// with the same name, group and type.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreNewJob()
        {
            JobDetailImpl detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            var result = _sut.RetrieveJob(new JobKey(detail.Name, detail.Group));

            Assert.NotNull(result);
            Assert.Equal(detail.Name, result.Key.Name);
            Assert.Equal(detail.Group, result.Key.Group);
            Assert.Equal(typeof(NoOpJob), result.JobType);
        }

        /// <summary>
        /// Tests that after storing an exsting job, that job is updated if the replaceExisting param is passed as true.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreExistingJobOverwrite()
        {
            JobDetailImpl detail = TestJobFactory.CreateTestJob();
            detail.Description = "Original";

            _sut.StoreJob(detail, false);

            detail.Description = "Updated";
            _sut.StoreJob(detail, true);

            var result = _sut.RetrieveJob(new JobKey(detail.Name, detail.Group));

            Assert.Equal("Updated", result.Description);
        }

        /// <summary>
        /// Tests that after storing an exsting job, an exception is thrown if that job is updated and the replaceExisting param is passed as False.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreExistingJobDontOverwriteThrows()
        {
            JobDetailImpl detail = TestJobFactory.CreateTestJob();
            _sut.StoreJob(detail, false);

            Assert.Throws<ObjectAlreadyExistsException>(() => _sut.StoreJob(detail, false));
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

