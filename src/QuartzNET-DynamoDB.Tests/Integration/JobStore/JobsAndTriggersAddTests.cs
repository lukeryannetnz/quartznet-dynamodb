using System;
using System.Collections.Generic;
using Quartz.Job;
using Quartz.Simpl;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Contains tests related to the addition of Jobs and Triggers.
    /// </summary>
    public class JobsAndTriggersAddTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;

        public JobsAndTriggersAddTests()
        {
            _sut = DynamoClientFactory.CreateTestJobStore();
            var signaler = new RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that after storing new jobs and triggers, they can be retrieved.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreNewJobsAndTriggers()
        {
            var jobdetail1 = TestJobFactory.CreateTestJob();
            var trigger1 = TestTriggerFactory.CreateTestTrigger(jobdetail1.Name, jobdetail1.Group);

            var jobdetail2 = TestJobFactory.CreateTestJob();
            var trigger2 = TestTriggerFactory.CreateTestTrigger(jobdetail2.Name, jobdetail2.Group);

            var triggersAndJobs = new Dictionary<IJobDetail, Collection.ISet<ITrigger>>
            {
                {jobdetail1, new Collection.HashSet<ITrigger>(new[] {trigger1})},
                {jobdetail2, new Collection.HashSet<ITrigger>(new[] {trigger2})}
            };

            _sut.StoreJobsAndTriggers(triggersAndJobs, true);

            var retrievedJob1 = _sut.RetrieveJob(new JobKey(jobdetail1.Name, jobdetail1.Group));
            Assert.NotNull(retrievedJob1);
            Assert.Equal(jobdetail1.Name, retrievedJob1.Key.Name);
            Assert.Equal(jobdetail1.Group, retrievedJob1.Key.Group);
            Assert.Equal(typeof(NoOpJob), retrievedJob1.JobType);

            var retrievedtriggers1 = _sut.GetTriggersForJob(jobdetail1.Key);
            Assert.Equal(1, retrievedtriggers1.Count);
            Assert.Equal(trigger1.Key, retrievedtriggers1[0].Key);

            var retrievedJob2 = _sut.RetrieveJob(new JobKey(jobdetail2.Name, jobdetail2.Group));
            Assert.NotNull(retrievedJob2);
            Assert.Equal(jobdetail2.Name, retrievedJob2.Key.Name);
            Assert.Equal(jobdetail2.Group, retrievedJob2.Key.Group);
            Assert.Equal(typeof(NoOpJob), retrievedJob2.JobType);

            var retrievedtriggers2 = _sut.GetTriggersForJob(jobdetail2.Key);
            Assert.Equal(1, retrievedtriggers2.Count);
            Assert.Equal(trigger2.Key, retrievedtriggers2[0].Key);
        }

        /// <summary>
        /// Tests that when storing an existing job with overwrite false an object already exists exception is thrown
        /// and no changes are made to the stored record.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreExistingJobOverwriteFalse()
        {
            const string initialJobDescription = "This is a job that is used to test.";

            var job = TestJobFactory.CreateTestJob();
            job.Description = initialJobDescription;
            _sut.StoreJob(job, false);

            job.Description += "UPDATED";

            var triggersAndJobs = new Dictionary<IJobDetail, Collection.ISet<ITrigger>>
            {
                {job, new Collection.HashSet<ITrigger>()},
            };

            // Throws because we have passed false for the replace parameter and the job already exists in dynamo.
            Assert.Throws<ObjectAlreadyExistsException>(() => { _sut.StoreJobsAndTriggers(triggersAndJobs, false); });

            var storedJob = _sut.RetrieveJob(job.Key);
            Assert.Equal(initialJobDescription, storedJob.Description);
        }

        #region IDisposable implementation

        bool _disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    DynamoClientFactory.CleanUpDynamo();

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