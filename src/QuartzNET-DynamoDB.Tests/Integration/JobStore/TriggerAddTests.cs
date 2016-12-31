using System;
using Quartz.Simpl;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    public class TriggerAddTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;
        private readonly DynamoClientFactory _testFactory;

        public TriggerAddTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when replace trigger is called and the new trigger is for a different job, an exception is thrown.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void TriggerReplaceDifferentJobFails()
        {
            var job = TestJobFactory.CreateTestJob();
            var trigger = TestTriggerFactory.CreateTestTrigger(job.Name, job.Group);
            _sut.StoreJobAndTrigger(job, trigger);

            var newTrigger = TestTriggerFactory.CreateTestTrigger("New Job That Doesn't Exist", job.Group);

            Assert.Throws<JobPersistenceException>(() => { _sut.ReplaceTrigger(trigger.Key, newTrigger); });
        }

        /// <summary>
        /// Tests that when replace trigger is called, if the trigger can't be found then false is returned.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void TriggerNotFound()
        {
            var newTrigger = TestTriggerFactory.CreateTestTrigger("Job Name", "New Job Group");

            // newTrigger isn't in the database so can't be stored.
            var result = _sut.ReplaceTrigger(newTrigger.Key, newTrigger);

            Assert.False(result);
        }

        /// <summary>
        /// Tests that when replace trigger is called with a new trigger for the same job, the old trigger is removed and the new one stored.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void TriggerReplaceHappyPath()
        {
            var job = TestJobFactory.CreateTestJob();
            var trigger = TestTriggerFactory.CreateTestTrigger(job.Name, job.Group);
            _sut.StoreJobAndTrigger(job, trigger);

            var newTrigger = TestTriggerFactory.CreateTestTrigger(job.Name, job.Group);

            var result = _sut.ReplaceTrigger(trigger.Key, newTrigger);

            Assert.True(result);

            var oldTriggerState = _sut.GetTriggerState(trigger.Key);
            Assert.Equal(TriggerState.None, oldTriggerState);

            var newTriggerState = _sut.GetTriggerState(newTrigger.Key);
            Assert.Equal(TriggerState.Normal, newTriggerState);
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

