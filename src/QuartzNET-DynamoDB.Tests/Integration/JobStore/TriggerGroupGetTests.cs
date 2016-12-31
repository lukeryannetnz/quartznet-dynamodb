using System;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    public class TriggerGroupGetTests : IDisposable
    {
        private readonly DynamoDB.JobStore _sut;

        public TriggerGroupGetTests()
        {
            _sut = DynamoClientFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Get paused trigger groups returns one record.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void GetPausedTriggerGroupReturnsOneRecord()
        {
            //create a trigger group by calling for it to be paused.
            string triggerGroup = Guid.NewGuid().ToString();
            _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

            var result = _sut.GetPausedTriggerGroups();

            Assert.True(result.Contains(triggerGroup));
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

