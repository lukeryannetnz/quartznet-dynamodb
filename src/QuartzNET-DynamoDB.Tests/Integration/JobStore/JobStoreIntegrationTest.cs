using System;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    /// <summary>
    /// Abstract class that provides common dynamo db cleanup for JobStore integration testing.
    /// </summary>
    public abstract class JobStoreIntegrationTest : IDisposable
    {
        protected DynamoDB.JobStore _sut;

        protected DynamoClientFactory _testFactory;

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
