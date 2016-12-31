using System;
using System.Threading;
using Quartz.DynamoDB.Tests.Integration;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    public class GetTriggerTests : IDisposable
	{
        private readonly DynamoDB.JobStore _sut;
        private readonly DynamoClientFactory _testFactory;

		public GetTriggerTests ()
		{
			_testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		//[Fact]
		[Trait ("Category", "Integration")]
		public void GetNumberOfTriggersIncrementsWhenTriggerAdded ()
		{
			int triggerCount = _sut.GetNumberOfTriggers();

			var job = TestJobFactory.CreateTestJob ();
			var trigger = TestTriggerFactory.CreateTestTrigger (job.Name, job.Group);
			_sut.StoreJobAndTrigger (job, trigger);

			// Dynamo describe table is eventually consistent so give it a little time. Flaky I know, but hey - what are you going to do?
			Thread.Sleep(5000);

			int newTriggerCount = _sut.GetNumberOfTriggers ();
			Assert.Equal (triggerCount + 1, newTriggerCount);
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

