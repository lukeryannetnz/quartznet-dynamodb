using System;
using Xunit;
using Quartz.DynamoDB.DataModel.Storage;
using Quartz.DynamoDB.DataModel;
using System.Linq;
using Quartz.Simpl;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    public class SchedulerIntegrationTests : IDisposable
	{
        private readonly DynamoDB.JobStore _sut;

		public SchedulerIntegrationTests ()
		{
            _sut = TestJobStoreFactory.CreateTestJobStore();
		}
			
		[Fact]
		[Trait("Category", "Integration")]
		/// <summary>
		/// Ensures that only one scheduler record is created by the jobstore.
		/// Tests common JobStore methods that interact with the scheduler.
		/// </summary>
		public void SingleSchedulerCreated()
		{
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
			var loadHelper = new SimpleTypeLoadHelper();

			_sut.Initialize(loadHelper, signaler);
			var client = DynamoDbClientFactory.Create();
			var schedulerRepository = new Repository<DynamoScheduler> (client);

			int intialSchedulerCount = schedulerRepository.Scan (null, null, null).Count();

			_sut.SchedulerStarted ();

			int schedulerStartedCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, schedulerStartedCount);

			_sut.SchedulerPaused ();

			int schedulerPausedCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, schedulerPausedCount);

			_sut.AcquireNextTriggers (new DateTimeOffset(DateTime.Now), 1, TimeSpan.FromMinutes(5));

			int triggersAcquiredCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, triggersAcquiredCount);

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

