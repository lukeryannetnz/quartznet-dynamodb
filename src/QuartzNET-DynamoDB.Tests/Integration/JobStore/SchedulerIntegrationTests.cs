using System;
using Xunit;
using Quartz.DynamoDB.DataModel.Storage;
using Quartz.DynamoDB.DataModel;
using System.Linq;
using Quartz.Simpl;

namespace Quartz.DynamoDB.Tests
{
	public class SchedulerIntegrationTests
	{
		public SchedulerIntegrationTests ()
		{
		}
			
		[Fact]
		[Trait("Category", "Integration")]
		/// <summary>
		/// Ensures that only one scheduler record is created by the jobstore.
		/// Tests common JobStore methods that interact with the scheduler.
		/// </summary>
		public void SingleSchedulerCreated()
		{
			var sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
			var loadHelper = new SimpleTypeLoadHelper();

			sut.Initialize(loadHelper, signaler);
			var client = DynamoDbClientFactory.Create();
			var schedulerRepository = new Repository<DynamoScheduler> (client);

			int intialSchedulerCount = schedulerRepository.Scan (null, null, null).Count();

			sut.SchedulerStarted ();

			int schedulerStartedCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, schedulerStartedCount);

			sut.SchedulerPaused ();

			int schedulerPausedCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, schedulerPausedCount);

			sut.AcquireNextTriggers (new DateTimeOffset(DateTime.Now), 1, TimeSpan.FromMinutes(5));

			int triggersAcquiredCount = schedulerRepository.Scan (null, null, null).Count();

			Assert.Equal (intialSchedulerCount + 1, triggersAcquiredCount);

		}
	}
}

