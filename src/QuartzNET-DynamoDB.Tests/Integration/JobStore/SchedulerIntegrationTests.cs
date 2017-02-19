using System;
using Xunit;
using Quartz.DynamoDB.DataModel.Storage;
using Quartz.DynamoDB.DataModel;
using System.Linq;
using Quartz.Simpl;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
    public class SchedulerIntegrationTests : JobStoreIntegrationTest
    {
        public SchedulerIntegrationTests()
        {
            _testFactory = new DynamoClientFactory();
            _sut = _testFactory.CreateTestJobStore();
        }

        /// <summary>
        /// Ensures that only one scheduler record is created by the jobstore.
        /// Tests common JobStore methods that interact with the scheduler.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
         public void SingleSchedulerCreated()
        {
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
            var client = DynamoDbClientFactory.Create();
            var schedulerRepository = new Repository<DynamoScheduler>(client);

            int intialSchedulerCount = schedulerRepository.Scan(null, null, null).Count();

            _sut.SchedulerStarted();

            int schedulerStartedCount = schedulerRepository.Scan(null, null, null).Count();

            Assert.Equal(intialSchedulerCount + 1, schedulerStartedCount);

            _sut.SchedulerPaused();

            int schedulerPausedCount = schedulerRepository.Scan(null, null, null).Count();

            Assert.Equal(intialSchedulerCount + 1, schedulerPausedCount);

            _sut.AcquireNextTriggers(new DateTimeOffset(DateTime.Now), 1, TimeSpan.FromMinutes(5));

            int triggersAcquiredCount = schedulerRepository.Scan(null, null, null).Count();

            Assert.Equal(intialSchedulerCount + 1, triggersAcquiredCount);

        }
    }
}

