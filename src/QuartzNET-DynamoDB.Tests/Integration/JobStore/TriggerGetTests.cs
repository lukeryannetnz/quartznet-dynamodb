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

		public GetTriggerTests ()
		{
			_sut = new Quartz.DynamoDB.JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		[Fact]
		[Trait ("Category", "Integration")]
		public void GetNumberOfTriggersIncrementsWhenTriggerAdded ()
		{
			int triggerCount = _sut.GetNumberOfTriggers();

			var job = TestJobFactory.CreateTestJob ();
			var trigger = TestTriggerFactory.CreateTestTrigger (job.Name, job.Group);
			_sut.StoreJobAndTrigger (job, trigger);

			// Dynamo describe table is eventually consistent so give it a little time. Flaky I know, but hey - what are you going to do?
			Thread.Sleep (500);

			int newTriggerCount = _sut.GetNumberOfTriggers ();
			Assert.Equal (triggerCount + 1, newTriggerCount);
		}

        public void Dispose()
        {
            _sut.Dispose();
        }
	}
}

