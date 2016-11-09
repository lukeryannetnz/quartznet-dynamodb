using System;
using System.Threading;
using Quartz.DynamoDB.Tests.Integration;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
	/// <summary>
	/// Contains tests related to retrieving Jobs and Job Groups.
	/// </summary>
    public class JobGetTests : IDisposable
	{
        private readonly DynamoDB.JobStore _sut;

		public JobGetTests ()
		{
			_sut = new Quartz.DynamoDB.JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		/// <summary>
		/// Tests that after a job is added, the number of jobs increments.
		/// </summary>
		[Fact]
		[Trait ("Category", "Integration")]
		public void GetNumberOfJobsIncrementsWhenJobAdded ()
		{
			var jobCount = _sut.GetNumberOfJobs();

			JobDetailImpl detail = TestJobFactory.CreateTestJob ();
			_sut.StoreJob (detail, false);

			// Dynamo describe table is eventually consistent so give it a little time. Flaky I know, but hey - what are you going to do?
			Thread.Sleep (50); 

			var newCount = _sut.GetNumberOfJobs ();

			Assert.Equal(jobCount + 1, newCount);

		}

        public void Dispose()
        {
            _sut.Dispose();
        }
	}
}

