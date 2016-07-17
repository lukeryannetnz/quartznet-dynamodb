using System;
using System.Threading;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests related to retrieving Jobs and Job Groups.
	/// </summary>
	public class JobStoreGetJobTests
	{
		IJobStore _sut;

		public JobStoreGetJobTests ()
		{
			_sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		/// <summary>
		/// Tests that after a job is added, the number of jobs increments.
		/// </summary>
		/// <returns>The number of jobs returns zero.</returns>
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
	}
}

