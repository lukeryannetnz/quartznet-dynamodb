using System;
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
		/// Tests that when no jobs exist, zero is returned when GetNumberOfJobs is called.
		/// </summary>
		/// <returns>The number of jobs returns zero.</returns>
		[Fact]
		[Trait ("Category", "Integration")]
		public void GetNumberOfJobsReturnsZero ()
		{
			var jobCount = _sut.GetNumberOfJobs();

			Assert.Equal (0, jobCount);
		}
	}
}

