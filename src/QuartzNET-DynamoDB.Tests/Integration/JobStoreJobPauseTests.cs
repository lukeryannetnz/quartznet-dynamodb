using System;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;
using System.Linq;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests related to the Pausing of Jobs and Job Groups.
	/// </summary>
	public class JobStoreJobPauseTests
	{
		IJobStore _sut;

		public JobStoreJobPauseTests ()
		{
			_sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize(loadHelper, signaler);
		}
	
		/// <summary>
		/// Tests that when Pause Jobs is called with a group matcher equalling a job group, one job group is paused.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseJobsOneGroupEquals()
		{
			string jobGroup = Guid.NewGuid().ToString();
			var result = _sut.PauseJobs(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupEquals(jobGroup));

			Assert.Equal(1, result.Count);
			Assert.Equal(jobGroup, result.Single());
		}
	}
}

