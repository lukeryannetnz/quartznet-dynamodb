using System;
using System.Collections.Generic;
using Quartz.Impl;
using Quartz.Job;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests.Integration.JobStore
{
	/// <summary>
	/// Contains tests related to the Removal of Jobs and Job Groups.
	/// </summary>
    public class JobRemoveTests : IDisposable
	{
        private readonly DynamoDB.JobStore _sut;

		public JobRemoveTests ()
		{
			_sut = new Quartz.DynamoDB.JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		/// <summary>
		/// Tests that when Remove Jobs is called with two jobs it removes all jobs
		/// </summary>
		[Fact]
		[Trait ("Category", "Integration")]
		public void RemoveJobsHappyPath()
		{
			IList<JobKey> jobKeys = new List<JobKey>();
			for (int i = 0; i < 2; i++) 
			{
				string jobGroup = Guid.NewGuid ().ToString ();
				// Create a random job, store it.
				string jobName = Guid.NewGuid ().ToString ();
				JobDetailImpl detail = new JobDetailImpl (jobName, jobGroup, typeof (NoOpJob));
				_sut.StoreJob (detail, false);

				jobKeys.Add (detail.Key);
			}

			var result = _sut.RemoveJobs (jobKeys);

			Assert.True (result);
		}

		/// <summary>
		/// Tests that when Remove Jobs is called with three keys and one that doesn't exist, it removes the ones that exist.
		/// </summary>
		[Fact]
		[Trait ("Category", "Integration")]
		public void RemoveJobsOneJobDoesntExist ()
		{
			IList<JobKey> jobKeys = new List<JobKey> ();

			jobKeys.Add(new JobKey ("ThisDoesn'tExist"));

			for (int i = 0; i < 2; i++) {
				string jobGroup = Guid.NewGuid ().ToString ();
				// Create a random job, store it.
				string jobName = Guid.NewGuid ().ToString ();
				JobDetailImpl detail = new JobDetailImpl (jobName, jobGroup, typeof (NoOpJob));
				_sut.StoreJob (detail, false);

				jobKeys.Add (detail.Key);
			}

			var result = _sut.RemoveJobs (jobKeys);

			// Should return false as not all jobs were removed.
            Assert.False(result);
		}

        public void Dispose()
        {
            _sut.Dispose();
        }
	}
}

