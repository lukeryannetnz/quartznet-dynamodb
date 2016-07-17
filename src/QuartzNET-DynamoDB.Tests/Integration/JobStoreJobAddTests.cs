using System;
using System.Collections.Generic;
using Quartz.Impl;
using Quartz.Job;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests related to the Addition of Jobs and Job Groups.
	/// </summary>
	public class JobStoreJobAddTests
	{
		IJobStore _sut;

		public JobStoreJobAddTests ()
		{
			_sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize (loadHelper, signaler);
		}

		/// <summary>
		/// Tests that when CheckExit is called for a job that exists, it returns true.
		/// </summary>
		[Fact]
		[Trait ("Category", "Integration")]
		public void CheckExistsWhenJobExsits ()
		{
			CreateTestJob ();

		}

		JobDetailImpl CreateTestJob ()
		{
			string jobGroup = Guid.NewGuid ().ToString ();
			// Create a random job, store it.
			string jobName = Guid.NewGuid ().ToString ();
			JobDetailImpl detail = new JobDetailImpl (jobName, jobGroup, typeof (NoOpJob));
			_sut.StoreJob (detail, false);

			return detail;
		}
	}
}

