using System;
using Xunit;
using Quartz.Impl;
using Quartz.Job;
using Quartz.Spi;
using Quartz.Simpl;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Tests the JobStore functions for storing jobs and triggers.
	/// </summary>
	public class JobStoreJobTriggerStoreTests
	{
		IJobStore _sut;

		public JobStoreJobTriggerStoreTests ()
		{
			_sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize(loadHelper, signaler);	
		}

		/// <summary>
		/// Tests that after storing a new job, that job can be retrieved
		/// with the same name, group and type.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void StoreNewJob()
		{
			string jobName = Guid.NewGuid().ToString();
			string jobGroup = Guid.NewGuid().ToString();

			JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));

			_sut.StoreJob(detail, false);

			var result = _sut.RetrieveJob(new JobKey(jobName, jobGroup));

			Assert.NotNull(result);
			Assert.Equal(jobName, result.Key.Name);
			Assert.Equal(jobGroup, result.Key.Group);
			Assert.Equal(typeof(NoOpJob), result.JobType);
		}

		/// <summary>
		/// Tests that after storing an exsting job, that job is updated if the replaceExisting param is passed as true.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void StoreExistingJobOverwrite()
		{
			string jobName = Guid.NewGuid().ToString();
			string jobGroup = Guid.NewGuid().ToString();

			JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
			detail.Description = "Original";

			_sut.StoreJob(detail, false);

			detail.Description = "Updated";
			_sut.StoreJob(detail, true);

			var result = _sut.RetrieveJob(new JobKey(jobName, jobGroup));

			Assert.Equal("Updated", result.Description);
		}

		/// <summary>
		/// Tests that after storing an exsting job, an exception is thrown if that job is updated and the replaceExisting param is passed as False.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void StoreExistingJobDontOverwriteThrows()
		{
			string jobName = Guid.NewGuid().ToString();
			string jobGroup = Guid.NewGuid().ToString();

			JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));

			_sut.StoreJob(detail, false);

			Assert.Throws<ObjectAlreadyExistsException>(() => _sut.StoreJob(detail, false));
		}
	}
}

