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


	}
}

