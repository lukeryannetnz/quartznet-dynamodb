using System;
using Quartz.Impl;
using Quartz.Job;

namespace Quartz.DynamoDB.Tests.Integration
{
	public class TestJobFactory
	{
		public static JobDetailImpl CreateTestJob ()
		{
			string jobGroup = Guid.NewGuid ().ToString ();
			// Create a random job, store it.
			string jobName = Guid.NewGuid ().ToString ();
			JobDetailImpl detail = new JobDetailImpl (jobName, jobGroup, typeof (NoOpJob));

			return detail;
		}
	}
}

