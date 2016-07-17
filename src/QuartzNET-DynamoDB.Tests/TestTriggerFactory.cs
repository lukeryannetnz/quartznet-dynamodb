using System;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Quartz.DynamoDB.Tests
{
	public class TestTriggerFactory
	{
		public static IOperableTrigger CreateTestTrigger (string jobName)
		{
			string triggerGroup = Guid.NewGuid ().ToString ();

			IOperableTrigger tr = new SimpleTriggerImpl ("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours (1));

			return tr;
		}
	}
}

