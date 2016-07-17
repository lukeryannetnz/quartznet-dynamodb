using System;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Quartz.DynamoDB.Tests.Integration
{
    public class TestTriggerFactory
    {
        public static IOperableTrigger CreateTestTrigger(string jobName, string jobGroup = "JobGroup")
        {
            string triggerGroup = Guid.NewGuid().ToString();

            IOperableTrigger tr = new SimpleTriggerImpl("test", triggerGroup, jobName, jobGroup, DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));

            tr.JobKey = new JobKey(jobName, jobGroup);
            return tr;
        }
    }
}

