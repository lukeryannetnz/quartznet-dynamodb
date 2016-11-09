using System;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Tests the DynamoTrigger serialisation for all attributes of the ICronTrigger trigger type.
    /// </summary>
    public class TriggerSerialisationCronTriggerTests
    {
        [Fact]
        [Trait("Category", "Unit")]
        public void CronExpressionStringSerializesCorrectly()
        {
            var trigger = CreateCronTrigger();

            var serialized = new DynamoTrigger(trigger).ToDynamo();
            var result = (CronTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.CronExpressionString, result.CronExpressionString);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void TimeZoneSerializesCorrectly()
        {
            var trigger = CreateCronTrigger();

            var serialized = new DynamoTrigger(trigger).ToDynamo();
            var result = (CronTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.TimeZone, result.TimeZone);
        }

        private static CronTriggerImpl CreateCronTrigger()
        {          
            var jobKey = new JobKey("test");
            var trigger = (CronTriggerImpl)TriggerBuilder.Create()
                .ForJob(jobKey)
                .WithCronSchedule("0 0 5 ? * *", x => x.InTimeZone(TimeZoneInfo.Local))
                .Build();
            return trigger;
        }
    }
}
