using System;
using Quartz.Collection;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Tests the Dynamo TriggerConverter for all attributes of the IDailyTimeIntervalTrigger trigger type.
    /// </summary>
    public class TriggerConverterDailyTimeIntervalTriggerTests
    {
        [Fact] [Trait("Category", "Unit")]

        public void DaysOfWeekSerializesCorrectly()
        {
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.DaysOfWeek = new HashSet<DayOfWeek>() { DayOfWeek.Wednesday, DayOfWeek.Saturday };

            var serialized = new DynamoTrigger(trigger).ToDynamo();
			IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(2, result.DaysOfWeek.Count);
            Assert.Contains(DayOfWeek.Wednesday, trigger.DaysOfWeek);
            Assert.Contains(DayOfWeek.Saturday, trigger.DaysOfWeek);
        }

        [Fact] [Trait("Category", "Unit")]

        public void EndTimeOfDaySerializesCorrectly()
        {
            var trigger = CreateDailyTimeIntervalTrigger();

            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.EndTimeOfDay, result.EndTimeOfDay);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatCountSerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatCount = 3;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.RepeatCount, result.RepeatCount);
        }


        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalSerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatInterval = 7;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.RepeatInterval, result.RepeatInterval);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalUnitSerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatIntervalUnit = IntervalUnit.Minute;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.RepeatIntervalUnit, result.RepeatIntervalUnit);
        }

        [Fact] [Trait("Category", "Unit")]

        public void StartTimeOfDaySerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.StartTimeOfDay = new TimeOfDay(01, 55, 38);
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.StartTimeOfDay, result.StartTimeOfDay);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimesTriggeredSerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.TimesTriggered = 716;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.TimesTriggered, result.TimesTriggered);
        }

        [Fact] [Trait("Category", "Unit")]

        public void DailyTimeIntervalTimeZoneSerializesCorrectly()
        {
            
            var trigger = CreateDailyTimeIntervalTrigger();

            var serialized = new DynamoTrigger(trigger).ToDynamo();
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)new DynamoTrigger(serialized).Trigger;

			Assert.Equal(trigger.TimeZone.DisplayName, result.TimeZone.DisplayName);
        }

        private static DailyTimeIntervalTriggerImpl CreateDailyTimeIntervalTrigger()
        {
            var jobKey = new JobKey("test");
            DailyTimeIntervalTriggerImpl trigger = (DailyTimeIntervalTriggerImpl)TriggerBuilder.Create()
                .ForJob(jobKey)
                .WithSimpleSchedule()
				.WithDailyTimeIntervalSchedule(x => x.OnEveryDay()
                    .StartingDailyAt(new TimeOfDay(10, 10))
                    .EndingDailyAt(new TimeOfDay(10, 20)))

                .Build();
            return trigger;
        }
    }
}
