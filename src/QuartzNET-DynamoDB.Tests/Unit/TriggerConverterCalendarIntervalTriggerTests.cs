using System;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Tests the Dynamo TriggerConverter for all attributes of the CalendarInteralTriggerImpl trigger type.
    /// </summary>
    public class TriggerConverterCalendarIntervalTriggerTests
    {
        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalUnitSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.RepeatIntervalUnit, result.RepeatIntervalUnit);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();

            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.RepeatInterval, result.RepeatInterval);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimesTriggeredSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();
            trigger.TimesTriggered = 13;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.TimesTriggered, result.TimesTriggered);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimeZoneSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();
            trigger.TimeZone = TimeZoneInfo.Utc;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

			Assert.Equal(trigger.TimeZone.DisplayName, result.TimeZone.DisplayName);
        }

        [Fact] [Trait("Category", "Unit")]

        public void MisfireInstructionSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.MisfireInstruction, result.MisfireInstruction);
        }

        [Fact] [Trait("Category", "Unit")]

        public void PreserveHourOfDayAcrossDaylightSavingsSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();
            trigger.PreserveHourOfDayAcrossDaylightSavings = true;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.PreserveHourOfDayAcrossDaylightSavings, result.PreserveHourOfDayAcrossDaylightSavings);
        }

        [Fact] [Trait("Category", "Unit")]

        public void SkipDayIfHourDoesNotExistSerialisesCorrectly()
        {
            
            var trigger = CreateTrigger();
            trigger.SkipDayIfHourDoesNotExist = true;
            var serialized = new DynamoTrigger(trigger).ToDynamo();
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.PreserveHourOfDayAcrossDaylightSavings, result.SkipDayIfHourDoesNotExist);
        }

        public CalendarIntervalTriggerImpl CreateTrigger()
        {
            CalendarIntervalTriggerImpl trigger = (CalendarIntervalTriggerImpl) TriggerBuilder.Create()
                .WithIdentity("myTrigger", "myTriggerGroup")
                .ForJob("job")
                .WithCalendarIntervalSchedule(x => x
                    .WithIntervalInHours(1))
                .StartAt(DateBuilder.FutureDate(10, IntervalUnit.Minute))
                .Build();

            return trigger;
        }
    }
}