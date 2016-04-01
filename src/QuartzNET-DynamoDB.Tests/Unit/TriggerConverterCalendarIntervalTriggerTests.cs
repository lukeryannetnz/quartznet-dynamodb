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
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();

            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

            Assert.Equal(trigger.RepeatIntervalUnit, result.RepeatIntervalUnit);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();

            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

            Assert.Equal(trigger.RepeatInterval, result.RepeatInterval);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimesTriggeredSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();
            trigger.TimesTriggered = 13;
            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

            Assert.Equal(trigger.TimesTriggered, result.TimesTriggered);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimeZoneSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();
            trigger.TimeZone = TimeZoneInfo.Utc;
            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

			Assert.Equal(trigger.TimeZone.DisplayName, result.TimeZone.DisplayName);
        }

        [Fact] [Trait("Category", "Unit")]

        public void MisfireInstructionSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();
            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

            Assert.Equal(trigger.MisfireInstruction, result.MisfireInstruction);
        }

        [Fact] [Trait("Category", "Unit")]

        public void PreserveHourOfDayAcrossDaylightSavingsSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();
            trigger.PreserveHourOfDayAcrossDaylightSavings = true;
            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

            Assert.Equal(trigger.PreserveHourOfDayAcrossDaylightSavings, result.PreserveHourOfDayAcrossDaylightSavings);
        }

        [Fact] [Trait("Category", "Unit")]

        public void SkipDayIfHourDoesNotExistSerialisesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = CreateTrigger();
            trigger.SkipDayIfHourDoesNotExist = true;
            var serialized = sut.ToEntry(trigger);
            CalendarIntervalTriggerImpl result = (CalendarIntervalTriggerImpl)sut.FromEntry(serialized);

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