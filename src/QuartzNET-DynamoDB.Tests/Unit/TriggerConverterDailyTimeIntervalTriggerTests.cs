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
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.DaysOfWeek = new HashSet<DayOfWeek>() { DayOfWeek.Wednesday, DayOfWeek.Saturday };

            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(2, result.DaysOfWeek.Count);
            Assert.Contains(DayOfWeek.Wednesday, trigger.DaysOfWeek);
            Assert.Contains(DayOfWeek.Saturday, trigger.DaysOfWeek);
        }

        [Fact] [Trait("Category", "Unit")]

        public void EndTimeOfDaySerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();

            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.EndTimeOfDay, result.EndTimeOfDay);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatCountSerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatCount = 3;
            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.RepeatCount, result.RepeatCount);
        }


        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalSerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatInterval = 7;
            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.RepeatInterval, result.RepeatInterval);
        }

        [Fact] [Trait("Category", "Unit")]

        public void RepeatIntervalUnitSerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.RepeatIntervalUnit = IntervalUnit.Minute;
            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.RepeatIntervalUnit, result.RepeatIntervalUnit);
        }

        [Fact] [Trait("Category", "Unit")]

        public void StartTimeOfDaySerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.StartTimeOfDay = new TimeOfDay(01, 55, 38);
            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.StartTimeOfDay, result.StartTimeOfDay);
        }

        [Fact] [Trait("Category", "Unit")]

        public void TimesTriggeredSerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();
            trigger.TimesTriggered = 716;
            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.TimesTriggered, result.TimesTriggered);
        }

        [Fact] [Trait("Category", "Unit")]

        public void DailyTimeIntervalTimeZoneSerializesCorrectly()
        {
            TriggerConverter sut = new TriggerConverter();
            var trigger = CreateDailyTimeIntervalTrigger();

            var serialized = sut.ToEntry(trigger);
            IDailyTimeIntervalTrigger result = (IDailyTimeIntervalTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.TimeZone, result.TimeZone);
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
