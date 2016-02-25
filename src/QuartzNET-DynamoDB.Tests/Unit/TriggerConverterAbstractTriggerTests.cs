using System;
using System.Collections.Generic;
using System.Linq;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// /// Tests the Dynamo TriggerConverter for all attributes of the AbstractTrigger trigger type.
    /// </summary>
    public class TriggerConverterAbstractTriggerTests
    {
        [Fact]
        public void KeySerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.Key.Name, result.Key.Name);
            Assert.Equal(trigger.Key.Group, result.Key.Group);
        }

        [Fact]
        public void JobKeySerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.JobKey.Name, result.JobKey.Name);
            Assert.Equal(trigger.JobKey.Group, result.JobKey.Group);
        }

        [Fact]
        public void CalendarNameSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.CalendarName = "Test calendar name";
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.CalendarName, result.CalendarName);
        }

        [Fact]
        public void DescriptionSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.Description = "Test description";

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.Description, result.Description);
        }

        [Fact]
        public void FireInstanceIdSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.FireInstanceId = "Test id";

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.FireInstanceId, result.FireInstanceId);
        }

        [Fact]
        public void GroupSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.Group = "Test group id";

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.Group, result.Group);
        }

        [Fact]
        public void JobDataMapSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            IDictionary<string, object> jobData = new Dictionary<string, object>();
            jobData.Add("key", "value");
            trigger.JobDataMap = new JobDataMap(jobData);

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(1, result.JobDataMap.Count);
            Assert.Equal(trigger.JobDataMap.Keys.First(), result.JobDataMap.Keys.First());
            Assert.Equal(trigger.JobDataMap.Values.First(), result.JobDataMap.Values.First());
        }

        [Fact]
        public void JobGroupSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.JobGroup = "Test job group name";
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.JobGroup, result.JobGroup);
        }

        [Fact]
        public void JobNameSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.JobName = "Test job  name";
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.JobName, result.JobName);
        }

        [Fact]
        public void MisfireInstructionSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.MisfireInstruction = 3;
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.MisfireInstruction, result.MisfireInstruction);
        }

        [Fact]
        public void NameSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.Name = "Test name";
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.Name, result.Name);
        }

        [Fact]
        public void PrioritySerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.Priority = 78;
            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.Priority, result.Priority);
        }

        [Fact]
        public void EndTimeUtcSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.EndTimeUtc = new DateTimeOffset(2015, 12, 25, 07, 30, 53, TimeSpan.Zero);

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.EndTimeUtc, result.EndTimeUtc);
        }

        [Fact]
        public void StartTimeUtcSerializesCorrectly()
        {
            var sut = new TriggerConverter();
            var trigger = new TestTrigger();
            trigger.StartTimeUtc = new DateTimeOffset(2015, 12, 25, 07, 30, 53, TimeSpan.Zero);

            var serialized = sut.ToEntry(trigger);
            AbstractTrigger result = (AbstractTrigger)sut.FromEntry(serialized);

            Assert.Equal(trigger.StartTimeUtc, result.StartTimeUtc);
        }

        [Serializable]
        private sealed class TestTrigger : AbstractTrigger
        {
            public TestTrigger()
            {
                Key = new TriggerKey("tname", "tgroup");
                JobKey = new JobKey("jname", "jgroup");
            }

            public override IScheduleBuilder GetScheduleBuilder()
            {
                throw new NotImplementedException();
            }

            public override DateTimeOffset? FinalFireTimeUtc
            {
                get { throw new NotImplementedException(); }
            }

            public override void Triggered(ICalendar cal)
            {
                throw new NotImplementedException();
            }

            public override DateTimeOffset? ComputeFirstFireTimeUtc(ICalendar cal)
            {
                throw new NotImplementedException();
            }

            public override bool GetMayFireAgain()
            {
                throw new NotImplementedException();
            }

            public override DateTimeOffset? GetNextFireTimeUtc()
            {
                throw new NotImplementedException();
            }

            public override DateTimeOffset? GetFireTimeAfter(DateTimeOffset? afterTime)
            {
                throw new NotImplementedException();
            }

            protected override bool ValidateMisfireInstruction(int misfireInstruction)
            {
                return true;
            }

            public override void UpdateAfterMisfire(ICalendar cal)
            {
                throw new NotImplementedException();
            }

            public override void UpdateWithNewCalendar(ICalendar cal, TimeSpan misfireThreshold)
            {
                throw new NotImplementedException();
            }

            public override void SetNextFireTimeUtc(DateTimeOffset? nextFireTime)
            {
                throw new NotImplementedException();
            }

            public override void SetPreviousFireTimeUtc(DateTimeOffset? previousFireTime)
            {
                throw new NotImplementedException();
            }

            public override DateTimeOffset? GetPreviousFireTimeUtc()
            {
                throw new NotImplementedException();
            }

            public override bool HasMillisecondPrecision => false;
        }
    }
}