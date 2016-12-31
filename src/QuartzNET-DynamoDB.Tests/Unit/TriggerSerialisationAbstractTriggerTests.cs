using System;
using System.Collections.Generic;
using System.Linq;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Tests the DynamoTrigger serialisation for all attributes of the AbstractTrigger trigger type.
    /// </summary>
    public class TriggerSerialisationAbstractTriggerTests
    {
        [Fact] 
		[Trait("Category", "Unit")]
        public void KeySerializesCorrectly()
        {
            var trigger = new TestTrigger();

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.Key.Name, result.Key.Name);
            Assert.Equal(trigger.Key.Group, result.Key.Group);
        }

        [Fact] [Trait("Category", "Unit")]

        public void JobKeySerializesCorrectly()
        {
			var trigger = new TestTrigger();

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.JobKey.Name, result.JobKey.Name);
            Assert.Equal(trigger.JobKey.Group, result.JobKey.Group);
        }

        [Fact] [Trait("Category", "Unit")]

        public void CalendarNameSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.CalendarName = "Test calendar name";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.CalendarName, result.CalendarName);
        }

        [Fact] [Trait("Category", "Unit")]

        public void DescriptionSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.Description = "Test description";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.Description, result.Description);
        }

        [Fact] [Trait("Category", "Unit")]

        public void FireInstanceIdSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.FireInstanceId = "Test id";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.FireInstanceId, result.FireInstanceId);
        }

        [Fact] [Trait("Category", "Unit")]

        public void GroupSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.Group = "Test group id";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.Group, result.Group);
        }

        [Fact] [Trait("Category", "Unit")]

        public void JobDataMapSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			IDictionary<string, object> jobData = new Dictionary<string, object>();
			jobData.Add("key", "value");
			trigger.JobDataMap = new JobDataMap(jobData);

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(1, result.JobDataMap.Count);
            Assert.Equal(trigger.JobDataMap.Keys.First(), result.JobDataMap.Keys.First());
            Assert.Equal(trigger.JobDataMap.Values.First(), result.JobDataMap.Values.First());
        }

        [Fact] [Trait("Category", "Unit")]

        public void JobGroupSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.JobGroup = "Test job group name";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.JobGroup, result.JobGroup);
        }

        [Fact] [Trait("Category", "Unit")]

        public void JobNameSerializesCorrectly()
        {
			var trigger = new TestTrigger();
			trigger.JobName = "Test job  name";

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.JobName, result.JobName);
        }

        [Fact] [Trait("Category", "Unit")]

        public void MisfireInstructionSerializesCorrectly()
        {
            
            var trigger = new TestTrigger();
            trigger.MisfireInstruction = 3;
			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.MisfireInstruction, result.MisfireInstruction);
        }

        [Fact] [Trait("Category", "Unit")]

        public void NameSerializesCorrectly()
        {
            
            var trigger = new TestTrigger();
            trigger.Name = "Test name";
			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.Name, result.Name);
        }

        [Fact] [Trait("Category", "Unit")]

        public void PrioritySerializesCorrectly()
        {
            
            var trigger = new TestTrigger();
            trigger.Priority = 78;
			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.Priority, result.Priority);
        }

        [Fact] [Trait("Category", "Unit")]

        public void EndTimeUtcSerializesCorrectly()
        {
            
            var trigger = new TestTrigger();
            trigger.EndTimeUtc = new DateTimeOffset(2015, 12, 25, 07, 30, 53, TimeSpan.Zero);

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.EndTimeUtc, result.EndTimeUtc);
        }

        [Fact] 
		[Trait("Category", "Unit")]
        public void StartTimeUtcSerializesCorrectly()
        {
            
            var trigger = new TestTrigger();
            trigger.StartTimeUtc = new DateTimeOffset(2015, 12, 25, 07, 30, 53, TimeSpan.Zero);

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

            Assert.Equal(trigger.StartTimeUtc, result.StartTimeUtc);
        }

		[Fact] 
		[Trait("Category", "Unit")]
		public void NextFireTimeUtcSerializesCorrectly()
		{

			var trigger = new TestTrigger();
			var nextFireTime = new DateTimeOffset (1980, 12, 25, 07, 30, 53, TimeSpan.Zero);
			trigger.SetNextFireTimeUtc (nextFireTime);

			var serialized = new DynamoTrigger(trigger).ToDynamo();
			AbstractTrigger result = new DynamoTrigger(serialized).Trigger;

			Assert.Equal(nextFireTime, result.GetNextFireTimeUtc());
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateSerialised()
		{
			var sut = new DynamoTrigger (new TestTrigger());
            sut.State = new DynamoTriggerState(-78);

			var serialised = sut.ToDynamo ();

			var deserialised = new DynamoTrigger ();
			deserialised.InitialiseFromDynamoRecord (serialised);

            Assert.Equal (sut.State.InternalValue, deserialised.State.InternalValue);
		}

        [Serializable]
        private sealed class TestTrigger : AbstractTrigger
        {
			private DateTimeOffset? nextFireTimeUtc;

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
				return nextFireTimeUtc;
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
				nextFireTimeUtc = nextFireTime;
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