using System;
using Quartz.Impl.Calendar;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
    /// <summary>
    /// Tests the DynamoCalendar serialisation for all quartz derived calendar types.
    /// </summary>
    public class CalendarSerialisationTests
    {
        [Fact]
        [Trait("Category", "Unit")]
        public void BaseCalendarDescription()
        {
            BaseCalendar cal = new BaseCalendar() { Description = "Hi mum" };

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.Equal(sut.Description, deserialised.Description);
        }
    }
}

