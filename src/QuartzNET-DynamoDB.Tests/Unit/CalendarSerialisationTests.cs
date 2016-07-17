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
        /// <summary>
        /// Tests that the description property of the base calendar type serialises and deserialises correctly.
        /// </summary>
        /// <returns>The calendar description.</returns>
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

        /// <summary>
        /// Tests that the excluded days property of the annual calendar serialises and deserialises correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void AnnualDaysExcluded()
        {
            var importantDate = new DateTime(2015, 04, 02);

            AnnualCalendar cal = new AnnualCalendar();
            cal.SetDayExcluded(DateTime.Today, true);
            cal.SetDayExcluded(importantDate, true);

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.True(((AnnualCalendar)deserialised.Calendar).IsDayExcluded(DateTime.Today));
            Assert.True(((AnnualCalendar)deserialised.Calendar).IsDayExcluded(importantDate));
        }

        /// <summary>
        /// Tests that the expression property of the cron calendar serialises and deserialises correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void CronExpression()
        {
            CronCalendar cal = new CronCalendar("0 0 0/1 1/1 * ? *");

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.Equal(cal.CronExpression.ToString(), ((CronCalendar)deserialised.Calendar).CronExpression.ToString());
        }
    }
}
