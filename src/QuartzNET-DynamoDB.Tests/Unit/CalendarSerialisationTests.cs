using System;
using Quartz.Impl.Calendar;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
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

        /// <summary>
        /// Tests that the time range serialises correctly for the daily calendar.
        /// TODO: I think there may be a timezone bug here that requires the author and recipient servers to share timezone.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void DailyTimeRange()
        {
            DailyCalendar cal = new DailyCalendar(new DateTime(2015, 04, 02, 14, 00, 00), new DateTime(2015, 04, 02, 23, 30, 00));

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            DateTime now = DateTime.Now;

            Assert.Equal(cal.GetTimeRangeStartingTimeUtc(now), ((DailyCalendar)deserialised.Calendar).GetTimeRangeStartingTimeUtc(now));
            Assert.Equal(cal.GetTimeRangeEndingTimeUtc(now), ((DailyCalendar)deserialised.Calendar).GetTimeRangeEndingTimeUtc(now));
        }

        /// <summary>
        /// Tests that the invert time range property serialises correclty for the daily calendar type.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void DailyInvertTimeRange()
        {
            DailyCalendar cal = new DailyCalendar(new DateTime(2015, 04, 02, 14, 00, 00), new DateTime(2015, 04, 02, 23, 30, 00));
            cal.InvertTimeRange = true;

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            DateTime now = DateTime.Now;

            Assert.Equal(cal.InvertTimeRange, ((DailyCalendar)deserialised.Calendar).InvertTimeRange);
        }

        /// <summary>
        /// Tests that the excluded days collection on the holiday calendar type is serialised correctly.
        /// </summary>
        /// <returns>The calendar excluded days.</returns>
        [Fact]
        [Trait("Category", "Unit")]
        public void HolidayCalendarExcludedDays()
        {
            var importantDate = new DateTime(2015, 04, 02, 5, 5, 5, DateTimeKind.Utc);
            var cal = new HolidayCalendar();
            cal.AddExcludedDate(importantDate.Date);
            cal.AddExcludedDate(DateTime.UtcNow.Date);

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.True(((HolidayCalendar)deserialised.Calendar).ExcludedDates.Contains(importantDate.Date));
            Assert.True(((HolidayCalendar)deserialised.Calendar).ExcludedDates.Contains(DateTime.UtcNow.Date));
        }

        /// <summary>
        /// Tests that the excluded days property of the monthly calendar serialises and deserialises correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void MonthlyDaysExcluded()
        {
            MonthlyCalendar cal = new MonthlyCalendar();
            cal.SetDayExcluded(1, true);
            cal.SetDayExcluded(31, true);

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.True(((MonthlyCalendar)deserialised.Calendar).IsDayExcluded(1));
            Assert.True(((MonthlyCalendar)deserialised.Calendar).IsDayExcluded(31));
        }

        /// <summary>
        /// Tests that the excluded days property of the monthly calendar serialises and deserialises correctly when no days are excluded.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void MonthlyNoDaysExcluded()
        {
            MonthlyCalendar cal = new MonthlyCalendar();

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            for (int i = 1; i <= 31; i++)
            {
                Assert.False(((MonthlyCalendar)deserialised.Calendar).IsDayExcluded(i));
            }
        }

        /// <summary>
        /// Tests that the excluded days property of the weekly calendar serialises and deserialises correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void WeeklyDaysExcluded()
        {
            WeeklyCalendar cal = new WeeklyCalendar();
            cal.SetDayExcluded(DayOfWeek.Monday, true);
            cal.SetDayExcluded(DayOfWeek.Sunday, true);

            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.True(((WeeklyCalendar)deserialised.Calendar).IsDayExcluded(DayOfWeek.Monday));
            Assert.True(((WeeklyCalendar)deserialised.Calendar).IsDayExcluded(DayOfWeek.Sunday));
        }

        /// <summary>
        /// Tests that the excluded days property of the weekly calendar serialises and deserialises correctly when no days are excluded.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void WeeklyDaysNoExcluded()
        {
            WeeklyCalendar cal = new WeeklyCalendar();
           
            var sut = new DynamoCalendar("test", cal);
            var serialised = sut.ToDynamo();
            var deserialised = new DynamoCalendar(serialised);

            Assert.False(((WeeklyCalendar)deserialised.Calendar).IsDayExcluded(DayOfWeek.Monday));
        }
    }
}
