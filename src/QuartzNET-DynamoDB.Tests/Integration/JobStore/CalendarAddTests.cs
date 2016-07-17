using System;
using Quartz.Impl.Calendar;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
    /// <summary>
    /// Contains tests related to the addition of calendars.
    /// </summary>
    public class CalendarAddTests
    {
        IJobStore _sut;

        public CalendarAddTests()
        {
            _sut = new JobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        [Fact]
        [Trait("Category", "Integration")]
        public void StoreNewCalendar()
        {
            var calendarName = Guid.NewGuid().ToString();
            ICalendar cal = new MonthlyCalendar();
            _sut.StoreCalendar(calendarName, cal, false, true);

            var storedCalendar = _sut.RetrieveCalendar(calendarName);

            Assert.NotNull(storedCalendar);
            Assert.Equal(cal.Description, storedCalendar.Description);
            Assert.Equal(cal.GetType(), storedCalendar.GetType());
        }
    }
}

