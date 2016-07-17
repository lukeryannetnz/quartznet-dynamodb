using System;
using Quartz.Impl.Calendar;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
    /// <summary>
    /// Contains tests related to the loading of calendars.
    /// </summary>
    public class CalendarGetTests
    {
        IJobStore _sut;

        public CalendarGetTests()
        {
            _sut = new JobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that after storing a new calendar, that calendar can be retrieved
        /// with the same name, description and type.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void StoreNewCalendar()
        {
            MonthlyCalendar cal = new MonthlyCalendar();
            string calName = Guid.NewGuid().ToString();
            _sut.StoreCalendar(calName, cal, false, true);

            var result = _sut.RetrieveCalendar(calName);

            Assert.NotNull(result);
            Assert.Equal(cal.Description, result.Description);
            Assert.Equal(cal.GetType(), result.GetType());
        }
    }
}

