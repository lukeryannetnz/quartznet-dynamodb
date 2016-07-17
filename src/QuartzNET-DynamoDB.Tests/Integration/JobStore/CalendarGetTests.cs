using System;
using System.Threading;
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
        /// Tests that after a calendar is added, the number of calendars increments.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void GetNumberOfJobsIncrementsWhenJobAdded()
        {
            var jobCount = _sut.GetNumberOfCalendars();

            MonthlyCalendar cal = new MonthlyCalendar();
            string calName = Guid.NewGuid().ToString();
            _sut.StoreCalendar(calName, cal, false, true);

            // Dynamo describe table is eventually consistent so give it a little time. Flaky I know, but hey - what are you going to do?
            Thread.Sleep(50);

            var newCount = _sut.GetNumberOfCalendars();

            Assert.Equal(jobCount + 1, newCount);

        }
    }
}

