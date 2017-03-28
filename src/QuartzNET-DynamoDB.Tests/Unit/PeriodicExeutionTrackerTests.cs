using System;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Contains tests for the periodic execution tracker.
    /// </summary>
    public class PeriodicExeutionTrackerTests
    {
        /// <summary>
        /// The first call should return true, regarless of the whether the timespan has elapsed or not.
        /// </summary>
        [Fact]
        public void FirstCallReturnsTrue()
        {
            var sut = new PeriodicExecutionTracker(TimeSpan.FromHours(10));

            var result = sut.ShouldExecute();

            Assert.True(result);
        }

        /// <summary>
        /// Before the timespan elapses, don't execute the operation.
        /// </summary>
        [Fact]
        public void BeforeTimespanElapsedShouldNotExecute()
        {
            var utcNow = DateTime.UtcNow;
            var frequency = TimeSpan.FromHours(24);
            var twentyThreeHoursInFuture = utcNow.Add(frequency).Subtract(TimeSpan.FromHours(1));

            var sut = new PeriodicExecutionTracker(frequency);
            var firstCallIsAlwaysTrue = sut.ShouldExecute();

            var result = sut.ShouldExecute(twentyThreeHoursInFuture);
            Assert.False(result);
        }

        /// <summary>
        /// At the exact time the frequency elapses, do execute the method.
        /// </summary>
        [Fact]
        public void ExactlyWhenTimespanElapsedShouldExecute()
        {
            var utcNow = DateTime.UtcNow;
            var frequency = TimeSpan.FromHours(24);

            var sut = new PeriodicExecutionTracker(frequency);
            var firstCallIsAlwaysTrue = sut.ShouldExecute();

            var result = sut.ShouldExecute(utcNow.Add(frequency));
            Assert.True(result);
        }

        /// <summary>
        /// Any time after the frequency elapses, do execute the method.
        /// </summary>
        [Fact]
        public void AfterTimespanElapsedShouldExecute()
        {
            var utcNow = DateTime.UtcNow;
            var frequency = TimeSpan.FromHours(24);
            var twentyFiveHoursInFuture = utcNow.Add(frequency).Add(TimeSpan.FromHours(1));

            var sut = new PeriodicExecutionTracker(frequency);
            var firstCallIsAlwaysTrue = sut.ShouldExecute();

            var result = sut.ShouldExecute(twentyFiveHoursInFuture);
            Assert.True(result);
        }

        /// <summary>
        /// Tests that subsequent executions return true after the elapsed period.
        /// Tests that the tracker is tracking state correctly.
        /// </summary>
        [Fact]
        public void SubsequentExecutionsOnFrequency()
        {
            var utcNow = DateTime.UtcNow;
            var frequency = TimeSpan.FromHours(24);

            var sut = new PeriodicExecutionTracker(frequency);
            var firstCallIsAlwaysTrue = sut.ShouldExecute();

            var lastExecution = utcNow;

            for (int i = 0; i <= 9; i++)
            {
                lastExecution = lastExecution.Add(frequency);

                var result = sut.ShouldExecute(lastExecution);
                Assert.True(result);
            }
        }

        /// <summary>
        /// Tests that subsequent executions return false just before the elapsed period.
        /// Tests that the tracker is tracking state correctly.
        /// </summary>
        [Fact]
        public void SubsequentExecutionsBeforeFrequency()
        {
            var utcNow = DateTime.UtcNow;
            var frequency = TimeSpan.FromHours(24);

            var sut = new PeriodicExecutionTracker(frequency);
            var firstCallIsAlwaysTrue = sut.ShouldExecute();

            var lastExecution = utcNow;

            for (int i = 0; i <= 9; i++)
            {
                lastExecution = lastExecution.Add(frequency);

                var justBefore = sut.ShouldExecute(lastExecution.AddMilliseconds(-1));
                Assert.False(justBefore);

                sut.ShouldExecute(lastExecution);
            }
        }
    }
}
