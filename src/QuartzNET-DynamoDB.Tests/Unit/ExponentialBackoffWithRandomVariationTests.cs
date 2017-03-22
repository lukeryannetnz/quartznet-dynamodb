using System;
using Quartz.DynamoDB.DataModel.Storage;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    public class ExponentialBackoffWithRandomVariationTests
    {
        /// <summary>
        /// First attempt sleep should be between 1 and 2.
        /// </summary>
        [Fact]
        public void CalculateWaitDurationFirstAttempt()
        {
            TimeSpan result = ExponentialBackoffWithRandomVariation.CalculateWaitDuration(1);
            Assert.True(result >= TimeSpan.FromSeconds(1));
            Assert.True(result <= TimeSpan.FromSeconds(2));
        }

        /// <summary>
        /// Second attempt sleep should be between 8 and 16.
        /// </summary>
        [Fact]
        public void CalculateWaitDurationSecondAttempt()
        {
            TimeSpan result = ExponentialBackoffWithRandomVariation.CalculateWaitDuration(2);
            Assert.True(result >= TimeSpan.FromSeconds(8));
            Assert.True(result <= TimeSpan.FromSeconds(16));
        }

        /// <summary>
        /// Third attempt sleep should be between 27 and 54.
        /// </summary>
        [Fact]
        public void CalculateWaitDurationThirdAttempt()
        {
            TimeSpan result = ExponentialBackoffWithRandomVariation.CalculateWaitDuration(3);
            Assert.True(result >= TimeSpan.FromSeconds(27));
            Assert.True(result <= TimeSpan.FromSeconds(54));
        }

        /// <summary>
        /// Fourth attempt sleep should be between 64 and 128.
        /// </summary>
        [Fact]
        public void CalculateWaitDurationFourthAttempt()
        {
            TimeSpan result = ExponentialBackoffWithRandomVariation.CalculateWaitDuration(4);
            Assert.True(result >= TimeSpan.FromSeconds(64));
            Assert.True(result <= TimeSpan.FromSeconds(128));
        }

        /// <summary>
        /// Fifth attempt sleep should be between 125 and 250.
        /// </summary>
        [Fact]
        public void CalculateWaitDurationFifthAttempt()
        {
            TimeSpan result = ExponentialBackoffWithRandomVariation.CalculateWaitDuration(5);
            Assert.True(result >= TimeSpan.FromSeconds(125));
            Assert.True(result <= TimeSpan.FromSeconds(250));
        }
    }
}
