using System;
using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
    /// <summary>
    /// Contains tests for the DynamoTriggerState class.
    /// </summary>
    public class DynamoTriggerStateTests
    {
        /// <summary>
        /// Asserts that when the internal value is the same, object comparison should return true.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonSameIfInternalValueSame()
        {
            var sut = new DynamoTriggerState(7);
            var sut2 = new DynamoTriggerState(7);

            Assert.True(sut == sut2);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonFalseIfInternalValueSame()
        {
            var sut = new DynamoTriggerState(7);
            var sut2 = new DynamoTriggerState(7);

            Assert.False(sut != sut2);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonFalseIfLeftNull()
        {
            var sut = new DynamoTriggerState(7);

            Assert.False(null == sut);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonTrueIfLeftNull()
        {
            var sut = new DynamoTriggerState(7);

            Assert.True(null != sut);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonFalseIfRightNull()
        {
            var sut = new DynamoTriggerState(7);

            Assert.False(sut == null);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonTrueIfRightNull()
        {
            var sut = new DynamoTriggerState(7);

            Assert.True(sut != null);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonFalseIfInternalValueDifferent()
        {
            var sut = new DynamoTriggerState(7);
            var sut2 = new DynamoTriggerState(13);

            Assert.False(sut == sut2);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void ComparisonTrueIfInternalValueDifferent()
        {
            var sut = new DynamoTriggerState(7);
            var sut2 = new DynamoTriggerState(13);

            Assert.True(sut != sut2);
        }
    }
}
