using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Contains tests for the dynamo job class.
    /// </summary>
    public class DynamoJobTests
    {
        public DynamoJobTests()
        {
        }

        /// <summary>
        /// Tests that a new DynamoJob object has its state set correctly.
        /// </summary>
        [Fact]
        [Trait("Category", "Unit")]
        public void InitialisedState()
        {
            var sut = new DynamoJob();

            Assert.Equal(DynamoJobState.Active, sut.State);
        }
    }
}
