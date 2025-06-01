using System;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    public class DynamoConfigurationTests
    {
        public DynamoConfigurationTests()
        {
            // Initialize configuration before each test
            DynamoConfiguration.Initialize(TestConfiguration.GetConfiguration());
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void NoConfigurationDefaultDelayReturned()
        {
            Assert.Equal(500, DynamoConfiguration.BootstrapRetryDelayMilliseconds);
        }
    }
}
