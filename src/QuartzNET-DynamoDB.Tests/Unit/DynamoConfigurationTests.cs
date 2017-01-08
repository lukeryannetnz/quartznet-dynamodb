using System;
using System.Configuration;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    public class DynamoConfigurationTests
    {
        [Fact]
        [Trait("Category", "Unit")]
        public void NoConfigurationDefaultDelayReturned()
        {
            var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            config.AppSettings.Settings.Remove("BootstrapRetryDelayMilliseconds");
            config.Save(ConfigurationSaveMode.Full);

            Assert.Equal(500, DynamoConfiguration.BootstrapRetryDelayMilliseconds);
        }
    }
}
