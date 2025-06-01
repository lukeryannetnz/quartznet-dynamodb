using Microsoft.Extensions.Configuration;
using System.IO;

namespace Quartz.DynamoDB.Tests.Unit
{
    public static class TestConfiguration
    {
        private static IConfiguration _configuration;

        public static IConfiguration GetConfiguration()
        {
            if (_configuration == null)
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true)
                    .AddJsonFile("appsettings.test.json", optional: true)
                    .AddEnvironmentVariables();

                _configuration = builder.Build();
            }

            return _configuration;
        }
    }
} 