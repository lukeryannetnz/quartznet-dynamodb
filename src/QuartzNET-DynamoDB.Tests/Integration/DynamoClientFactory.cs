using System;
using Amazon.DynamoDBv2;

namespace Quartz.DynamoDB.Tests.Integration
{
    public class DynamoClientFactory
    {
        private readonly string _instanceName = Guid.NewGuid().ToString();

        public DynamoDB.JobStore CreateTestJobStore()
        {
            var var = new DynamoDB.JobStore();
            var.InstanceName = _instanceName;

            return var;
        }

        public AmazonDynamoDBClient BootStrapDynamo()
        {
            var client = DynamoDbClientFactory.Create();
            DynamoConfiguration.InstanceName = _instanceName;
            new DynamoBootstrapper().BootStrap(client);

            return client;
        }

        public void CleanUpDynamo()
        {
            using (var client = DynamoDbClientFactory.Create())
            {
                DynamoConfiguration.InstanceName = _instanceName;

                foreach (var table in DynamoConfiguration.AllTableNames)
                {
                    client.DeleteTable(table);
                }
            }
        }
    }
}