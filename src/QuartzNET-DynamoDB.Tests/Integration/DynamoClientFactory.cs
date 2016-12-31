using System;
using Amazon.DynamoDBv2;

namespace Quartz.DynamoDB.Tests
{
    public class TestJobStoreFactory
    {
        private static JobStore _store;
        private static string InstanceName;

        public static DynamoDB.JobStore CreateTestJobStore()
        {
            _store = new JobStore();
            InstanceName = Guid.NewGuid().ToString();
            _store.InstanceName = InstanceName;

            return _store;
        }

        public static AmazonDynamoDBClient BootStrapDynamo()
        {
            var client = DynamoDbClientFactory.Create();
            DynamoConfiguration.InstanceName = InstanceName;
            new DynamoBootstrapper().BootStrap(client);

            return client;
        }

        public static void CleanUpDynamo()
        {
            using (var client = DynamoDbClientFactory.Create())
            {
                DynamoConfiguration.InstanceName = InstanceName;

                foreach (var table in DynamoConfiguration.AllTableNames)
                {
                    client.DeleteTable(table);
                }
            }
        }
    }
}