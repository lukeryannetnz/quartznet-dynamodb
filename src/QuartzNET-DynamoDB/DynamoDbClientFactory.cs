using System;
using Amazon.DynamoDBv2;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// Creates instances of the dynamo db client.
    /// </summary>
    public static class DynamoDbClientFactory
    {
        public static AmazonDynamoDBClient Create()
        {
            if (!string.IsNullOrWhiteSpace(Quartz.DynamoDB.DynamoConfiguration.ServiceUrl))
            {
                // First, set up a DynamoDB client for DynamoDB Local
                AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
                ddbConfig.ServiceURL = Quartz.DynamoDB.DynamoConfiguration.ServiceUrl;

                return new AmazonDynamoDBClient(ddbConfig);
            }

            // If no url in the config, use the profile and region from the config.
            return new AmazonDynamoDBClient();
        }
    }
}
