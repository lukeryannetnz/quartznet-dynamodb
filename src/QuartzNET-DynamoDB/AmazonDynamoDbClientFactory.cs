using Amazon.DynamoDBv2;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// Creates instances of the dynamo db client.
    /// </summary>
    internal static class AmazonDynamoDbClientFactory
    {
        internal static AmazonDynamoDBClient Create()
        {
            // First, set up a DynamoDB client for DynamoDB Local
            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://localhost:8000";
            
            return  new AmazonDynamoDBClient(ddbConfig); 
        }
    }
}
