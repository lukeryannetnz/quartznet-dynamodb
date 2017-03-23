using Amazon.DynamoDBv2;

namespace Quartz.DynamoDB.Tests.Integration
{
    class TestDynamoBootstrapper : DynamoBootstrapper
    {
        /// <summary>
        /// The local dynamodb doesn't yet support TTL so do nothing when this call is made otherwise an
        /// unknown operation exception is thrown by local dynamo.
        /// </summary>
        protected override void SetTimeToLive(IAmazonDynamoDB client, string tableName, string attributeName)
        {
            return;
        }
    }
}
