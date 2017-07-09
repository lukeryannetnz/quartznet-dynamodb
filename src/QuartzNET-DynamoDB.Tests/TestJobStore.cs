namespace Quartz.DynamoDB.Tests
{
    using Quartz.DynamoDB.Tests.Integration;

    /// <summary>
    /// Job store that uses a test dynamo boot strapper.
    /// </summary>
    public class TestJobStore : JobStore
    {
        public TestJobStore() : base(new TestDynamoBootstrapper())
        {
        }
    }
}
