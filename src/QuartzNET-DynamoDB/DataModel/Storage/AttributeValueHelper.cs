using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB.DataModel.Storage
{
    public static class AttributeValueHelper
    {
        public static AttributeValue StringOrNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return new AttributeValue() { NULL = true };
            }

            return new AttributeValue() { S = value };
        }
    }
}
