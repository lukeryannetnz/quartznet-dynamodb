using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Contains helper extensions to the Amazon dynamo db Document model types.
    /// </summary>
    public static class DynamoDocumentExtensions
    {
        public static string TryGetStringValueOtherwiseReturnDefault(this Document doc, string key)
        {
            DynamoDBEntry value;
            if (doc.TryGetValue(key, out value))
            {
                return value;
            }

            return string.Empty;
        }
    }
}