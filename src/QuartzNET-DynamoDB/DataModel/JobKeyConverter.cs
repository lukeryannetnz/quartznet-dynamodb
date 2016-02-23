using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A converter from quartz JobKey to dynamo Document and back.
    /// </summary>
    public class JobKeyConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            JobKey key = value as JobKey;
            if (key == null)
            {
                throw new ArgumentException("value must be of type JobKey");
            }

            Document doc = new Document
            {
                ["Name"] = key.Name,
                ["Group"] = key.Group
            };
            return doc;
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            Document doc = entry as Document;
            if (doc == null)
            {
                throw new ArgumentException("entry must be of type Amazon.DynamoDBv2.DocumentModel.Document");
            }

            string name = doc["Name"];
            string group = doc["Group"];

            return new JobKey(name, group);
        }
    }
}