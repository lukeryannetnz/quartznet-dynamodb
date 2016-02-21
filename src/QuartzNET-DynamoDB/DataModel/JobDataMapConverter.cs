using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Quartz.Simpl;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Coverts a JobDataMap object to a dynamo db type.
    /// Uses the NewtonSoft JSON serializer and type reflection to serialize objects to strings. Could no doubt be improved.
    /// </summary>
    public class JobDataMapConverter : IPropertyConverter
    {
        private readonly SimpleTypeLoadHelper _typeHelper = new SimpleTypeLoadHelper();

        public DynamoDBEntry ToEntry(object value)
        {
            JobDataMap dataMap = value as JobDataMap;

            if (dataMap == null)
            {
                throw new ArgumentException("must be of type JobKey", nameof(value));
            }

            var serializedData = new Dictionary<string, DynamoDBEntry>();

            foreach (KeyValuePair<string, object> keyValuePair in dataMap)
            {
                string o = JsonConvert.SerializeObject(keyValuePair.Value);
                Document doc = new Document();
                doc["object"] = o;
                doc["type"] = GetStorableJobTypeName(keyValuePair.Value.GetType());
                serializedData.Add(keyValuePair.Key, doc);
            }

            return new Document(serializedData);
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            Document document = entry as Document;

            if (document == null)
            {
                throw new ArgumentException("must be of type Document", nameof(entry));
            }

            IDictionary<string, object> deserializedData = new Dictionary<string, object>();

            foreach (KeyValuePair<string, DynamoDBEntry> keyValuePair in document)
            {
                Document doc = (Document)keyValuePair.Value;
                Type t = _typeHelper.LoadType(doc["type"]);
                object o = JsonConvert.DeserializeObject(doc["object"], t);
                deserializedData.Add(keyValuePair.Key, o);
            }

            return new JobDataMap(deserializedData);
        }

        private static string GetStorableJobTypeName(System.Type jobType)
        {
            return jobType.FullName + ", " + jobType.Assembly.GetName().Name;
        }
    }
}