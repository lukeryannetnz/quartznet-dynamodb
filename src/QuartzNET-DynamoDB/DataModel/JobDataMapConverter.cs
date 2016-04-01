using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Quartz.Simpl;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Coverts a JobDataMap object to a dynamo db type.
    /// Uses the NewtonSoft JSON serializer and type reflection to serialize objects to strings. Could no doubt be improved.
    /// </summary>
    public class JobDataMapConverter
    {
        private readonly SimpleTypeLoadHelper _typeHelper = new SimpleTypeLoadHelper();

		public AttributeValue ToEntry(JobDataMap dataMap)
        {
            if (dataMap == null)
            {
				throw new ArgumentNullException("dataMap");
            }

			var serializedData = new AttributeValue () 
			{
				M = new Dictionary<string, AttributeValue>()
			};

            foreach (KeyValuePair<string, object> keyValuePair in dataMap)
            {
                string o = JsonConvert.SerializeObject(keyValuePair.Value);
                string type = GetStorableJobTypeName(keyValuePair.Value.GetType());
				serializedData.M.Add (keyValuePair.Key, new AttributeValue () { 
					M = new Dictionary<string, AttributeValue> () {
						{ "type", new AttributeValue (){ S = type } },
						{ "object", new AttributeValue () { S = o } }
					}
				});
            }

			return serializedData;
        }

		public JobDataMap FromEntry(AttributeValue entry)
        {
			if (entry == null)
            {
				throw new ArgumentNullException(nameof(entry));
            }

            IDictionary<string, object> deserializedData = new Dictionary<string, object>();

			foreach (var keyValuePair in entry.M)
            {
				var type = keyValuePair.Value.M["type"].S;
                Type t = _typeHelper.LoadType(type);
				object o = JsonConvert.DeserializeObject(keyValuePair.Value.M["object"].S, t);
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