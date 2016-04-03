using System;
using Amazon.DynamoDBv2;
using Quartz.DynamoDB;
using Quartz.DynamoDB.DataModel;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Quartz.Util;
using System.Net;

namespace Quartz.DynamoDB.DataModel.Storage
{
	public class Repository<T> : IRepository<T> where T : IInitialisableFromDynamoRecord, IConvertableToDynamoRecord, IDynamoTableType, new()
	{
		private AmazonDynamoDBClient _client;

		public Repository (AmazonDynamoDBClient client)
		{
			_client = client;
		}

		public T Load(Dictionary<string, AttributeValue> key)
		{
			T entity = new T();

			if (key == null || key.Count < 1) 
			{
				throw new ArgumentException ("Invalid key provided");
			}

			var request = new GetItemRequest ()
			{ 
				TableName = entity.DynamoTableName, 
				Key = key
			};

			var response = _client.GetItem (request);

			if (response.IsItemSet) 
			{
				entity.InitialiseFromDynamoRecord (response.Item);
				return entity;
			} 

			return default(T);
		}

		public void Store(T entity)
		{
			if (entity == null) 
			{
				throw new ArgumentNullException (nameof(entity));
			}

			var dictionary = entity.ToDynamo();
			var response = _client.PutItem(new PutItemRequest(entity.DynamoTableName, dictionary));

			if(response.HttpStatusCode != HttpStatusCode.OK)
			{
				throw new JobPersistenceException($"Non 200 response code received from dynamo {response.ToString()}");
			}
		}

		public void Delete(Dictionary<string, AttributeValue> key)
		{
			if (key == null) 
			{
				throw new ArgumentException ("Invalid key provided.");
			}

			T entity = new T();

			var response = _client.DeleteItem (new DeleteItemRequest (entity.DynamoTableName, key));

			if(response.HttpStatusCode != HttpStatusCode.OK)
			{
				throw new JobPersistenceException($"Non 200 response code received from dynamo {response.ToString()}");
			}
		}

		public IEnumerable<T> Scan(Dictionary<string,AttributeValue> expressionAttributeValues, string filterExpression)
		{
			T entity = new T();

			var request = new ScanRequest
			{
				TableName = entity.DynamoTableName,
			};

			if (expressionAttributeValues != null) 
			{
				request.ExpressionAttributeValues = expressionAttributeValues;
			}

			if (!string.IsNullOrWhiteSpace (filterExpression)) 
			{
				request.FilterExpression = filterExpression;
			}

			var response = _client.Scan(request);
			var result = response.Items;

			List<T> matchedRecords = new List<T>();

			foreach (Dictionary<string, AttributeValue> item in response.Items)
			{
				T value = new T ();
				value.InitialiseFromDynamoRecord (item);

				matchedRecords.Add (value);
			}

			return matchedRecords;
		}
	}
}

