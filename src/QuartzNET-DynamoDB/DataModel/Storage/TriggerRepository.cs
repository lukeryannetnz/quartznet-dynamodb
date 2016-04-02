using System;
using Amazon.DynamoDBv2;
using Quartz.DynamoDB;
using Quartz.DynamoDB.DataModel;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Quartz.Util;

namespace Quartz.DynamoDB.DataModel.Storage
{
	/// <summary>
	/// Deals with storing and retrieving DynamoTriggers from the Dynamo api.
	/// </summary>
	public class Repository<T, TKey> : IRepository<TKey, T> where T : IInitialiseFromDynamoRecord, IDynamoTableType, new()
	{
		private AmazonDynamoDBClient _client;

		public Repository (AmazonDynamoDBClient client)
		{
			_client = client;
		}

		public T Load(Key<TKey> key)
		{
			T entity = default(T);

			if (key == null || string.IsNullOrWhiteSpace (key.Group) || string.IsNullOrWhiteSpace (key.Name)) 
			{
				throw new ArgumentException ("Invalid key provided");
			}

			var request = new GetItemRequest ()
			{ 
				TableName = entity.DynamoTableName, 
				Key = new Dictionary<string, AttributeValue> 
				{ 
					{"Group", new AttributeValue(){ S = key.Group}}, 
					{"Name", new AttributeValue(){ S = key.Name}}
				}
			};

			var response = _client.GetItem (request);

			if (response.IsItemSet)
			{
				entity.InitialiseFromDynamoRecord (response.Item);
			}	

			return entity;
		}

		public void Store(T entity)
		{
			if (entity == null) 
			{
				throw new ArgumentException ("Invalid job provided. Must have Job property set which must have Key property set.");
			}

			var dictionary = job.ToDynamo();
			var response = _client.PutItem(new PutItemRequest(DynamoConfiguration.JobDetailTableName, dictionary));

			if(response.HttpStatusCode != HttpStatusCode.OK)
			{
				throw new JobPersistenceException($"Non 200 response code received from dynamo {response.ToString()}");
			}
		}
	}
}

