using System;
using Quartz.DynamoDB.DataModel;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using System.Net;

namespace Quartz.DynamoDB
{
	/// <summary>
	/// Deals with storing and retrieving DynamoJobs from the Dynamo API.
	/// </summary>
	public class JobRepository
	{
		private AmazonDynamoDBClient _client;

		public JobRepository (AmazonDynamoDBClient client)
		{
			_client = client;
		}

		public DynamoJob LoadJob(JobKey key)
		{
			if (key == null || string.IsNullOrWhiteSpace (key.Group) || string.IsNullOrWhiteSpace (key.Name)) 
			{
				throw new ArgumentException ("Invalid key provided");
			}

			var request = new GetItemRequest ()
			{ 
				TableName = DynamoConfiguration.JobDetailTableName, 
				Key = new Dictionary<string, AttributeValue> 
				{ 
					{"Group", new AttributeValue(){ S = key.Group}}, 
					{"Name", new AttributeValue(){ S = key.Name}}
				}
			};
					
			var response = _client.GetItem (request);

			return response.IsItemSet ? new DynamoJob (response.Item) : null;
		}

		public void StoreJob(DynamoJob job)
		{
			if (job == null 
				|| job.Job == null 
				|| string.IsNullOrWhiteSpace (job.Job.Key.Group) 
				|| string.IsNullOrWhiteSpace (job.Job.Key.Name)) 
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

