using System;
using Amazon.DynamoDBv2;
using Quartz.DynamoDB;
using Quartz.DynamoDB.DataModel;
using Amazon.DynamoDBv2.Model;
using Quartz.Util;

namespace Quartz.DynamoDB.DataModel.Storage
{
	interface IRepository<TKey, T>
	{
		T Load (Key<TKey> key);
	}

}

