using System;
using Amazon.DynamoDBv2;
using Quartz.DynamoDB;
using Quartz.DynamoDB.DataModel;
using Amazon.DynamoDBv2.Model;
using Quartz.Util;

namespace Quartz.DynamoDB.DataModel.Storage
{
	/// <summary>
	/// Deals with storing and retrieving Dynamo records from the Dynamo api.
	/// </summary>
	interface IRepository<T, TKey>
	{
		/// <summary>
		/// Load a single record of type T from the table T is associated with, matching the provided Key.
		/// </summary>
		/// <param name="key">Key.</param>
		T Load (Key<TKey> key);

		/// <summary>
		/// Store the specified entity in the table T is associated with.
		/// </summary>
		/// <param name="entity">Entity.</param>
		void Store (T entity);
	}
}

