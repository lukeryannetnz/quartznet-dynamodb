using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel.Storage
{
	/// <summary>
	/// Indicates that this type can be stored in a dynamo table.
	/// </summary>
	public interface IDynamoTableType
	{
		/// <summary>
		/// Gets the name of the dynamo table.
		/// </summary>
		/// <value>The name of the dynamo table.</value>
		string DynamoTableName { get; }
	}

}
