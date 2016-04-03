using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel.Storage
{
	/// <summary>
	/// Indicates that this type can be converted to a dynamo db record.
	/// </summary>
	public interface IConvertibleToDynamoRecord
	{
		/// <summary>
		/// Converts the given instance to a dynamo db record.
		/// </summary>
		/// <returns>The dynamo db record.</returns>
		Dictionary<string, AttributeValue> ToDynamo ();
	}

}
