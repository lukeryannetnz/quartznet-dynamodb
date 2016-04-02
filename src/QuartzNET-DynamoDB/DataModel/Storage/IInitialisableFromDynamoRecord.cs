using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel.Storage
{
	/// <summary>
	/// Indicates that this type can be initialised from dynamo db record.
	/// </summary>
	public interface IInitialisableFromDynamoRecord
	{
		/// <summary>
		/// Initialises the instance from dynamo record retrieved from the dynamo API.
		/// </summary>
		/// <param name="record">Record.</param>
		void InitialiseFromDynamoRecord (Dictionary<string, AttributeValue> record);
	}
}
