using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl;
using Quartz.Simpl;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel.Storage
{
	public interface IConvertToDynamoRecord
	{
		Dictionary<string, AttributeValue> ToDynamo ();
	}

}
