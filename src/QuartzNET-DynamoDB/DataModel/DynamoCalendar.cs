using System;
using Quartz.DynamoDB.DataModel.Storage;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB
{
	/// <summary>
	/// A wrapper class for a Quartz Calendar instance that can be serialized and stored in Amazon DynamoDB.
	/// </summary>
	public class DynamoCalendar : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType
	{
		public DynamoCalendar ()
		{
		}

		public string Name
		{
			get;
			set;
		}

		public System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> ToDynamo()
		{
			Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

			record.Add("Name", AttributeValueHelper.StringOrNull (Name));

			return record;		
		}

		public void InitialiseFromDynamoRecord(System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> record)
		{
			Name = record["Name"].S;
		}

		public string DynamoTableName
		{
			get
			{
				return DynamoConfiguration.CalendarTableName;
			}
		}

		public System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> Key
		{
				get
				{
					return new Dictionary<string, AttributeValue> { 
						{ "Name", new AttributeValue (){ S = Name } }
					};
				}			
		}
	}
}

