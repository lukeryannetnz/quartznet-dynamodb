using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Spi;
using Quartz.Impl.Triggers;
using System.Linq;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel
{
	/// <summary>
	/// A wrapper class for a Quartz Trigger Group instance that can be serialized and stored in Amazon DynamoDB.
	/// </summary>
	public class DynamoTriggerGroup : IInitialisableFromDynamoRecord,IConvertibleToDynamoRecord, IDynamoTableType
	{
		public enum DynamoTriggerGroupState
		{
			/// <summary>
			/// Indicates that the Trigger Group is Active.
			/// </summary>
			Active = 0,

			/// <summary>
			/// Indicates that the Trigger Group is Paused.
			/// This means that all triggers in this Group should also be Paused, including
			/// any new triggers that are added to it.
			/// </summary>
			Paused = 1,
		}

		public string Name
		{
			get;
			set;
		}

		public DynamoTriggerGroupState State
		{
			get;
			set;
		}

		public string DynamoTableName
		{
			get
			{
				return "DynamoTriggerGroup";
			}
		}

		public Dictionary<string, AttributeValue> Key
		{
			get
			{
				return new Dictionary<string, AttributeValue> { 
					{ "Name", new AttributeValue (){ S = Name } }
				};
			}
		}

		public Dictionary<string, AttributeValue> ToDynamo()
		{
			Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

			record.Add("Name", AttributeValueHelper.StringOrNull (Name));
			record.Add("State", AttributeValueHelper.StringOrNull (State.ToString()));

			return record;
		}

		public void InitialiseFromDynamoRecord(Dictionary<string, AttributeValue> record)
		{
			Name = record["Name"].S;
			State = (DynamoTriggerGroupState)Enum.Parse(typeof(DynamoTriggerGroupState), record ["State"].S);
		}
	}
}