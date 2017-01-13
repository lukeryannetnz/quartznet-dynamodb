using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A wrapper class for a Quartz Job Group instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    public class DynamoJobGroup : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType
    {
        public string Name
        {
            get;
            set;
        }

        public DynamoJobGroupState State
        {
            get;
            set;
        }

        public string DynamoTableName
        {
            get
            {
                return DynamoConfiguration.JobGroupTableName;
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

            record.Add("Name", AttributeValueHelper.StringOrNull(Name));
            record.Add("State", AttributeValueHelper.StringOrNull(State.ToString()));

            return record;
        }

        public void InitialiseFromDynamoRecord(Dictionary<string, AttributeValue> record)
        {
            Name = record["Name"].S;
            State = (DynamoJobGroupState)Enum.Parse(typeof(DynamoJobGroupState), record["State"].S);
        }
    }
}