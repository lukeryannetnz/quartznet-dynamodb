using System;
using Quartz.DynamoDB.DataModel.Storage;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An instance of a quartz job scheduler.
    /// </summary>
	public class DynamoScheduler : IDynamoTableType, IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord
    {
        private readonly DateTimeOffsetConverter converter = new DateTimeOffsetConverter();

        public string InstanceId { get; set; }

        /// <summary>
        /// Gets or sets the expires time as a unix epoch value in UTC timezone.
        /// 
        /// Data model scan conditions are only supported for simple properties, 
        /// not those that require converters so for now I've buried the converter inside this property.
        /// This may be the straw that breaks the camels back and causes me to move away from the
        /// DataModel to only using the DocumentModel.
        /// </summary>
        public int? ExpiresUtcEpoch
        {
            get;
			set;
        }

        /// <summary>
        /// Gets the expires time as a DateTimeOffset value in UTC timezone.
        /// This is just here for conversion convenience.
        /// </summary>
        public DateTimeOffset? ExpiresUtc
        {
            get
            {
                if (!ExpiresUtcEpoch.HasValue)
                {
                    return null;
                }

				return converter.FromEntry(ExpiresUtcEpoch.Value);
            }
            set
            {
				if (value.HasValue)
				{
					ExpiresUtcEpoch = converter.ToEntry (value.Value);
				}
            }
        }
			
		public string State { get; set; }

		public string DynamoTableName 
		{
			get 
			{
				return DynamoConfiguration.SchedulerTableName;
			}
		}

		public Dictionary<string, AttributeValue> Key 
		{ 
			get 
			{
				return CreateKeyDictionary (InstanceId);
			}
		}

		/// <summary>
		/// Creates a dynamo key dictionary for the Scheduler table with the provided instance id.
		/// </summary>
		/// <returns>The dynamo key.</returns>
		/// <param name="instanceId">Instance identifier.</param>
		public static Dictionary<string, AttributeValue> CreateKeyDictionary(string instanceId)
		{
			return new Dictionary<string, AttributeValue> () {
				{ "InstanceId", new AttributeValue ()
					{
						S = instanceId 
					}
				}
			};
		}

		public void InitialiseFromDynamoRecord (System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> record)
		{
			InstanceId = record ["InstanceId"].S;
			ExpiresUtcEpoch = record["ExpiresUtcEpoch"].NULL ? (int?)null : int.Parse(record["ExpiresUtcEpoch"].N);
			State = record["State"].NULL ? string.Empty : record["State"].S;
		}

		public System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> ToDynamo ()
		{
			Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

			record.Add("InstanceId", new AttributeValue { S = InstanceId });
			record.Add("ExpiresUtcEpoch", ExpiresUtcEpoch.HasValue ? new AttributeValue { N = ExpiresUtcEpoch.Value.ToString() } : new AttributeValue { NULL = true });
			record.Add("State", string.IsNullOrWhiteSpace(State) ? new AttributeValue { NULL = true } : new AttributeValue { S = State });


			return record;		
		}
    }
}
