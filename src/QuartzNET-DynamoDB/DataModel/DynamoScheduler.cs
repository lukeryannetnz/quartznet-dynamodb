using System;
using Amazon.DynamoDBv2.DataModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An instance of a quartz job scheduler.
    /// </summary>
    [DynamoDBTable("Scheduler")]
    public class DynamoScheduler
    {
        private readonly DateTimeOffsetConverter converter = new DateTimeOffsetConverter();

        [DynamoDBHashKey]
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
            get;set;
        }

        /// <summary>
        /// Gets the expires time as a DateTimeOffset value in UTC timezone.
        /// Ignored so it isn't stored, this is just here for conversion convenience.
        /// </summary>
        [DynamoDBIgnore]
        public DateTimeOffset? ExpiresUtc
        {
            get
            {
                if (!ExpiresUtcEpoch.HasValue)
                {
                    return null;
                }

                return (DateTimeOffset)converter.FromEntry(ExpiresUtcEpoch);
            }
            set
            {
                ExpiresUtcEpoch = (int?)converter.ToEntry(value);
            }
        }

        public string State { get; set; }
    }
}
