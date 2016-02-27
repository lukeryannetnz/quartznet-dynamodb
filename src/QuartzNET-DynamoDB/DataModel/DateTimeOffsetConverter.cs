using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Converts .NET DateTime objects to integer UTC unix-EPOCH time format for storage.
    /// <see cref="http://www.epochconverter.com/"/>
    /// DateTimeOffset is preferable to DateTime as it is explicit about timezone and stores everything relative to UTC, rather than assuming local.
    /// Note that this converter doesn't serialize the offset. If a local offset is provided, the same time is returned in UTC.
    /// </summary>
    public class DateTimeOffsetConverter : IPropertyConverter
    {
        /// <summary>
        /// Returns a dynamodb entry (int) with the datetime relative to unix epoch time.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DynamoDBEntry ToEntry(object value)
        {
            DateTimeOffset? offset = value as DateTimeOffset?;
            if (!offset.HasValue)
            {
                return null;
            }

            return offset.Value.UtcDateTime.ToUnixEpochTime();
        }

        /// <summary>
        /// Returns the UTC DateTimeOffset that the epoch dynamo record represents.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public object FromEntry(DynamoDBEntry entry)
        {
            int secondsSinceUtcEpoch = entry.AsInt();
            var utcDateTime = UnixEpochDateTimeExtensions.UtcEpochTime.AddSeconds(secondsSinceUtcEpoch);

            return new DateTimeOffset(utcDateTime);
        }
    }
}
