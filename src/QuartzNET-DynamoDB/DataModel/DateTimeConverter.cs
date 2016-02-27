using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Converts .NET DateTime objects to integer unix-EPOCH time format for storage.
    /// <see cref="http://www.epochconverter.com/"/>
    /// If given a local time, will convert it to UTC on return.
    /// </summary>
    public class DateTimeConverter : IPropertyConverter
    {
        /// <summary>
        /// Returns a dynamodb entry (int) with the datetime relative to unix epoch time.
        /// If the input datetime.Kind is not UTC, will convert it to UTC.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DynamoDBEntry ToEntry(object value)
        {
            DateTime dt = (DateTime)value;
            
            return dt.ToUnixEpochTime();
        }

        /// <summary>
        /// Returns the UTC DateTime  that the epoch dynamo record represents.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public object FromEntry(DynamoDBEntry entry)
        {
            int secondsSinceEpoch = entry.AsInt();

            return UnixEpochDateTimeExtensions.UtcEpochTime.AddSeconds(secondsSinceEpoch);
        }
    }
}