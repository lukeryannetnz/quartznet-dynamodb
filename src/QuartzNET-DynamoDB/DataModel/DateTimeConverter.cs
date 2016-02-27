using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Converts .NET DateTime objects to integer unix-EPOCH time format for storage.
    /// <see cref="http://www.epochconverter.com/"/>
    /// </summary>
    public class DateTimeConverter : IPropertyConverter
    {

        public DynamoDBEntry ToEntry(object value)
        {
            DateTime dt = (DateTime)value;
            
            return dt.ToUnixEpochTime();
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            int secondsSinceEpoch = entry.AsInt();

            return UnixEpochDateTimeExtensions.EpochTime.AddSeconds(secondsSinceEpoch);
        }
    }

    /// <summary>
    /// DateTime helpers for UnixEpoch time.
    /// </summary>
    public static class UnixEpochDateTimeExtensions
    {
        public static readonly DateTime EpochTime = new DateTime(1970, 1, 1);

        public static int ToUnixEpochTime(this DateTime datetime)
        {
            TimeSpan t = datetime - EpochTime;
            int secondsSinceEpoch = (int)t.TotalSeconds;

            return secondsSinceEpoch;
        }
    }
}