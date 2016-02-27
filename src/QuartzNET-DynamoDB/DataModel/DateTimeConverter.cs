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
        private readonly DateTime _epochTime = new DateTime(1970, 1, 1);

        public DynamoDBEntry ToEntry(object value)
        {
            DateTime dt = (DateTime)value;

            TimeSpan t = dt - _epochTime;
            int secondsSinceEpoch = (int)t.TotalSeconds;

            return secondsSinceEpoch;
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            int secondsSinceEpoch = entry.AsInt();

            return _epochTime.AddSeconds(secondsSinceEpoch);
        }
    }
}