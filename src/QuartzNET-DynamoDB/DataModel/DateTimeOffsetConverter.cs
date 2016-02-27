using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Quartz.DynamoDB.DataModel
{
    public class DateTimeOffsetConverter : IPropertyConverter
    {
        private readonly DateTimeConverter _dateTimeConverter = new DateTimeConverter();

        public DynamoDBEntry ToEntry(object value)
        {
            DateTimeOffset? offset = value as DateTimeOffset?;
            if (!offset.HasValue)
            {
                return null;
            }

            return _dateTimeConverter.ToEntry(offset.Value.UtcDateTime);
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            var utcDateTime = (DateTime)_dateTimeConverter.FromEntry(entry);

            return new DateTimeOffset(utcDateTime);
        }
    }
}
