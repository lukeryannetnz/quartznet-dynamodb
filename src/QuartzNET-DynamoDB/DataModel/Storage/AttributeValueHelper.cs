using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.DynamoDB.DataModel.Storage
{
    internal static class AttributeValueHelper
    {
        internal static AttributeValue StringOrNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return new AttributeValue() { NULL = true };
            }

            return new AttributeValue() { S = value };
        }
    }
}
