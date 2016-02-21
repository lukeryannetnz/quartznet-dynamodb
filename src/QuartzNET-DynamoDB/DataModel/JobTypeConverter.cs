using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.Simpl;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Converts a quartz JobType system.Type to and from dynamo db entry type.
    /// </summary>
    internal class JobTypeConverter : IPropertyConverter
    {
        private readonly SimpleTypeLoadHelper _typeHelper = new SimpleTypeLoadHelper();

        public DynamoDBEntry ToEntry(object value)
        {
            Type t = value as Type;

            if (t == null)
            {
                throw new ArgumentException("must be of type system.Type", nameof(value));
            }

            return GetStorableJobTypeName(t);
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            string typeString = entry;
            if (string.IsNullOrWhiteSpace(typeString))
            {
                throw new ArgumentException("must be of type string", nameof(entry));
            }

            return _typeHelper.LoadType(typeString);
        }

        private static string GetStorableJobTypeName(System.Type jobType)
        {
            return jobType.FullName + ", " + jobType.Assembly.GetName().Name;
        }
    }
}
