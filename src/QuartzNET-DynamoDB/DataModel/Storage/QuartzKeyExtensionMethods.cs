using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB
{
	public static class QuartzKeyExtensionMethods
	{
		public static Dictionary<string, AttributeValue> ToDictionary(this JobKey key)
		{
			return new Dictionary<string, AttributeValue> { 
				{ "Group", new AttributeValue (){ S = key.Group } }, 
				{ "Name", new AttributeValue (){ S = key.Name } }
			};
		}

		public static Dictionary<string, AttributeValue> ToDictionary(this TriggerKey key)
		{
			return new Dictionary<string, AttributeValue> { 
				{ "Group", new AttributeValue (){ S = key.Group } }, 
				{ "Name", new AttributeValue (){ S = key.Name } }
			};
		}
	}
}

