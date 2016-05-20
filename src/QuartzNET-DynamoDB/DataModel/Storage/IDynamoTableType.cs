using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB.DataModel.Storage
{
    /// <summary>
    /// Indicates that this type can be stored in a dynamo table.
    /// </summary>
    public interface IDynamoTableType
	{
		/// <summary>
		/// Gets the name of the dynamo table to store this type in.
		/// </summary>
		/// <value>The name of the dynamo table.</value>
		string DynamoTableName { get; }

		/// <summary>
		/// Gets the dynamo db key for this instance.
		/// </summary>
		/// <value>The key.</value>
		Dictionary<string, AttributeValue> Key { get; }
	}

}
