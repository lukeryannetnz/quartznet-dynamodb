using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB.DataModel.Storage
{
    /// <summary>
    /// Deals with storing and retrieving Dynamo records from the Dynamo api.
    /// </summary>
    interface IRepository<T> : IDisposable
    {
        /// <summary>
        /// Load a single record of type T from the table T is associated with, matching the provided Key.
        /// </summary>
        /// <param name="key">Key.</param>
        T Load(Dictionary<string, AttributeValue> key);

        /// <summary>
        /// Store the specified entity in the table T is associated with.
        /// </summary>
        /// <param name="entity">Entity.</param>
        void Store(T entity);

        /// <summary>
		/// Store the specified entities in the table T is associated with.
		/// </summary>
		/// <param name="entities">The entities to store.</param>
		/// <exception cref="ArgumentNullException">Thrown if entities is null.</exception>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if there are no items in the entities collection.</exception>
		/// <exception cref="JobPersistenceException">Thrown if a non 200 HTTP code is received from DynamoDB.</exception>
		void Store(IList<T> entities);

        /// <summary>
        /// Store the specified entity, in the table T is associated with. 
        /// Only store the specific entity is the condition specified in conditionExpression is met..
        /// See dynamo docs for more details on conditions http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
        /// </summary>
        /// <param name="entity">Entity.</param>
        /// <param name="expressionAttributeValues">Expression attribute values. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html"/></param>
        /// <param name="conditionExpression">Condition expression. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ConditionExpression"/></param>
        /// <param name="expressionAttributeNames">Expression attribute names. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ExpressionAttributeNames"/></param>
        /// <returns>>ALL_OLD the values that were replaced. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ReturnValues"/> </returns>
        Dictionary<string, AttributeValue> Store(T entity, Dictionary<string, AttributeValue> expressionAttributeValues, Dictionary<string, string> expressionAttributeNames, string conditionExpression);

        /// <summary>
        /// Delete the entity with the specified key.
        /// </summary>
        /// <param name="key">Key.</param>
        void Delete(Dictionary<string, AttributeValue> key);

        /// <summary>
        /// Scan the table with the provided expressionAttributeValues and filterExpression.
        /// <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html"/> 
        /// </summary>
        /// <param name="expressionAttributeValues">Expression attribute values. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html"/></param>
        /// <param name="filterExpression">Filter expression. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#FilteringResults"/></param>
        /// <param name="expressionAttributeNames">Expression attribute names. <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ExpressionAttributeNames"/></param>
        IEnumerable<T> Scan(Dictionary<string, AttributeValue> expressionAttributeValues, Dictionary<string, string> expressionAttributeNames, string filterExpression);

        /// <summary>
        /// Deletes the table. This permanently deletes ALL DATA in the table!
        /// </summary>
        void DeleteTable();

        /// <summary>
        /// Returns information about the table, including the current status of the table, when it was created, the primary key schema, and any indexes on the table.
        /// <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html"/> 
        /// </summary>
        /// <returns>The table.</returns>
        DescribeTableResponse DescribeTable();
    }
}

