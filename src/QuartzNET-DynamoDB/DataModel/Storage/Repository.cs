using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Polly;

namespace Quartz.DynamoDB.DataModel.Storage
{
    public class Repository<T> : IRepository<T> where T : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType, new()
    {
        private readonly AmazonDynamoDBClient _client;
        private readonly Policy _writeRetryPolicy;
        private readonly Policy _readRetryPolicy;

        public Repository(AmazonDynamoDBClient client)
        {
            _client = client;
            _writeRetryPolicy = Policy
                .Handle<ProvisionedThroughputExceededException>()
                .WaitAndRetry(5, ExponentialBackoffWithRandomVariation.CalculateWaitDuration);

            _readRetryPolicy = Policy
             .Handle<ProvisionedThroughputExceededException>()
             .WaitAndRetry(3, ExponentialBackoffWithRandomVariation.CalculateWaitDuration);
        }

        public T Load(Dictionary<string, AttributeValue> key)
        {
            T entity = new T();

            if (key == null || key.Count < 1)
            {
                throw new ArgumentException("Invalid key provided");
            }

            var request = new GetItemRequest()
            {
                TableName = entity.DynamoTableName,
                Key = key,
                ConsistentRead = true
            };

            try
            {
                var response = _readRetryPolicy.Execute(() => _client.GetItem(request));

                if (response.IsItemSet)
                {
                    entity.InitialiseFromDynamoRecord(response.Item);
                    return entity;
                }
            }
            catch (ResourceNotFoundException)
            {
            }

            return default(T);
        }

        public void Store(IList<T> entities)
        {
            if (entities == null)
            {
                throw new ArgumentNullException(nameof(entities));
            }

            if (!entities.Any())
            {
                throw new ArgumentOutOfRangeException(nameof(entities));
            }

            List<T> batch = new List<T>();
            for (int i = 1; i <= entities.Count(); i++)
            {
                batch.Add(entities[i - 1]);
                if (i == entities.Count())
                {
                    // If we've reached the end of the collection, send off the save request
                    SendBatchWriteRequest(batch);
                }
                else if (i % 25 == 0)
                {
                    // If we've reached a factor of 25, send off the save request.
                    SendBatchWriteRequest(batch);
                    // Then clear the collection and keep going.
                    batch.Clear();
                }
            }
        }

        private void SendBatchWriteRequest(IEnumerable<T> items)
        {
            BatchWriteItemRequest batchRequest = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>()
            };

            foreach (var entity in items)
            {
                if (!batchRequest.RequestItems.ContainsKey(entity.DynamoTableName))
                {
                    batchRequest.RequestItems.Add(entity.DynamoTableName, new List<WriteRequest>());
                }
                batchRequest.RequestItems[entity.DynamoTableName].Add(new WriteRequest(new PutRequest(entity.ToDynamo())));
            }

            var response = _writeRetryPolicy.Execute(() => _client.BatchWriteItem(batchRequest));

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException($"Non 200 response code received from dynamo {response}");
            }
        }

        public void Store(T entity)
        {
            Store(entity, null, null, string.Empty);
        }

        public Dictionary<string, AttributeValue> Store(T entity, Dictionary<string, AttributeValue> expressionAttributeValues, Dictionary<string, string> expressionAttributeNames, string conditionExpression)
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            var dictionary = entity.ToDynamo();
            var request = new PutItemRequest(entity.DynamoTableName, dictionary);

            if (!string.IsNullOrWhiteSpace(conditionExpression))
            {
                request.ConditionExpression = conditionExpression;
                request.ExpressionAttributeValues = expressionAttributeValues;
                request.ExpressionAttributeNames = expressionAttributeNames;
                request.ReturnValues = ReturnValue.ALL_OLD;
            }

            var response = _writeRetryPolicy.Execute(() => _client.PutItem(request));

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException($"Non 200 response code received from dynamo {response}");
            }

            return response.Attributes;
        }

        public void Delete(Dictionary<string, AttributeValue> key)
        {
            if (key == null)
            {
                throw new ArgumentException("Invalid key provided.");
            }

            T entity = new T();

            var response = _writeRetryPolicy.Execute(() => _client.DeleteItem(new DeleteItemRequest(entity.DynamoTableName, key)));

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException($"Non 200 response code received from dynamo {response}");
            }
        }

        public IEnumerable<T> Scan(Dictionary<string, AttributeValue> expressionAttributeValues, Dictionary<string, string> expressionAttributeNames, string filterExpression)
        {
            T entity = new T();

            var request = new ScanRequest
            {
                TableName = entity.DynamoTableName,
                ConsistentRead = true
            };

            if (expressionAttributeValues != null)
            {
                request.ExpressionAttributeValues = expressionAttributeValues;
            }

            if (expressionAttributeNames != null)
            {
                request.ExpressionAttributeNames = expressionAttributeNames;
            }

            if (!string.IsNullOrWhiteSpace(filterExpression))
            {
                request.FilterExpression = filterExpression;
            }

            List<T> matchedRecords = new List<T>();

            try
            {
                var response = _readRetryPolicy.Execute(() => _client.Scan(request));

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    T value = new T();
                    value.InitialiseFromDynamoRecord(item);

                    matchedRecords.Add(value);
                }
            }
            catch (ResourceNotFoundException)
            {
            }

            return matchedRecords;
        }

        public void DeleteTable()
        {
            T entity = new T();

            var response = _writeRetryPolicy.Execute(() => _client.DeleteTable(entity.DynamoTableName));

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException($"Non 200 response code received from dynamo {response}");
            }
        }

        public DescribeTableResponse DescribeTable()
        {
            T entity = new T();

            return _writeRetryPolicy.Execute(() => _client.DescribeTable(entity.DynamoTableName));
        }

        #region IDisposable implementation

        bool _disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _client?.Dispose();
                }

                _disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }

        #endregion
    }
}

