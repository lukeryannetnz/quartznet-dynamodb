using System;
using System.Collections.Generic;
using System.Threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// Bootstraps dynamo db to the required state. Ensures tables exist etc.
    /// </summary>
    public class DynamoBootstrapper
    {
        public void BootStrap(IAmazonDynamoDB client)
        {
            if (ShouldCreate(client, DynamoConfiguration.JobDetailTableName))
            {
                CreateJobDetailTable(client);
            }

            if (ShouldCreate(client, DynamoConfiguration.JobGroupTableName))
            {
                CreateJobGroupTable(client);
            }

            if (ShouldCreate(client, DynamoConfiguration.TriggerTableName))
            {
                CreateTriggerTable(client);
            }

            if (ShouldCreate(client, DynamoConfiguration.TriggerGroupTableName))
            {
                CreateTriggerGroupTable(client);
            }

            if (ShouldCreate(client, DynamoConfiguration.SchedulerTableName))
            {
                CreateSchedulerTable(client);
            }

            if (ShouldCreate(client, DynamoConfiguration.CalendarTableName))
            {
                CreateCalendarTable(client);
            }
        }

        private bool ShouldCreate(IAmazonDynamoDB client, string tableName)
        {
            if (!TableExists(client, tableName))
            {
                Console.WriteLine($"Table {tableName} doesn't exist.");
                return true;
            }

            try
            {
                var table = client.DescribeTable(tableName);

                Console.WriteLine($"Table {tableName} status {table.Table.TableStatus}");

                if (table.Table.TableStatus == TableStatus.CREATING
                    || table.Table.TableStatus == TableStatus.UPDATING)
                {
                    EnsureTableActive(client, tableName);
                    return false;
                }
                if (table.Table.TableStatus == TableStatus.DELETING)
                {
                    EnsureTableDeleted(client, tableName);
                    return true;
                }
                if (table.Table.TableStatus == TableStatus.ACTIVE)
                {
                    return false;
                }
            }
            catch (ResourceNotFoundException)
            {
                return true;
            }

            throw new Exception($"Not sure if we should create table: {tableName}. Unknown table status, panic!");
        }

        private static void EnsureTableDeleted(IAmazonDynamoDB client, string tableName)
        {
            for (int i = 0; i < 120; i++)
            {
                if (TableDeleted(client, tableName))
                {
                    return;
                }

                Console.WriteLine($"Waiting for Table {tableName} to delete.");

                Thread.Sleep(DynamoConfiguration.BootstrapRetryDelayMilliseconds);
            }

            throw new Exception($"Table {tableName} not created within a reasonable time. Panic!");
        }

        private static bool TableDeleted(IAmazonDynamoDB client, string tableName)
        {
            try
            {
                client.DescribeTable(tableName);
            }
            catch (ResourceNotFoundException)
            {
                return true;
            }

            return false;
        }

        private static void EnsureTableActive(IAmazonDynamoDB client, string tableName)
        {
            for (int i = 0; i < 120; i++)
            {
                try
                {
                    if (TableActive(client, tableName))
                    {
                        return;
                    }
                }
                catch (ResourceNotFoundException)
                {
                }

                Console.WriteLine($"Waiting for Table {tableName} to become active.");

                Thread.Sleep(DynamoConfiguration.BootstrapRetryDelayMilliseconds);
            }

            throw new Exception($"Table {tableName} not created within a reasonable time. Panic!");
        }

        private static bool TableActive(IAmazonDynamoDB client, string tableName)
        {
            try
            {
                var table = client.DescribeTable(tableName);
                if (table.Table.TableStatus == TableStatus.ACTIVE)
                {
                    return true;
                }
            }
            catch (ResourceNotFoundException)
            {
            }

            return false;
        }

        private static bool TableExists(IAmazonDynamoDB client, string tableName)
        {
            string lastEvaluatedTableName = null;

            do
            {
                // Create a request object to specify optional parameters.
                var request = new ListTablesRequest
                {
                    Limit = 10, // Page size.
                    ExclusiveStartTableName = lastEvaluatedTableName
                };

                var response = client.ListTables(request);

                if (response.TableNames.Contains(tableName))
                {
                    return true;
                }

                lastEvaluatedTableName = response.LastEvaluatedTableName;
            }
            while (lastEvaluatedTableName != null);

            return false;
        }

        private static void CreateJobDetailTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.JobDetailTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Group",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Group",
                        KeyType = "HASH"
                    },
                    new KeySchemaElement
                    {
                        AttributeName = "Name",
                        KeyType = "RANGE"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }

        private void CreateJobGroupTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.JobGroupTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Name",
                        KeyType = "HASH"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }

        private void CreateCalendarTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.CalendarTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Name",
                        KeyType = "HASH"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }

        private void CreateTriggerGroupTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.TriggerGroupTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Name",
                        KeyType = "HASH"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }

        private void CreateTriggerTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.TriggerTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Group",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Group",
                        KeyType = "HASH"
                    },
                    new KeySchemaElement
                    {
                        AttributeName = "Name",
                        KeyType = "RANGE"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }

        private void CreateSchedulerTable(IAmazonDynamoDB client)
        {
            // Build a 'CreateTableRequest' for the new table
            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = DynamoConfiguration.SchedulerTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "InstanceId",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "InstanceId",
                        KeyType = "HASH"
                    }
                }
            };

            Console.WriteLine($"Creating table {createRequest.TableName}.");

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);

            SetTimeToLive(client, DynamoConfiguration.SchedulerTableName, "ExpiresUtcEpoch");
        }

        /// <summary>
        /// Enables the time to live feature for the provided table on the given attribute name.
        /// Note that it may take up to one hour for the change to fully process.
        /// <see href="https://aws.amazon.com/blogs/developer/time-to-live-support-in-amazon-dynamodb/"/>
        /// </summary>
        /// <param name="client">The dynamo client.</param>
        /// <param name="tableName">The table name to enable ttl on.</param>
        /// <param name="attributeName">The column to use for TTL, this must contain epoch values.</param>
        protected virtual void SetTimeToLive(IAmazonDynamoDB client, string tableName, string attributeName)
        {
            client.UpdateTimeToLive(new UpdateTimeToLiveRequest
            {
                TableName = tableName,
                TimeToLiveSpecification = new TimeToLiveSpecification
                {
                    Enabled = true,
                    AttributeName = attributeName
                }
            });
        }
    }
}