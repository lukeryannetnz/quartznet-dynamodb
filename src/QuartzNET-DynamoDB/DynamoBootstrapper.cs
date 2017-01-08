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
                Console.WriteLine(string.Format("Table {0} doesn't exist.", tableName));
                return true;
            }

            try
            {
                var table = client.DescribeTable(tableName);

                Console.WriteLine(string.Format("Table {0} status {1}", tableName, table.Table.TableStatus));

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

            throw new System.Exception(string.Format("Not sure if we should create table: {0}. Unknown table status, panic!", tableName));
        }

        private static void EnsureTableDeleted(IAmazonDynamoDB client, string tableName)
        {
            for (int i = 0; i < 120; i++)
            {
                if (TableDeleted(client, tableName))
                {
                    return;
                }

                Console.WriteLine(string.Format("Waiting for Table {0} to delete.", tableName));

                Thread.Sleep(DynamoConfiguration.BootstrapRetryDelayMilliseconds);
            }

            throw new System.Exception(string.Format("Table {0} not created within a reasonable time. Panic!", tableName));
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

                Console.WriteLine(string.Format("Waiting for Table {0} to become active.", tableName));

                Thread.Sleep(DynamoConfiguration.BootstrapRetryDelayMilliseconds);
            }

            throw new System.Exception(string.Format("Table {0} not created within a reasonable time. Panic!", tableName));
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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

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

            Console.WriteLine(string.Format("Creating table {0}.", createRequest.TableName));

            // Provisioned-throughput settings are required even though
            // the local test version of DynamoDB ignores them
            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            // Using the DynamoDB client, make a synchronous CreateTable request
            client.CreateTable(createRequest);

            EnsureTableActive(client, createRequest.TableName);
        }
    }
}