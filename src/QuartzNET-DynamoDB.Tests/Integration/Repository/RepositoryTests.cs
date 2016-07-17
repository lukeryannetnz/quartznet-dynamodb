using System;
using Xunit;
using Quartz.DynamoDB.DataModel.Storage;
using Quartz.DynamoDB.DataModel;
using System.Linq;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests for the repository class.
	/// </summary>
	public class RepositoryTests
	{
		[Fact]
		[Trait("Category", "Integration")]
		public void PersistTwoSchedulersSameId_OneRecord()
		{
			var client = DynamoDbClientFactory.Create();
			new DynamoBootstrapper().BootStrap(client);

			var sut = new Repository<DynamoScheduler> (client);

			int initialSchedulerCount = sut.Scan (null, null, null).Count();

			var scheduler = new DynamoScheduler
			{
				InstanceId = "testInstance" + DateTime.UtcNow.Ticks.ToString(),
				ExpiresUtc = (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime,
				State = "Running"
			};

			sut.Store(scheduler);

			var expressionAttributeValues = new Dictionary<string,AttributeValue> 
			{
				{":instance", new AttributeValue { S = scheduler.InstanceId }}
			};

			var scheduler2 = sut.Scan (expressionAttributeValues, null, "InstanceId = :instance").Single();

			scheduler2.ExpiresUtc = (SystemTime.Now () + new TimeSpan (0, 20, 0)).UtcDateTime;

			sut.Store(scheduler2);

			int finalCount = sut.Scan (null, null, null).Count();

			Assert.Equal (initialSchedulerCount + 1, finalCount);
		}
	}
}

