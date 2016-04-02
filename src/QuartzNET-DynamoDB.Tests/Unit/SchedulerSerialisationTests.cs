using System;
using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Tests the DynamoScheduler serialisation.
	/// </summary>
	public class SchedulerSerialisationTests
	{
		[Fact] 
		[Trait("Category", "Unit")]
		public void InstanceIdSerializesCorrectly()
		{
			var scheduler = new DynamoScheduler();
			scheduler.InstanceId = Guid.NewGuid ().ToString ();

			var serialized = scheduler.ToDynamo();

			DynamoScheduler result = new DynamoScheduler ();
			result.InitialiseFromDynamoRecord (serialized);

			Assert.Equal(scheduler.InstanceId, result.InstanceId);
		}

		[Fact] 
		[Trait("Category", "Unit")]
		public void StateSerializesCorrectly()
		{
			var scheduler = new DynamoScheduler();
			scheduler.State = Guid.NewGuid ().ToString ();

			var serialized = scheduler.ToDynamo();

			DynamoScheduler result = new DynamoScheduler ();
			result.InitialiseFromDynamoRecord (serialized);

			Assert.Equal(scheduler.State, result.State);
		}

		[Fact] 
		[Trait("Category", "Unit")]
		public void ExpiresUtcEpochSerializesCorrectly()
		{
			var scheduler = new DynamoScheduler();
			scheduler.ExpiresUtcEpoch = 7809235;

			var serialized = scheduler.ToDynamo();

			DynamoScheduler result = new DynamoScheduler ();
			result.InitialiseFromDynamoRecord (serialized);

			Assert.Equal(scheduler.ExpiresUtcEpoch, result.ExpiresUtcEpoch);
		}
	}
}

