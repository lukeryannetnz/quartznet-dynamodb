using System;
using Xunit;
using Quartz.Simpl;
using Quartz.Spi;
using System.Linq;
using Quartz.Impl.Triggers;
using Quartz.DynamoDB.DataModel.Storage;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl;
using Quartz.Job;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests related to the Pausing of Triggers and Trigger Groups.
	/// </summary>
	public class JobStoreTriggerPauseTests
	{
		IJobStore _sut;
		Repository<DynamoTrigger> _triggerRepository;

		public JobStoreTriggerPauseTests ()
		{
			_sut = new JobStore ();
			var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler ();
			var loadHelper = new SimpleTypeLoadHelper ();

			_sut.Initialize(loadHelper, signaler);

			var client = DynamoDbClientFactory.Create();
			_triggerRepository = new Repository<DynamoTrigger> (client);

		}

		/// <summary>
		/// Tests that when Pause triggers is called with a group matcher equalling a trigger group, one trigger group is paused.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseTriggersOneGroupEquals()
		{
			string triggerGroup = Guid.NewGuid().ToString();
			var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

			Assert.Equal(1, result.Count);
			Assert.Equal(triggerGroup, result.Single());
		}

		/// <summary>
		/// Tests that when Pause triggers is called with a group matcher starts with and no groups match, then 0 should be returned.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseTriggersStartsWithNoMatches()
		{
			string triggerGroup = Guid.NewGuid().ToString();

			var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupStartsWith(triggerGroup.Substring(0, 8)));
			Assert.Equal(0, result.Count);
		}

		/// <summary>
		/// Tests that when Pause triggers is called with a group matcher starts with and one groups matches, 
		/// then that group should be paused,
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseTriggersStartsWithOneMatch()
		{
			string triggerGroup = Guid.NewGuid().ToString();
			// Create a random job, store it.
			string jobName = Guid.NewGuid().ToString();
			JobDetailImpl detail = new JobDetailImpl (jobName, "JobGroup", typeof(NoOpJob));
			_sut.StoreJob(detail, false);

			// Create a trigger for the job, in the trigger group.
			IOperableTrigger tr = new SimpleTriggerImpl ("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
			_sut.StoreTrigger(tr, false);

			var result = _sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupStartsWith(triggerGroup.Substring(0, 8)));
			Assert.Equal(1, result.Count);
		}

		/// <summary>
		/// Tests that when Pause triggers is called for a trigger group that doesn't exist, that trigger group is stored
		/// paused so that future triggers added against it are paused. 
		/// This feels odd, but simulates the behaviour of the mongodb job store.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseTriggersNewTriggerGroup()
		{
			// Pause triggers for the (new) trigger group
			string triggerGroup = Guid.NewGuid().ToString();
			_sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

			// Create a random job, store it.
			string jobName = Guid.NewGuid().ToString();
			JobDetailImpl detail = new JobDetailImpl (jobName, "JobGroup", typeof(NoOpJob));
			_sut.StoreJob(detail, false);

			// Create a trigger for the job, in the trigger group that is paused.
			IOperableTrigger tr = new SimpleTriggerImpl ("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
			_sut.StoreTrigger(tr, false);

			var trigger = _triggerRepository.Load(tr.Key.ToDictionary());

			Assert.Equal("Paused", trigger.State);
		}

		/// <summary>
		/// Tests that when Pause triggers is called for a trigger group that exists and has a trigger in it, 
		/// the triggers in that group should be paused.
		/// </summary>
		[Fact]
		[Trait("Category", "Integration")]
		public void PauseTriggersExistingTriggerGroup()
		{
			string triggerGroup = Guid.NewGuid().ToString();

			// Create a random job, store it.
			string jobName = Guid.NewGuid().ToString();
			JobDetailImpl detail = new JobDetailImpl (jobName, "JobGroup", typeof(NoOpJob));
			_sut.StoreJob(detail, false);

			// Create a trigger for the job, in the trigger group.
			IOperableTrigger tr = new SimpleTriggerImpl ("test", triggerGroup, jobName, "JobGroup", DateTimeOffset.UtcNow, null, 1, TimeSpan.FromHours(1));
			_sut.StoreTrigger(tr, false);

			// Trigger should be waiting to be picked up.
			var trigger = _triggerRepository.Load(tr.Key.ToDictionary());
			Assert.Equal("Waiting", trigger.State);

			_sut.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher<TriggerKey>.GroupEquals(triggerGroup));

			trigger = _triggerRepository.Load(tr.Key.ToDictionary());
			Assert.Equal("Paused", trigger.State);
		}
	}
}

