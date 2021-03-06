﻿using System;
using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
	/// <summary>
	/// Contains tests for the DynamoTrigger class.
	/// </summary>
	public class DynamoTriggerTests
	{
		/// <summary>
		/// Tests that a new DynamoTrigger object has its state set correctly.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void InitialisedState()
		{
			var sut = new DynamoTrigger();

			Assert.Equal(DynamoTriggerState.Waiting, sut.State);
		}

		/// <summary>
		/// Tests that when state is string.Empty, TriggerState is None.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateNone()
		{
			var sut = new DynamoTrigger();
			sut.State = null;

			Assert.Equal(TriggerState.None, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is Complete, TriggerState is Complete.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateComplete()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.Complete;

			Assert.Equal(TriggerState.Complete, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is Paused, TriggerState is Paused.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStatePaused()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.Paused;

			Assert.Equal(TriggerState.Paused, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is PausedAndBlocked, TriggerState is Paused.
		/// This is because PausedAndBlocked is an internal state that we do not want to expose
		/// to externally.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStatePausedAndBlocked()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.PausedAndBlocked;

			Assert.Equal(TriggerState.Paused, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is Blocked, TriggerState is Blocked.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateBlocked()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.Blocked;

			Assert.Equal(TriggerState.Blocked, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is Error, TriggerState is Error.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateError()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.Error;

			Assert.Equal(TriggerState.Error, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is waiting, TriggerState is Normal.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateWaiting()
		{
			var sut = new DynamoTrigger();
			sut.State = DynamoTriggerState.Waiting;

			Assert.Equal(TriggerState.Normal, sut.TriggerState);
		}

		/// <summary>
		/// Tests that when state is anything else, TriggerState is Normal.
		/// </summary>
		[Fact]
		[Trait("Category", "Unit")]
		public void TriggerStateNormal()
		{
			var sut = new DynamoTrigger();
			sut.State = new DynamoTriggerState(new Random().Next(0, 999999));

			Assert.Equal(TriggerState.Normal, sut.TriggerState);
		}
	}
}
