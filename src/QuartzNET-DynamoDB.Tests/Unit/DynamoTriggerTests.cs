using System;
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
        public void InitialisedState()
        {
            var sut = new DynamoTrigger();

            Assert.Equal("Waiting", sut.State);
        }

        /// <summary>
        /// Tests that when state is string.Empty, TriggerState is None.
        /// </summary>
        [Fact]
        public void TriggerStateNone()
        {
            var sut = new DynamoTrigger();
            sut.State = string.Empty;

            Assert.Equal(TriggerState.None, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is Complete, TriggerState is Complete.
        /// </summary>
        [Fact]
        public void TriggerStateComplete()
        {
            var sut = new DynamoTrigger();
            sut.State = "Complete";

            Assert.Equal(TriggerState.Complete, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is Paused, TriggerState is Paused.
        /// </summary>
        [Fact]
        public void TriggerStatePaused()
        {
            var sut = new DynamoTrigger();
            sut.State = "Paused";

            Assert.Equal(TriggerState.Paused, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is PausedAndBlocked, TriggerState is Paused.
        /// This is because PausedAndBlocked is an internal state that we do not want to expose
        /// to externally.
        /// </summary>
        [Fact]
        public void TriggerStatePausedAndBlocked()
        {
            var sut = new DynamoTrigger();
            sut.State = "PausedAndBlocked";

            Assert.Equal(TriggerState.Paused, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is Blocked, TriggerState is Blocked.
        /// </summary>
        [Fact]
        public void TriggerStateBlocked()
        {
            var sut = new DynamoTrigger();
            sut.State = "Blocked";

            Assert.Equal(TriggerState.Blocked, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is Error, TriggerState is Error.
        /// </summary>
        [Fact]
        public void TriggerStateError()
        {
            var sut = new DynamoTrigger();
            sut.State = "Error";

            Assert.Equal(TriggerState.Error, sut.TriggerState);
        }

        /// <summary>
        /// Tests that when state is anything else, TriggerState is Normal.
        /// </summary>
        [Fact]
        public void TriggerStateNormal()
        {
            var sut = new DynamoTrigger();
            sut.State = new Random().Next(0, 999999).ToString();

            Assert.Equal(TriggerState.Normal, sut.TriggerState);
        }
    }
}
