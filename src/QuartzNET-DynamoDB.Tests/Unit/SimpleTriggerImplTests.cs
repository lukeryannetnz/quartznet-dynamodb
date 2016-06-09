using System;
using Quartz.Impl.Triggers;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
	/// <summary>
	/// Contains tests for the SimpleTriggerImpl Quartz.NET trigger. I wrote these to help me understand how this class works.
	/// </summary>
	public class SimpleTriggerImplTests
	{
		SimpleTriggerImpl _sut;

		public SimpleTriggerImplTests ()
		{
			_sut = (SimpleTriggerImpl)TriggerBuilder.Create()
				.WithIdentity("FiveSecondTrigger", "SimpleTriggersGroup")
				.StartNow()
				.WithSimpleSchedule(x => x
					.WithIntervalInSeconds(5)
					.RepeatForever())
				.Build();	
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void TriggeredIncrementsNextFiredTime()
		{
			_sut.Triggered(null);
			var nextFireTime = _sut.GetNextFireTimeUtc();
			_sut.Triggered(null);
			var triggeredNextFireTime = _sut.GetNextFireTimeUtc();

			Console.WriteLine("NextFireTime: {0}", nextFireTime);
			Console.WriteLine("TriggeredNextFireTime: {0}", triggeredNextFireTime);

			Assert.NotEqual(nextFireTime, triggeredNextFireTime);
			Assert.Equal(nextFireTime.Value.AddSeconds(5), triggeredNextFireTime);
		}

		[Fact]
		[Trait("Category", "Unit")]
		public void CallTwiceSameInputSameOutput()
		{
			var nextFireTime = _sut.GetNextFireTimeUtc();
			var firstCall = _sut.GetFireTimeAfter(nextFireTime);
			var secondCall = _sut.GetFireTimeAfter(nextFireTime);
			var thirdCall = _sut.GetFireTimeAfter(nextFireTime);

			Console.WriteLine("firstCall: {0}", firstCall);
			Console.WriteLine("secondCall: {0}", secondCall);
			Console.WriteLine("thirdCall: {0}", thirdCall);

			Assert.Equal(firstCall, secondCall);
			Assert.Equal(secondCall, thirdCall);
		}
	}
}

