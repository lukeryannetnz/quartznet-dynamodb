using Quartz.Impl.Triggers;

namespace Quartz.DynamoDB.Tests.Unit
{
    public class TriggerConverterCalendarIntervalTriggerTests
    {
        public CalendarIntervalTriggerImpl CreateTrigger()
        {
            CalendarIntervalTriggerImpl trigger = (CalendarIntervalTriggerImpl) TriggerBuilder.Create()
                .WithIdentity("myTrigger", "myTriggerGroup")
                .WithSimpleSchedule(x => x
                    .WithIntervalInHours(1)
                    .WithRepeatCount(100))
                .StartAt(DateBuilder.FutureDate(10, IntervalUnit.Minute))
                .Build();

            return trigger;
        }
    }
}