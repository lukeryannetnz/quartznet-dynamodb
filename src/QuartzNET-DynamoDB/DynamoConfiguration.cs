namespace Quartz.DynamoDB
{
    internal class DynamoConfiguration
    {
        internal static string JobDetailTableName => "JobDetail";

        internal static string TriggerTableName => "Trigger";

        public static string SchedulerTableName => "Scheduler";
    }
}
