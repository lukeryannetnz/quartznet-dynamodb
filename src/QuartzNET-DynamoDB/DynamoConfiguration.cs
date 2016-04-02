namespace Quartz.DynamoDB
{
    public class DynamoConfiguration
    {
        public static string JobDetailTableName => "JobDetail";

        public static string TriggerTableName => "Trigger";

        public static string SchedulerTableName => "Scheduler";
    }
}
