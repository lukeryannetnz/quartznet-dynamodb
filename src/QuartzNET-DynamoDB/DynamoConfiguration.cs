using System.Collections.Generic;
using System.Configuration;

namespace Quartz.DynamoDB
{
    public class DynamoConfiguration
    {
        public static string InstanceName { get; set; }

        public static string JobDetailTableName => TableNamePrefix + "Job";

        public static string JobGroupTableName => TableNamePrefix + "JobGroup";

        public static string TriggerTableName => TableNamePrefix + "Trigger";

        public static string TriggerGroupTableName => TableNamePrefix + "TriggerGroup";

        public static string SchedulerTableName => TableNamePrefix + "Scheduler";

        public static string CalendarTableName => TableNamePrefix + "Calendar";

        public static string ServiceUrl => ConfigurationManager.AppSettings["DynamoServiceURL"] ?? string.Empty;

        private static string TableNamePrefix
        {
            get
            {
                if (string.IsNullOrWhiteSpace(InstanceName))
                {
                    return string.Empty;
                }

                return string.Format("{0}.", InstanceName);
            }
        }

        public static IEnumerable<string> AllTableNames
        {
            get
            {
                return new[]
                {
                    JobDetailTableName, 
                    JobGroupTableName, 
                    TriggerTableName, 
                    TriggerGroupTableName, 
                    CalendarTableName, 
                    SchedulerTableName };
            }
        }
    }
}