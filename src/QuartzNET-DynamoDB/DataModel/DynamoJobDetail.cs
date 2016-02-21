using System;
using Amazon.DynamoDBv2.DataModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An IJobDetail instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    [DynamoDBTable("JobDetail")]

    public class DynamoJobDetail : IJobDetail
    {
        public DynamoJobDetail()
        {
            Key = new JobKey(string.Empty);
        }

        public JobBuilder GetJobBuilder()
        {
            throw new NotImplementedException();
        }

        [DynamoDBHashKey]
        public string Group
        {
            get { return Key.Group; }
            set { Key = new JobKey(Name, value); }
        }

        [DynamoDBProperty("Name")]
        public string Name
        {
            get { return Key.Name; }
            set { Key = new JobKey(value, Group); }
        }

        [DynamoDBIgnore]
        public JobKey Key { get; set; }

        [DynamoDBProperty]
        public string Description { get; set; }

        [DynamoDBProperty(typeof(JobTypeConverter))]
        public Type JobType { get; set; }

        [DynamoDBProperty(typeof(JobDataMapConverter))]
        public JobDataMap JobDataMap { get; set; }

        [DynamoDBProperty]
        public bool Durable { get; set; }

        [DynamoDBProperty]
        public bool PersistJobDataAfterExecution { get; set; }

        [DynamoDBProperty]
        public bool ConcurrentExecutionDisallowed { get; set; }

        [DynamoDBProperty]
        public bool RequestsRecovery { get; set; }

        public object Clone()
        {
            return Clone(this);
        }

        public static DynamoJobDetail Clone(IJobDetail job)
        {
            return new DynamoJobDetail()
            {
                Key = job.Key,
                Description = job.Description,
                JobType = job.JobType,
                JobDataMap = job.JobDataMap,
                Durable = job.Durable,
                PersistJobDataAfterExecution = job.PersistJobDataAfterExecution,
                ConcurrentExecutionDisallowed = job.ConcurrentExecutionDisallowed,
                RequestsRecovery = job.RequestsRecovery
            };
        }
    }
}
