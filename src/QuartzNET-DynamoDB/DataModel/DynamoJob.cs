using Amazon.DynamoDBv2.DataModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An wrapper class for a Quartz JobDetail instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    [DynamoDBTable("JobDetail")]

    public class DynamoJob
    {
        public DynamoJob()
        {
        }

        public DynamoJob(IJobDetail trigger)
        {
            Job = trigger;
        }

        [DynamoDBHashKey]
        public string Group
        {
            get { return Job.Key.Group; }
            set { }
        }

        public string Name
        {
            get { return Job.Key.Name; }
            set { }
        }

        [DynamoDBProperty(typeof(JobDetailConverter))]
        public IJobDetail Job { get; set; }
    }
}
