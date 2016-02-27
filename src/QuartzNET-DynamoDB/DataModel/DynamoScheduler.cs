using System;
using Amazon.DynamoDBv2.DataModel;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An instance of a quartz job scheduler.
    /// </summary>
    [DynamoDBTable("Scheduler")]
    public class DynamoScheduler
    {
        [DynamoDBHashKey]
        public string InstanceId { get; set; }

        public DateTime Expires { get; set; }

        public string State { get; set; }
    }
}
