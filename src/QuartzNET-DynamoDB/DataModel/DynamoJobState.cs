namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// The state of a dynamo job.
    /// </summary>
    public enum DynamoJobState
    {
        /// <summary>
        /// Indicates that the Job is Active.
        /// </summary>
        Active = 0,

        /// <summary>
        /// Indicates that the Job is Blocked.
        /// A job is blocked when concurrent execution is disallowed and the job is being executed. 
        /// </summary>
        Blocked = 1,
    }
}
