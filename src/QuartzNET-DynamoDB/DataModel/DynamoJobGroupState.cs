using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel
{
    public enum DynamoJobGroupState
    {
        /// <summary>
        /// Indicates that the Job Group is Active.
        /// </summary>
        Active = 0,

        /// <summary>
        /// Indicates that the Job Group is Paused.
        /// This means that all jobs in this Group should also be Paused, including
        /// any new jobs that are added to it.
        /// </summary>
        Paused = 1,
    }
    
}