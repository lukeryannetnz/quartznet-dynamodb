using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel
{
    public enum DynamoTriggerGroupState
    {
        /// <summary>
        /// Indicates that the Trigger Group is Active.
        /// </summary>
        Active = 0,

        /// <summary>
        /// Indicates that the Trigger Group is Paused.
        /// This means that all triggers in this Group should also be Paused, including
        /// any new triggers that are added to it.
        /// </summary>
        Paused = 1,
    }
    
}