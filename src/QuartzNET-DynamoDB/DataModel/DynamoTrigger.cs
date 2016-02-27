using System;
using System.Diagnostics;
using System.Runtime.Remoting.Channels;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.Spi;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// An wrapper class for a Quartz Trigger instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    [DynamoDBTable("Trigger")]
    public class DynamoTrigger
    {
        public DynamoTrigger()
        {
            State = "Waiting";
        }

        public DynamoTrigger(IOperableTrigger trigger) : this()
        {
            Trigger = trigger;
        }

        [DynamoDBHashKey]
        public string Group
        {
            get { return Trigger.Key.Group; }
            set { }
        }

        public string Name
        {
            get { return Trigger.Key.Name; }
            set { }
        }

        [DynamoDBProperty(typeof(TriggerConverter))]
        public IOperableTrigger Trigger { get; set; }

        public string State { get; set; }

        [DynamoDBProperty(typeof(DateTimeOffsetConverter))]
        public DateTimeOffset? NextFireTimeUtc
        {
            get { return Trigger.GetNextFireTimeUtc(); }
            set
            { }
        }

        /// <summary>
        /// The scheduler instance currently working on this trigger.
        /// TODO: is this correct?
        /// </summary>
        public string SchedulerInstanceId { get; set; }

        /// <summary>
        /// Returns the State property as the TriggerState enumeration required by the JobStore contract.
        /// </summary>
        [DynamoDBIgnore]
        public TriggerState TriggerState
        {
            get
            {
                switch (State)
                {
                    case "":
                        {
                            return TriggerState.None;
                        }
                    case "Complete":
                        {
                            return TriggerState.Complete;
                        }
                    case "Paused":
                        {
                            return TriggerState.Paused;
                        }
                    case "PausedAndBlocked":
                        {
                            return TriggerState.Paused;
                        }
                    case "Blocked":
                        {
                            return TriggerState.Blocked;
                        }
                    case "Error":
                        {
                            return TriggerState.Error;
                        }
                    default:
                        {
                            return TriggerState.Normal;
                        }
                }
            }
        }
    }
}