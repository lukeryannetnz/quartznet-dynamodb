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
        private readonly DateTimeOffsetConverter converter = new DateTimeOffsetConverter();

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

        /// <summary>
        /// Gets the next fire time as a unix epoch value in UTC timezone.
        /// 
        /// Data model scan conditions are only supported for simple properties, 
        /// not those that require converters so for now I've buried the converter inside this property.
        /// This may be the straw that breaks the camels back and causes me to move away from the
        /// DataModel to only using the DocumentModel.
        /// </summary>
        [DynamoDBProperty]
        public string NextFireTimeUtcEpoch
        {
            get
            {
                var value = converter.ToEntry(Trigger.GetNextFireTimeUtc());

                if (value == null)
                {
                    return string.Empty;
                }
                else
                {
                    return value.AsString();
                }
            }
            set
            {
                if (value != null)
                {
                    var offset = (DateTimeOffset)converter.FromEntry(value);
                    if (offset != null)
                    {
                        Trigger.SetNextFireTimeUtc(offset);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the next fire time as a DateTimeOffset value in UTC timezone.
        /// Ignored so it isn't stored, this is just here for conversion convenience.
        /// </summary>
        [DynamoDBIgnore]
        public DateTimeOffset? NextFireTimeUtc
        {
            get
            {
                if(!string.IsNullOrWhiteSpace(NextFireTimeUtcEpoch))
                {
                    return null;
                }

                return (DateTimeOffset) converter.FromEntry(NextFireTimeUtcEpoch);
            }
        }

        /// <summary>
        /// The scheduler instance currently working on this trigger.
        /// TODO: is this correct?
        /// </summary>
        public string SchedulerInstanceId { get; set; }

        /// <summary>
        /// The current state of this trigger. Generally a value from the Quartz.TriggerState
        /// enum but occasionally an internal value including: Waiting, PausedAndBlocked.
        /// </summary>
        public string State { get; set; }

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

        // Commenting out for now as the current implementation of optimistic locking doesn't seem to give feedback when the write isn't successful!

        ///// <summary>
        ///// Property to store version number for optimistic locking.
        ///// <see cref="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/VersionSupportHLAPI.html"/>
        ///// </summary>
        //[DynamoDBVersion]
        //public int? Version { get; set; }
    }
}