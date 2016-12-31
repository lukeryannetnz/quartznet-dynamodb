using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Spi;
using Quartz.Impl.Triggers;
using System.Linq;
using Quartz.DynamoDB.DataModel.Storage;
using System.Diagnostics;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A wrapper class for a Quartz Trigger instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    public class DynamoTrigger : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType
    {
        private readonly JobDataMapConverter jobDataMapConverter = new JobDataMapConverter();
        private readonly DateTimeOffsetConverter dateTimeOffsetConverter = new DateTimeOffsetConverter();

        public DynamoTrigger()
        {
            State = DynamoTriggerState.Waiting;
        }

        public DynamoTrigger(IOperableTrigger trigger) : this()
        {
            if (!(trigger is AbstractTrigger))
            {
                throw new ArgumentException("Trigger must be of type Quartz.Impl.Triggers.AbstractTrigger", nameof(trigger));
            }

            Trigger = (AbstractTrigger)trigger;
        }

        public DynamoTrigger(Dictionary<string, AttributeValue> item)
        {
            InitialiseFromDynamoRecord(item);
        }

        public void InitialiseFromDynamoRecord(Dictionary<string, AttributeValue> record)
        {
            string type = record.ContainsKey("Type") ? record["Type"].S : string.Empty;
            Debug.WriteLine("Initialising trigger of Type: {0}", type);

            switch (type)
            {
                case "CalendarIntervalTriggerImpl":
                    {
                        var calendarTrigger = new CalendarIntervalTriggerImpl();
                        Trigger = calendarTrigger;

                        calendarTrigger.PreserveHourOfDayAcrossDaylightSavings = record["PreserveHourOfDayAcrossDaylightSavings"].BOOL;
                        calendarTrigger.RepeatInterval = int.Parse(record["RepeatInterval"].N);
                        calendarTrigger.RepeatIntervalUnit = (IntervalUnit)int.Parse(record["RepeatIntervalUnit"].N);
                        calendarTrigger.TimesTriggered = int.Parse(record["TimesTriggered"].N);
                        calendarTrigger.TimeZone = TimeZoneInfo.FromSerializedString(record["TimeZone"].S);
                        break;
                    }
                case "CronTriggerImpl":
                    {
                        var cronTrigger = new CronTriggerImpl();
                        Trigger = cronTrigger;

                        cronTrigger.CronExpressionString = record["CronExpressionString"].S;
                        cronTrigger.TimeZone = TimeZoneInfo.FromSerializedString(record["TimeZone"].S);
                        break;
                    }

                case "DailyTimeIntervalTriggerImpl":
                    {
                        var dailyTrigger = new DailyTimeIntervalTriggerImpl();
                        Trigger = dailyTrigger;

                        var daysOfWeek = record["DaysOfWeek"].L
                            .Select(dow => (DayOfWeek)Enum.Parse(typeof(DayOfWeek), dow.S));

                        dailyTrigger.DaysOfWeek = new Quartz.Collection.HashSet<DayOfWeek>(daysOfWeek);

                        int endTimeOfDayHour = int.Parse(record["EndTimeOfDay_Hour"].N);
                        int endTimeOfDayMin = int.Parse(record["EndTimeOfDay_Minute"].N);
                        int endTimeOfDaySec = int.Parse(record["EndTimeOfDay_Second"].N);
                        dailyTrigger.EndTimeOfDay = new TimeOfDay(endTimeOfDayHour, endTimeOfDayMin, endTimeOfDaySec);
                        dailyTrigger.RepeatCount = int.Parse(record["RepeatCount"].N);
                        dailyTrigger.RepeatInterval = int.Parse(record["RepeatInterval"].N);
                        dailyTrigger.RepeatIntervalUnit = (IntervalUnit)int.Parse(record["RepeatIntervalUnit"].N);
                        int startTimeOfDayHour = int.Parse(record["StartTimeOfDay_Hour"].N);
                        int startTimeOfDayMin = int.Parse(record["StartTimeOfDay_Minute"].N);
                        int startTimeOfDaySec = int.Parse(record["StartTimeOfDay_Second"].N);
                        dailyTrigger.StartTimeOfDay = new TimeOfDay(startTimeOfDayHour, startTimeOfDayMin, startTimeOfDaySec);
                        dailyTrigger.TimesTriggered = int.Parse(record["TimesTriggered"].N);
                        dailyTrigger.TimeZone = TimeZoneInfo.FromSerializedString(record["TimeZone"].S);
                        break;
                    }

                case "SimpleTriggerImpl":
                    {
                        var simpleTrigger = new SimpleTriggerImpl();
                        Trigger = simpleTrigger;

                        simpleTrigger.RepeatCount = int.Parse(record["RepeatCount"].N);
                        simpleTrigger.RepeatInterval = new TimeSpan(long.Parse(record["RepeatInterval"].N));
                        simpleTrigger.TimesTriggered = int.Parse(record["TimesTriggered"].N);
                        break;
                    }
                default:
                    {
                        Trigger = new SimpleTriggerImpl();
                        break;
                    }
            }

            State = new DynamoTriggerState(int.Parse(record["State"].N));
            SchedulerInstanceId = record["SchedulerInstanceId"].S;
            Trigger.Name = record["Name"].S;
            Trigger.Group = record["Group"].S;
            Trigger.JobName = record["JobName"].S;
            Trigger.JobGroup = record["JobGroup"].S;
            Trigger.Description = record["Description"].S;
            Trigger.CalendarName = record["CalendarName"].S;
            Trigger.JobDataMap = (JobDataMap)jobDataMapConverter.FromEntry(record["JobDataMap"]);
            Trigger.MisfireInstruction = int.Parse(record["MisfireInstruction"].N);
            Trigger.FireInstanceId = record["FireInstanceId"].S;

            Trigger.StartTimeUtc = DateTimeOffset.Parse(record["StartTimeUtc"].S);

            if (record.ContainsKey("EndTimeUtc"))
            {
                Trigger.EndTimeUtc = DateTimeOffset.Parse(record["EndTimeUtc"].S);
            }

            if (record.ContainsKey("NextFireTimeUtcEpoch"))
            {
                Trigger.SetNextFireTimeUtc(dateTimeOffsetConverter.FromEntry(int.Parse(record["NextFireTimeUtcEpoch"].N)));
                Debug.WriteLine("Setting Trigger NextFireTimeUTC {0}", Trigger.GetNextFireTimeUtc().Value);
            }

            if (record.ContainsKey("PreviousFireTimeUtcEpoch"))
            {
                Trigger.SetPreviousFireTimeUtc(dateTimeOffsetConverter.FromEntry(int.Parse(record["PreviousFireTimeUtcEpoch"].N)));
            }

            Trigger.Priority = int.Parse(record["Priority"].N);
        }

        public string DynamoTableName
        {
            get
            {
                return DynamoConfiguration.TriggerTableName;
            }
        }

        public Dictionary<string, AttributeValue> Key
        {
            get
            {
                return Trigger.Key.ToDictionary();
            }
        }

        public AbstractTrigger Trigger { get; set; }

        /// <summary>
        /// The scheduler instance currently working on this trigger.
        /// TODO: is this correct?
        /// </summary>
        public string SchedulerInstanceId { get; set; }

        /// <summary>
        /// The current state of this trigger. Generally a value from the Quartz.TriggerState
        /// enum but occasionally an internal value including: Waiting, PausedAndBlocked.
        /// </summary>
        public DynamoTriggerState State { get; set; }

        /// <summary>
        /// Returns the State property as the TriggerState enumeration required by the JobStore contract.
        /// </summary>
        public TriggerState TriggerState
        {
            get
            {
                if (State == null)
                {
                    return TriggerState.None;
                }

                return State.TriggerState;
            }
        }

        public Dictionary<string, AttributeValue> ToDynamo()
        {
            Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

            record.Add("State", new AttributeValue() { N = State.InternalValue.ToString() });
            record.Add("SchedulerInstanceId", AttributeValueHelper.StringOrNull(SchedulerInstanceId));

            record.Add("Name", AttributeValueHelper.StringOrNull(Trigger.Name));
            record.Add("Group", AttributeValueHelper.StringOrNull(Trigger.Group));
            record.Add("JobName", AttributeValueHelper.StringOrNull(Trigger.JobName));
            record.Add("JobGroup", AttributeValueHelper.StringOrNull(Trigger.JobGroup));
            record.Add("Description", AttributeValueHelper.StringOrNull(Trigger.Description));
            record.Add("CalendarName", AttributeValueHelper.StringOrNull(Trigger.CalendarName));

            record.Add("JobDataMap", jobDataMapConverter.ToEntry(Trigger.JobDataMap));
            record.Add("MisfireInstruction", new AttributeValue() { N = Trigger.MisfireInstruction.ToString() });
            record.Add("FireInstanceId", AttributeValueHelper.StringOrNull(Trigger.FireInstanceId));
            record.Add("StartTimeUtc", AttributeValueHelper.StringOrNull(Trigger.StartTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));

            if (Trigger.EndTimeUtc.HasValue)
            {
                record.Add("EndTimeUtc", AttributeValueHelper.StringOrNull(Trigger.EndTimeUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
            }

            if (Trigger.GetNextFireTimeUtc().HasValue)
            {
                record.Add("NextFireTimeUtcEpoch", new AttributeValue() { N = dateTimeOffsetConverter.ToEntry(Trigger.GetNextFireTimeUtc().Value).ToString() });
                Debug.WriteLine("Storing Trigger NextFireTimeUTC {0}", Trigger.GetNextFireTimeUtc().Value);
            }

            record.Add("Priority", new AttributeValue() { N = Trigger.Priority.ToString() });

            if (Trigger is CalendarIntervalTriggerImpl)
            {
                CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl)Trigger;
                record.Add("PreserveHourOfDayAcrossDaylightSavings", new AttributeValue { BOOL = t.PreserveHourOfDayAcrossDaylightSavings });
                record.Add("RepeatInterval", new AttributeValue() { N = t.RepeatInterval.ToString() });
                record.Add("RepeatIntervalUnit", new AttributeValue() { N = ((int)t.RepeatIntervalUnit).ToString() });
                record.Add("TimesTriggered", new AttributeValue() { N = t.TimesTriggered.ToString() });
                record.Add("TimeZone", new AttributeValue() { S = t.TimeZone.ToSerializedString() });
                record.Add("Type", new AttributeValue() { S = "CalendarIntervalTriggerImpl" });
            }

            else if (Trigger is CronTriggerImpl)
            {
                CronTriggerImpl t = (CronTriggerImpl)Trigger;
                record.Add("CronExpressionString", new AttributeValue() { S = t.CronExpressionString });
                record.Add("TimeZone", new AttributeValue() { S = t.TimeZone.ToSerializedString() });
                record.Add("Type", new AttributeValue() { S = "CronTriggerImpl" });
            }

            else if (Trigger is DailyTimeIntervalTriggerImpl)
            {
                DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl)Trigger;
                if (Trigger.GetPreviousFireTimeUtc().HasValue)
                {
                    record.Add("PreviousFireTimeUtcEpoch", new AttributeValue() { N = dateTimeOffsetConverter.ToEntry(Trigger.GetPreviousFireTimeUtc().Value).ToString() });
                }
                record.Add("DaysOfWeek", new AttributeValue() { L = t.DaysOfWeek.Select(dow => new AttributeValue(dow.ToString())).ToList() });
                record.Add("EndTimeOfDay_Hour", new AttributeValue() { N = t.EndTimeOfDay.Hour.ToString() });
                record.Add("EndTimeOfDay_Minute", new AttributeValue() { N = t.EndTimeOfDay.Minute.ToString() });
                record.Add("EndTimeOfDay_Second", new AttributeValue() { N = t.EndTimeOfDay.Second.ToString() });
                record.Add("RepeatCount", new AttributeValue() { N = t.RepeatCount.ToString() });
                record.Add("RepeatInterval", new AttributeValue() { N = t.RepeatInterval.ToString() });
                record.Add("RepeatIntervalUnit", new AttributeValue() { N = ((int)t.RepeatIntervalUnit).ToString() });
                record.Add("StartTimeOfDay_Hour", new AttributeValue() { N = t.StartTimeOfDay.Hour.ToString() });
                record.Add("StartTimeOfDay_Minute", new AttributeValue() { N = t.StartTimeOfDay.Minute.ToString() });
                record.Add("StartTimeOfDay_Second", new AttributeValue() { N = t.StartTimeOfDay.Second.ToString() });
                record.Add("TimesTriggered", new AttributeValue() { N = t.TimesTriggered.ToString() });
                record.Add("TimeZone", new AttributeValue() { S = t.TimeZone.ToSerializedString() });

                record.Add("Type", new AttributeValue() { S = "DailyTimeIntervalTriggerImpl" });
            }

            else if (Trigger is SimpleTriggerImpl)
            {
                SimpleTriggerImpl t = (SimpleTriggerImpl)Trigger;
                record.Add("RepeatCount", new AttributeValue() { N = t.RepeatCount.ToString() });
                record.Add("RepeatInterval", new AttributeValue() { N = t.RepeatInterval.Ticks.ToString() });
                record.Add("TimesTriggered", new AttributeValue() { N = t.TimesTriggered.ToString() });
                if (Trigger.GetPreviousFireTimeUtc().HasValue)
                {
                    record.Add("PreviousFireTimeUtcEpoch", new AttributeValue() { N = dateTimeOffsetConverter.ToEntry(Trigger.GetPreviousFireTimeUtc().Value).ToString() });
                }
                record.Add("Type", new AttributeValue() { S = "SimpleTriggerImpl" });
            }

            return record;
        }
    }

    /// <summary>
    /// Represents the state of the dynamo quartz trigger. Similar to the Quartz.TriggerState enumeration, 
    /// but with additional values e.g. PausedAndBlocked.
    /// </summary>
    public class DynamoTriggerState
    {
        private readonly int internalValue;

        public static readonly DynamoTriggerState None = new DynamoTriggerState(0);

        public static readonly DynamoTriggerState Normal = new DynamoTriggerState(1);

        public static readonly DynamoTriggerState Paused = new DynamoTriggerState(2);

        public static readonly DynamoTriggerState PausedAndBlocked = new DynamoTriggerState(3);

        public static readonly DynamoTriggerState Complete = new DynamoTriggerState(4);

        public static readonly DynamoTriggerState Error = new DynamoTriggerState(5);

        public static readonly DynamoTriggerState Blocked = new DynamoTriggerState(6);

        public static readonly DynamoTriggerState Waiting = new DynamoTriggerState(7);

        public static readonly DynamoTriggerState Acquired = new DynamoTriggerState(8);

        public static readonly DynamoTriggerState Executing = new DynamoTriggerState(9);

        public int InternalValue
        {
            get { return internalValue;}
        }

        public DynamoTriggerState(int value)
        {
            internalValue = value;
        }

        /// <summary>
        /// Returns the State property as the Quartz.TriggerState enumeration required by the JobStore contract.
        /// </summary>
        public TriggerState TriggerState
        {
            get
            {
                switch (InternalValue)
                {
                    case 0:
                        {
                            return TriggerState.None;
                        }
                    case 1:
                        {
                            return TriggerState.Normal;
                        }
                    case 2:
                        {
                            return TriggerState.Paused;
                        }
                    case 3: //PausedAndBlocked
                        {
                            return TriggerState.Paused;
                        }
                    case 4:
                        {
                            return TriggerState.Complete;
                        }
                    case 5:
                        {
                            return TriggerState.Error;
                        }
                    case 6:
                        {
                            return TriggerState.Blocked;
                        }
                    default:
                        {
                            return TriggerState.Normal;
                        }
                }
            }
        }

        /// <summary>
        /// Determines whether the specified <see cref="object"/> is equal to the current <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
        /// Compares value of the internal integer.
        /// </summary>
        /// <param name="obj">The <see cref="object"/> to compare with the current <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.</param>
        /// <returns><c>true</c> if the specified <see cref="object"/> is equal to the current
        /// <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>; otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            DynamoTriggerState input = obj as DynamoTriggerState;

            if (object.ReferenceEquals(input, null))
            {
                return false;
            }

            return this.InternalValue.Equals(input.InternalValue);
        }

        /// <summary>
        /// Determines whether a specified instance of <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> is
        /// equal to another specified <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
        /// Compares value of the internal integer.
        /// </summary>
        /// <param name="left">The first <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
        /// <param name="right">The second <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
        /// <returns><c>true</c> if <c>left</c> and <c>right</c> are equal; otherwise, <c>false</c>.</returns>
        public static bool operator ==(DynamoTriggerState left, DynamoTriggerState right)
        {
            if (object.ReferenceEquals(left, null) || object.ReferenceEquals(right, null))
            {
                return false;
            }

            return left.Equals(right);
        }

        /// <summary>
        /// Determines whether a specified instance of <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> is not
        /// equal to another specified <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
        /// Compares value of the internal integer.
        /// </summary>
        /// <param name="left">The first <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
        /// <param name="right">The second <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
        /// <returns><c>true</c> if <c>left</c> and <c>right</c> are not equal; otherwise, <c>false</c>.</returns>
        public static bool operator !=(DynamoTriggerState left, DynamoTriggerState right)
        {
            if (object.ReferenceEquals(left, null) || object.ReferenceEquals(right, null))
            {
                return true;
            }

            return !left.Equals(right);
        }

        public override int GetHashCode()
        {
            return InternalValue.GetHashCode();
        }
    }
}