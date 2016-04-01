using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Remoting.Channels;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Quartz.Spi;
using Quartz.Impl.Triggers;
using System.Linq;

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
            if (!(trigger is AbstractTrigger))
            {
                throw new ArgumentException("Trigger must be of type Quartz.Impl.Triggers.AbstractTrigger", nameof(trigger));
            }
            Trigger = (AbstractTrigger)trigger;
        }

        public DynamoTrigger(Dictionary<string, AttributeValue> item)
        {
            string type = item["Type"].S;
            switch (type)
            {
                case "CalendarIntervalTriggerImpl":
                    {
                        var calendarTrigger = new CalendarIntervalTriggerImpl();
                        Trigger = calendarTrigger;

                        calendarTrigger.PreserveHourOfDayAcrossDaylightSavings = item["PreserveHourOfDayAcrossDaylightSavings"].BOOL;
                        calendarTrigger.RepeatInterval = int.Parse(item["RepeatInterval"].N);
                        calendarTrigger.RepeatIntervalUnit = (IntervalUnit)int.Parse(item["RepeatIntervalUnit"].N);
                        calendarTrigger.TimesTriggered = int.Parse(item["TimesTriggered"].N);
                        calendarTrigger.TimeZone = TimeZoneInfo.FromSerializedString(item["TimeZone"].S);
                        break;
                    }
                case "CronTriggerImpl":
                    {
                        Trigger = new CronTriggerImpl();
                        //todo: support CronTrigger
                        break;
                    }

                case "DailyTimeIntervalTriggerImpl":
                    {
                        var dailyTrigger = new DailyTimeIntervalTriggerImpl();
                        Trigger = dailyTrigger;

                        var daysOfWeek = item["DaysOfWeek"].L
                                .Select(dow => (DayOfWeek)Enum.Parse(typeof(DayOfWeek), dow.S));

                        dailyTrigger.DaysOfWeek = new Quartz.Collection.HashSet<DayOfWeek>(daysOfWeek);

                        int endTimeOfDayHour = int.Parse(item["EndTimeOfDay_Hour"].N);
                        int endTimeOfDayMin = int.Parse(item["EndTimeOfDay_Minute"].N);
                        int endTimeOfDaySec = int.Parse(item["EndTimeOfDay_Second"].N);
                        dailyTrigger.EndTimeOfDay = new TimeOfDay(endTimeOfDayHour, endTimeOfDayMin, endTimeOfDaySec);
                        dailyTrigger.RepeatCount = int.Parse(item["RepeatCount"].N);
                        dailyTrigger.RepeatInterval = int.Parse(item["RepeatInterval"].N);
                        dailyTrigger.RepeatIntervalUnit = (IntervalUnit)int.Parse(item["RepeatIntervalUnit"].N);
                        int startTimeOfDayHour = int.Parse(item["StartTimeOfDay_Hour"].N);
                        int startTimeOfDayMin = int.Parse(item["StartTimeOfDay_Minute"].N);
                        int startTimeOfDaySec = int.Parse(item["StartTimeOfDay_Second"].N);
                        dailyTrigger.StartTimeOfDay = new TimeOfDay(startTimeOfDayHour, startTimeOfDayMin, startTimeOfDaySec);
                        dailyTrigger.TimesTriggered = int.Parse(item["TimesTriggered"].N);
                        dailyTrigger.TimeZone = TimeZoneInfo.FromSerializedString(item["TimeZone"].S);
                        break;
                    }

                case "SimpleTriggerImpl":
                    {
                        var simpleTrigger = new SimpleTriggerImpl();
                        Trigger = simpleTrigger;

                        simpleTrigger.RepeatCount = int.Parse(item["RepeatCount"].N);
                        simpleTrigger.RepeatInterval = new TimeSpan(int.Parse(item["RepeatInterval"].N));
                        simpleTrigger.TimesTriggered = int.Parse(item["TimesTriggered"].N);
                        break;
                    }
                default:
                    {
                        Trigger = new SimpleTriggerImpl();
                        break;
                    }
            }
            Trigger.Name = item["Name"].S;
            Trigger.Group = item["Group"].S;
            Trigger.JobName = item["JobName"].S;
            Trigger.JobGroup = item["JobGroup"].S;
            Trigger.Description = item["Description"].S;
            Trigger.CalendarName = item["CalendarName"].S;
            //Trigger.JobDataMap = (JobDataMap)_jobDataMapConverter.FromEntry(item["JobDataMap"]);
            Trigger.MisfireInstruction = int.Parse(item["MisfireInstruction"].N);
            Trigger.FireInstanceId = item["FireInstanceId"].S;

            Trigger.StartTimeUtc = DateTimeOffset.Parse(item["StartTimeUtc"].S);

            if(item.ContainsKey("EndTimeUtc"))
            {
                Trigger.EndTimeUtc = DateTimeOffset.Parse(item["EndTimeUtc"].S);
            }
            Trigger.Priority = int.Parse(item["Priority"].N);
        }

        public AbstractTrigger Trigger { get; set; }

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
                if (!string.IsNullOrWhiteSpace(NextFireTimeUtcEpoch))
                {
                    return null;
                }

                return (DateTimeOffset)converter.FromEntry(NextFireTimeUtcEpoch);
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

        internal Dictionary<string, AttributeValue> ToDynamo()
        {
            Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

            record.Add("Name", AttributeValueHelper.StringOrNull(Trigger.Name));
            record.Add("Group", AttributeValueHelper.StringOrNull(Trigger.Group));
            record.Add("JobName", AttributeValueHelper.StringOrNull(Trigger.JobName));
            record.Add("JobGroup", AttributeValueHelper.StringOrNull(Trigger.JobGroup));
            record.Add("Name", AttributeValueHelper.StringOrNull(Trigger.Name));
            record.Add("Group", AttributeValueHelper.StringOrNull(Trigger.Group));
            record.Add("Description", AttributeValueHelper.StringOrNull(Trigger.Description));
            record.Add("CalendarName", AttributeValueHelper.StringOrNull(Trigger.CalendarName));
            //record.Add("JobDataMap",  _jobDataMapConverter.ToEntry(Trigger.JobDataMap);
            record.Add("MisfireInstruction", new AttributeValue() { N = Trigger.MisfireInstruction.ToString() });
            record.Add("FireInstanceId", AttributeValueHelper.StringOrNull(Trigger.FireInstanceId));
            record.Add("StartTimeUtc", AttributeValueHelper.StringOrNull(Trigger.StartTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
            if (Trigger.EndTimeUtc.HasValue)
            {
                record.Add("EndTimeUtc", AttributeValueHelper.StringOrNull(Trigger.EndTimeUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
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
                record.Add("previousFireTimeUtc", new AttributeValue() { S = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString() });
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
                record.Add("Type", new AttributeValue() { S = "SimpleTriggerImpl" });
            }

            return record;
        }
    }
}