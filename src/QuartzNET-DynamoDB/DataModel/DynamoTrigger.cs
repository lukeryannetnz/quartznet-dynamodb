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
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A wrapper class for a Quartz Trigger instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
	public class DynamoTrigger : IInitialisableFromDynamoRecord,IConvertableToDynamoRecord, IDynamoTableType
    {
		private readonly JobDataMapConverter jobDataMapConverter = new JobDataMapConverter();
		private readonly DateTimeOffsetConverter dateTimeOffsetConverter = new DateTimeOffsetConverter();

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
			InitialiseFromDynamoRecord(item);
        }
			
		public void InitialiseFromDynamoRecord (Dictionary<string, AttributeValue> record)
		{
			string type = record.ContainsKey ("Type") ? record ["Type"].S : string.Empty;

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
					Trigger = new CronTriggerImpl();
					//todo: support CronTrigger
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
					simpleTrigger.RepeatInterval = new TimeSpan(int.Parse(record["RepeatInterval"].N));
					simpleTrigger.TimesTriggered = int.Parse(record["TimesTriggered"].N);
					break;
				}
			default:
				{
					Trigger = new SimpleTriggerImpl();
					break;
				}
			}

			State = record ["State"].S;
			SchedulerInstanceId = record ["SchedulerInstanceId"].S;
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

			if(record.ContainsKey("EndTimeUtc"))
			{
				Trigger.EndTimeUtc = DateTimeOffset.Parse(record["EndTimeUtc"].S);
			}

			if(record.ContainsKey("NextFireTimeUtcEpoch"))
			{
				Trigger.SetNextFireTimeUtc(dateTimeOffsetConverter.FromEntry(int.Parse(record["NextFireTimeUtcEpoch"].N)));
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
				return Trigger.Key.ToDictionary ();
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
        public string State { get; set; }

        /// <summary>
        /// Returns the State property as the TriggerState enumeration required by the JobStore contract.
        /// </summary>
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

        public Dictionary<string, AttributeValue> ToDynamo()
        {
            Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

			record.Add("State", AttributeValueHelper.StringOrNull (State));
			record.Add("SchedulerInstanceId", AttributeValueHelper.StringOrNull (SchedulerInstanceId));

            record.Add("Name", AttributeValueHelper.StringOrNull(Trigger.Name));
            record.Add("Group", AttributeValueHelper.StringOrNull(Trigger.Group));
            record.Add("JobName", AttributeValueHelper.StringOrNull(Trigger.JobName));
            record.Add("JobGroup", AttributeValueHelper.StringOrNull(Trigger.JobGroup));
            record.Add("Description", AttributeValueHelper.StringOrNull(Trigger.Description));
            record.Add("CalendarName", AttributeValueHelper.StringOrNull(Trigger.CalendarName));

			record.Add("JobDataMap",jobDataMapConverter.ToEntry(Trigger.JobDataMap));
            record.Add("MisfireInstruction", new AttributeValue() { N = Trigger.MisfireInstruction.ToString() });
            record.Add("FireInstanceId", AttributeValueHelper.StringOrNull(Trigger.FireInstanceId));
            record.Add("StartTimeUtc", AttributeValueHelper.StringOrNull(Trigger.StartTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
		
            if (Trigger.EndTimeUtc.HasValue)
            {
                record.Add("EndTimeUtc", AttributeValueHelper.StringOrNull(Trigger.EndTimeUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
            }

			if (Trigger.GetNextFireTimeUtc().HasValue)
			{
				record.Add("NextFireTimeUtcEpoch", new AttributeValue() { N = dateTimeOffsetConverter.ToEntry(Trigger.GetNextFireTimeUtc().Value).ToString()});
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