using System;
using System.Linq;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.Collection;
using Quartz.Impl.Triggers;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A converter from quartz Trigger objects to dynamo Document and back.
    /// </summary>
    public class TriggerConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            if (!(value is AbstractTrigger))
            {
                throw new ArgumentException("An abstract trigger object must be provided.");
            }

            Document doc = new Document();
            AbstractTrigger trigger = (AbstractTrigger)value;

            doc["Name"] = trigger.Name ?? string.Empty;
            doc["Group"] = trigger.Group ?? string.Empty;
            doc["JobName"] = trigger.JobName ?? string.Empty;
            doc["JobGroup"] = trigger.JobGroup ?? string.Empty;
            //doc["JobKey"] = trigger.JobKey; //todo flatten
            doc["Name"] = trigger.Name ?? string.Empty;
            doc["Group"] = trigger.Group ?? string.Empty;
            doc["Description"] = trigger.Description ?? string.Empty;
            doc["CalendarName"] = trigger.CalendarName ?? string.Empty;
            //doc["JobDataMap"] = trigger.JobDataMap; //todo: flatten
            doc["MisfireInstruction"] = trigger.MisfireInstruction;
            doc["FireInstanceId"] = trigger.FireInstanceId ?? string.Empty;
            doc["StartTimeUtc"] = trigger.StartTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz");
            if (trigger.EndTimeUtc.HasValue)
            {
                doc["EndTimeUtc"] = trigger.EndTimeUtc.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz");
            }

            doc["Priority"] = trigger.Priority;

            if (value is CalendarIntervalTriggerImpl)
            {
                CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl)value;
                //doc["complete"] = t.Com
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["Type"] = "CalendarIntervalTriggerImpl";
            }

            else if (value is CronTriggerImpl)
            {
                CronTriggerImpl t = (CronTriggerImpl)value;
                doc["CronExpressionString"] = t.CronExpressionString;
                doc["TimeZone"] = t.TimeZone.ToSerializedString();
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["Type"] = "CronTriggerImpl";
            }

            else if (value is DailyTimeIntervalTriggerImpl)
            {
                DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl)value;
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["DaysOfWeek"] = t.DaysOfWeek.Select(dow => dow.ToString()).ToList();
                doc["EndTimeOfDay_Hour"] = t.EndTimeOfDay.Hour;
                doc["EndTimeOfDay_Minute"] = t.EndTimeOfDay.Minute;
                doc["EndTimeOfDay_Second"] = t.EndTimeOfDay.Second;
                doc["RepeatCount"] = t.RepeatCount;
                doc["RepeatInterval"] = t.RepeatInterval;
                doc["RepeatIntervalUnit"] = (int)t.RepeatIntervalUnit;
                doc["StartTimeOfDay_Hour"] = t.StartTimeOfDay.Hour;
                doc["StartTimeOfDay_Minute"] = t.StartTimeOfDay.Minute;
                doc["StartTimeOfDay_Second"] = t.StartTimeOfDay.Second;
                doc["TimesTriggered"] = t.TimesTriggered;
                doc["TimeZone"] = t.TimeZone.ToSerializedString();

                doc["Type"] = "DailyTimeIntervalTriggerImpl";
            }

            else if (value is SimpleTriggerImpl)
            {
                SimpleTriggerImpl t = (SimpleTriggerImpl)value;
                //doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                //doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["RepeatCount"] = t.RepeatCount;
                doc["RepeatInterval"] = t.RepeatInterval.Ticks;
                doc["TimesTriggered"] = t.TimesTriggered;
                doc["Type"] = "SimpleTriggerImpl";
            }

            return doc;
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            Document doc = entry as Document;
            if (doc == null)
            {
                throw new ArgumentException("entry must be of type Amazon.DynamoDBv2.DocumentModel.Document");
            }

            AbstractTrigger trigger;
            switch (doc["Type"])
            {
                case "CalendarIntervalTriggerImpl":
                    {
                        trigger = new CalendarIntervalTriggerImpl();
                        break;
                    }
                case "CronTriggerImpl":
                    {
                        trigger = new CronTriggerImpl();
                        break;
                    }

                case "DailyTimeIntervalTriggerImpl":
                    {
                        var dailyTrigger = new DailyTimeIntervalTriggerImpl();
                        trigger = dailyTrigger;

                        var daysOfWeek = doc["DaysOfWeek"]
                                .AsListOfString()
                                .Select(dow => (DayOfWeek)Enum.Parse(typeof(DayOfWeek), dow));

                        dailyTrigger.DaysOfWeek = new HashSet<DayOfWeek>(daysOfWeek);

                        int endTimeOfDayHour = doc["EndTimeOfDay_Hour"].AsInt();
                        int endTimeOfDayMin = doc["EndTimeOfDay_Minute"].AsInt();
                        int endTimeOfDaySec = doc["EndTimeOfDay_Second"].AsInt();
                        dailyTrigger.EndTimeOfDay = new TimeOfDay(endTimeOfDayHour, endTimeOfDayMin, endTimeOfDaySec);
                        dailyTrigger.RepeatCount = doc["RepeatCount"].AsInt();
                        dailyTrigger.RepeatInterval = doc["RepeatInterval"].AsInt();
                        dailyTrigger.RepeatIntervalUnit = (IntervalUnit)doc["RepeatIntervalUnit"].AsInt();
                        int startTimeOfDayHour = doc["StartTimeOfDay_Hour"].AsInt();
                        int startTimeOfDayMin = doc["StartTimeOfDay_Minute"].AsInt();
                        int startTimeOfDaySec = doc["StartTimeOfDay_Second"].AsInt();
                        dailyTrigger.StartTimeOfDay = new TimeOfDay(startTimeOfDayHour, startTimeOfDayMin, startTimeOfDaySec);
                        dailyTrigger.TimesTriggered = doc["TimesTriggered"].AsInt();
                        dailyTrigger.TimeZone = TimeZoneInfo.FromSerializedString(doc["TimeZone"]);
                        break;
                    }

                case "SimpleTriggerImpl":
                    {
                        var simpleTrigger = new SimpleTriggerImpl();
                        trigger = simpleTrigger;

                        simpleTrigger.RepeatCount = doc["RepeatCount"].AsInt();
                        simpleTrigger.RepeatInterval = new TimeSpan(doc["RepeatInterval"].AsLong());
                        simpleTrigger.TimesTriggered = doc["TimesTriggered"].AsInt();
                        break;
                    }
                default:
                    {
                        throw new Exception("Unexpected trigger type encountered.");
                    }
            }
            trigger.Name = doc.TryGetStringValueOtherwiseReturnDefault("Name");
            trigger.Group = doc.TryGetStringValueOtherwiseReturnDefault("Group");
            trigger.JobName = doc.TryGetStringValueOtherwiseReturnDefault("JobName");
            trigger.JobGroup = doc.TryGetStringValueOtherwiseReturnDefault("JobGroup");
            //doc["JobKey"] = trigger.JobKey; //todo flatten
            trigger.Name = doc.TryGetStringValueOtherwiseReturnDefault("Name");
            trigger.Group = doc.TryGetStringValueOtherwiseReturnDefault("Group");
            trigger.Description = doc.TryGetStringValueOtherwiseReturnDefault("Description");
            trigger.CalendarName = doc.TryGetStringValueOtherwiseReturnDefault("CalendarName");
            //doc["JobDataMap"] = trigger.JobDataMap; //todo: flatten
            trigger.MisfireInstruction = doc["MisfireInstruction"].AsInt();
            trigger.FireInstanceId = doc.TryGetStringValueOtherwiseReturnDefault("FireInstanceId");
            
            trigger.StartTimeUtc = DateTimeOffset.Parse(doc["StartTimeUtc"]);
            DynamoDBEntry value;
            if (doc.TryGetValue("EndTimeUtc", out value))
            {
                trigger.EndTimeUtc = DateTimeOffset.Parse(value);
            }
            trigger.Priority = doc["Priority"].AsInt();

            return trigger;
        }
    }
}