using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
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
            AbstractTrigger trigger = (AbstractTrigger) value;

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
            //doc["EndTimeUtc"] = trigger.EndTimeUtc;
            //doc["StartTimeUtc"] = trigger.StartTimeUtc;
            doc["Priority"] = trigger.Priority;

            if (value is CalendarIntervalTriggerImpl)
            {
                CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) value;
                //doc["complete"] = t.Com
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["Type"] = "CalendarIntervalTriggerImpl";
            }

            else if (value is CronTriggerImpl)
            {
                CronTriggerImpl t = (CronTriggerImpl) value;
                doc["CronExpressionString"] = t.CronExpressionString;
                doc["TimeZone"] = t.TimeZone.Id;
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["Type"] = "CronTriggerImpl";
            }

            else if (value is DailyTimeIntervalTriggerImpl)
            {
                DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) value;
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
                doc["Type"] = "DailyTimeIntervalTriggerImpl";
            }

            else if (value is SimpleTriggerImpl)
            {
                SimpleTriggerImpl t = (SimpleTriggerImpl) value;
                doc["nextFireTimeUtc"] = t.GetNextFireTimeUtc().GetValueOrDefault().ToString();
                doc["previousFireTimeUtc"] = t.GetPreviousFireTimeUtc().GetValueOrDefault().ToString();
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
                    trigger = new DailyTimeIntervalTriggerImpl();
                    break;
                }

                case "SimpleTriggerImpl":
                {
                    trigger = new SimpleTriggerImpl();
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
            //doc["EndTimeUtc"] = trigger.EndTimeUtc;
            //doc["StartTimeUtc"] = trigger.StartTimeUtc;
            trigger.Priority = doc["Priority"].AsInt();

            return trigger;
        }
    }
}