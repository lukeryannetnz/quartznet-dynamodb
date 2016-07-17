using System;
using Quartz.DynamoDB.DataModel.Storage;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl.Calendar;
using System.Linq;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// A wrapper class for a Quartz Calendar instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    public class DynamoCalendar : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType
    {
        public DynamoCalendar()
        {
        }

        public DynamoCalendar(string name, ICalendar calendar)
        {
            this.Name = name;
            this.Description = calendar.Description;
            this.Calendar = calendar;
        }

        public DynamoCalendar(Dictionary<string, AttributeValue> record)
        {
            InitialiseFromDynamoRecord(record);
        }

        public string Name
        {
            get;
            set;
        }

        public string Description
        {
            get;
            set;
        }

        public ICalendar Calendar
        {
            get;
            set;
        }

        public System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> ToDynamo()
        {
            Dictionary<string, AttributeValue> record = new Dictionary<string, AttributeValue>();

            record.Add("Name", AttributeValueHelper.StringOrNull(Name));
            record.Add("Description", AttributeValueHelper.StringOrNull(Description));

            if (Calendar == null)
            {
                return record;
            }

            if (Calendar is AnnualCalendar)
            {
                record.Add("Type", new AttributeValue { S = "AnnualCalendar" });
                List<AttributeValue> excludedDays = ((AnnualCalendar)Calendar).DaysExcluded.Select(d => new AttributeValue() { S = (d.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")) }).ToList();
                record.Add("ExcludedDays", new AttributeValue { L = excludedDays });
            }
            if (Calendar is CronCalendar)
            {
                record.Add("Type", new AttributeValue { S = "CronCalendar" });
                record.Add("CronExpression", AttributeValueHelper.StringOrNull(((CronCalendar)Calendar).CronExpression.ToString()));
            }
            if (Calendar is DailyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "DailyCalendar" });
                record.Add("RangeStartingTimeUTC", AttributeValueHelper.StringOrNull(((DailyCalendar)Calendar).GetTimeRangeStartingTimeUtc(DateTime.UtcNow).ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
                record.Add("RangeEndingTimeUTC", AttributeValueHelper.StringOrNull(((DailyCalendar)Calendar).GetTimeRangeEndingTimeUtc(DateTime.UtcNow).ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")));
                record.Add("InvertTimeRange", new AttributeValue { BOOL = ((DailyCalendar)Calendar).InvertTimeRange });
            }
            if (Calendar is HolidayCalendar)
            {
                record.Add("Type", new AttributeValue { S = "HolidayCalendar" });
                var tickStringList = ((HolidayCalendar)Calendar).ExcludedDates.Select(d => new AttributeValue() { N = d.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz") }).ToList();
                record.Add("ExcludedDates", new AttributeValue { L = tickStringList });
            }
            if (Calendar is MonthlyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "MonthlyCalendar" });
                List<AttributeValue> excludedDays = ((MonthlyCalendar)Calendar).DaysExcluded.Select(d => new AttributeValue() { N = (d ? "1" : "0") }).ToList();
                record.Add("ExcludedDays", new AttributeValue { L = excludedDays });

            }
            if (Calendar is WeeklyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "WeeklyCalendar" });
                List<AttributeValue> excludedDays = ((MonthlyCalendar)Calendar).DaysExcluded.Select(d => new AttributeValue() { N = (d ? "1" : "0") }).ToList();
                record.Add("ExcludedDays", new AttributeValue { L = excludedDays });
            }

            return record;
        }

        public void InitialiseFromDynamoRecord(System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> record)
        {
            Name = record["Name"].S;
            Description = record["Description"].S;

            if (record.ContainsKey("Type"))
            {
                switch (record["Type"].S)
                {
                    case "AnnualCalendar":
                        {
                            var annualCal = new AnnualCalendar();

                            foreach (var excluded in record["ExcludedDays"].L)
                            {
                                DateTimeOffset day = DateTimeOffset.Parse(excluded.S);
                                annualCal.SetDayExcluded(day, true);
                            }

                            Calendar = annualCal;
                            break;
                        }
                    case "CronCalendar":
                        {
                            var expression = record["CronExpression"].S;
                            var cronCal = new CronCalendar(expression);

                            Calendar = cronCal;
                            break;
                        }
                    case "DailyCalendar":
                        {
                            break;
                        }
                    case "HolidayCalendar":
                        {
                            break;
                        }
                    case "MonthlyCalendar":
                        {
                            var monthlyCal = new MonthlyCalendar();

                            Calendar = monthlyCal;
                            break;
                        }
                    case "WeeklyCalendar":
                        {
                            break;
                        }
                }
            }
            else
            {
                Calendar = new BaseCalendar();
            }

            Name = record["Name"].S;
            Description = record["Description"].S;
            Calendar.Description = Description;
        }

        public string DynamoTableName
        {
            get
            {
                return DynamoConfiguration.CalendarTableName;
            }
        }

        public System.Collections.Generic.Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> Key
        {
            get
            {
                return new Dictionary<string, AttributeValue> {
                        { "Name", new AttributeValue (){ S = Name } }
                    };
            }
        }

        private static string GetStorableCalendarTypeName(System.Type calendarType)
        {
            return calendarType.FullName + ", " + calendarType.Assembly.GetName().Name;
        }
    }
}

