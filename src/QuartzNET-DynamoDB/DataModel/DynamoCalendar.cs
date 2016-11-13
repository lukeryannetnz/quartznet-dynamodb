using System;
using Quartz.DynamoDB.DataModel.Storage;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Quartz.Impl.Calendar;
using System.Linq;
using Quartz.DynamoDB.DataModel;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// A wrapper class for a Quartz Calendar instance that can be serialized and stored in Amazon DynamoDB.
    /// </summary>
    public class DynamoCalendar : IInitialisableFromDynamoRecord, IConvertibleToDynamoRecord, IDynamoTableType
    {
        private readonly DateTimeOffsetConverter _dateTimeOffsetConverter = new DateTimeOffsetConverter();
        private readonly DateTimeConverter _dateTimeConverter = new DateTimeConverter();

        public DynamoCalendar()
        {
        }

        public DynamoCalendar(string name)
        {
            this.Name = name;
        }

        public DynamoCalendar(string name, ICalendar calendar) : this(name)
        {
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
                List<AttributeValue> excludedDays = ((AnnualCalendar)Calendar).DaysExcluded.Select(d => new AttributeValue() { N = _dateTimeOffsetConverter.ToEntry(d).ToString() }).ToList();

                if (excludedDays.Count > 0)
                {
                    record.Add("ExcludedDaysEpoch", new AttributeValue { L = excludedDays });
                }
            }
            if (Calendar is CronCalendar)
            {
                record.Add("Type", new AttributeValue { S = "CronCalendar" });
                record.Add("CronExpression", AttributeValueHelper.StringOrNull(((CronCalendar)Calendar).CronExpression.ToString()));
            }
            if (Calendar is DailyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "DailyCalendar" });
                record.Add("RangeStartingTimeUTCEpoch", new AttributeValue() { N = _dateTimeOffsetConverter.ToEntry(((DailyCalendar)Calendar).GetTimeRangeStartingTimeUtc(DateTime.UtcNow)).ToString() });
                record.Add("RangeEndingTimeUTCEpoch", new AttributeValue() { N = _dateTimeOffsetConverter.ToEntry(((DailyCalendar)Calendar).GetTimeRangeEndingTimeUtc(DateTime.UtcNow)).ToString() });
                record.Add("InvertTimeRange", new AttributeValue { BOOL = ((DailyCalendar)Calendar).InvertTimeRange });
            }
            if (Calendar is HolidayCalendar)
            {
                record.Add("Type", new AttributeValue { S = "HolidayCalendar" });
                var excludedDates = ((HolidayCalendar)Calendar).ExcludedDates.Select(d => new AttributeValue() { N = _dateTimeConverter.ToEntry(d).ToString() }).ToList();

                if (excludedDates.Count > 0)
                {
                    record.Add("ExcludedDatesEpoch", new AttributeValue { L = excludedDates });
                }
            }
            if (Calendar is MonthlyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "MonthlyCalendar" });
                List<AttributeValue> excludedDays = new List<AttributeValue>();

                bool[] days = ((MonthlyCalendar)Calendar).DaysExcluded;

                for (int i = 0; i < days.Length; i++)
                {
                    if (days[i])
                    {
                        excludedDays.Add(new AttributeValue { N = (i + 1).ToString() });
                    }
                }

                if (excludedDays.Count > 0)
                {
                    record.Add("ExcludedDays", new AttributeValue { L = excludedDays });
                }

            }
            if (Calendar is WeeklyCalendar)
            {
                record.Add("Type", new AttributeValue { S = "WeeklyCalendar" });
                List<AttributeValue> excludedDays = new List<AttributeValue>();

                bool[] days = ((WeeklyCalendar)Calendar).DaysExcluded;

                for (int i = 0; i < days.Length; i++)
                {
                    if (days[i])
                    {
                        excludedDays.Add(new AttributeValue { N = i.ToString() });
                    }
                }

                if (excludedDays.Count > 0)
                {
                    record.Add("ExcludedDays", new AttributeValue { L = excludedDays });
                }
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

                            if (record.ContainsKey("ExcludedDaysEpoch"))
                            {
                                foreach (var excluded in record["ExcludedDaysEpoch"].L)
                                {
                                    var day = _dateTimeOffsetConverter.FromEntry(int.Parse(excluded.N));
                                    annualCal.SetDayExcluded(day, true);
                                }
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
                            var startTime = _dateTimeConverter.FromEntry(int.Parse(record["RangeStartingTimeUTCEpoch"].N));
                            var endTime = _dateTimeConverter.FromEntry(int.Parse(record["RangeEndingTimeUTCEpoch"].N));

                            var dailyCal = new DailyCalendar(startTime, endTime);
                            dailyCal.InvertTimeRange = record["InvertTimeRange"].BOOL;

                            Calendar = dailyCal;
                            break;
                        }
                    case "HolidayCalendar":
                        {
                            var holidayCal = new HolidayCalendar();

                            if (record.ContainsKey("ExcludedDatesEpoch"))
                            {
                                foreach (var excluded in record["ExcludedDatesEpoch"].L)
                                {
                                    var day = _dateTimeConverter.FromEntry(int.Parse(excluded.N));
                                    holidayCal.AddExcludedDate(day);
                                }
                            }
                            Calendar = holidayCal;

                            break;
                        }
                    case "MonthlyCalendar":
                        {
                            var monthlyCal = new MonthlyCalendar();

                            if (record.ContainsKey("ExcludedDays"))
                            {
                                foreach (var excluded in record["ExcludedDays"].L)
                                {
                                    monthlyCal.SetDayExcluded(int.Parse(excluded.N), true);
                                }
                            }
                            Calendar = monthlyCal;
                            break;
                        }
                    case "WeeklyCalendar":
                        {
                            var monthlyCal = new WeeklyCalendar();

                            if (record.ContainsKey("ExcludedDays"))
                            {
                                foreach (var excluded in record["ExcludedDays"].L)
                                {
                                    monthlyCal.SetDayExcluded((DayOfWeek)Enum.Parse(typeof(DayOfWeek), excluded.N), true);
                                }
                            }
                            Calendar = monthlyCal;
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
                return new Dictionary<string, AttributeValue>
                {
                    {
                        "Name", new AttributeValue (){ S = Name}
                    }
                };
            }
        }

        private static string GetStorableCalendarTypeName(System.Type calendarType)
        {
            return calendarType.FullName + ", " + calendarType.Assembly.GetName().Name;
        }
    }
}

