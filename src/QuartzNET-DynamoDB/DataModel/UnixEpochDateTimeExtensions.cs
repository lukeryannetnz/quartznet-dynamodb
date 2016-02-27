﻿using System;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// DateTime helpers for UnixEpoch time.
    /// </summary>
    public static class UnixEpochDateTimeExtensions
    {
        public static readonly DateTime UtcEpochTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Converts the DateTime to unix epoch time. 
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public static int ToUnixEpochTime(this DateTime datetime)
        {
            var utc = datetime.ToUniversalTime();
            TimeSpan t = utc - UtcEpochTime;
            int secondsSinceEpoch = (int)t.TotalSeconds;

            return secondsSinceEpoch;
        }
    }
}