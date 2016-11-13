using System;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// Converts .NET DateTime objects to integer UTC unix-EPOCH time format for storage.
    /// </summary>
    public class DateTimeConverter
    {
        /// <summary>
        /// Returns a dynamodb entry (int) with the datetime relative to unix epoch time.
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public int ToEntry(DateTime datetime)
        {
            return datetime.ToUniversalTime().ToUnixEpochTime();
        }

        /// <summary>
        /// Returns the UTC DateTime that the epoch dynamo record represents.
        /// </summary>
        /// <param name="secondsSinceUtcEpoch"></param>
        /// <returns></returns>
        public DateTime FromEntry(int secondsSinceUtcEpoch)
        {
            return UnixEpochDateTimeExtensions.UtcEpochTime.AddSeconds(secondsSinceUtcEpoch);
        }
    }
}
