using System;
using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Contains tests for the DateTimeConverter class.
    /// </summary>
    public class DateTimeOffsetConverterTests
    {
        [Fact] [Trait("Category", "Unit")]

        public void UtcChristmas2015SerializesCorrectly()
        {
            DateTimeOffset xmas2015 = new DateTime(2015, 12, 25, 0, 0, 0, DateTimeKind.Utc);
            var sut = new DateTimeOffsetConverter();

            var epochTime = sut.ToEntry(xmas2015);
            Assert.Equal(1451001600, epochTime.AsInt());

            var result = sut.FromEntry(epochTime);
            Assert.Equal(xmas2015, (DateTimeOffset)result);
            Assert.Equal(xmas2015.Offset, ((DateTimeOffset)result).Offset);
        }

        [Fact] [Trait("Category", "Unit")]

        public void UtcPreEpochDateSerializesCorrectly()
        {
            DateTimeOffset ninthOctoberNineteenSixtyNine = new DateTime(1969, 10, 09, 07, 59, 59, DateTimeKind.Utc);
            var sut = new DateTimeOffsetConverter();

            var epochTime = sut.ToEntry(ninthOctoberNineteenSixtyNine);
            Assert.Equal(-7228801, epochTime.AsInt());

            var result = sut.FromEntry(epochTime);
            Assert.Equal(ninthOctoberNineteenSixtyNine, (DateTimeOffset)result);
            Assert.Equal(ninthOctoberNineteenSixtyNine.Offset, ((DateTimeOffset)result).Offset);
        }

        /// <summary>
        /// When given a DateTimeOffset for a local date (in this case whatever timezone the computer running these tests is using)
        /// the converter should convert the date/time to UTC and store it as UNIX Epoch. This means when retrieving
        /// back as a DateTimeOffset, the local timezone will be lost and the offset will be UTC.
        /// </summary>
        [Fact] [Trait("Category", "Unit")]

        public void LocalNowDateSerializesCorrectly()
        {
            DateTimeOffset localNow = DateTime.Now;
            var sut = new DateTimeOffsetConverter();

            var epochTime = sut.ToEntry(localNow);

            var result = sut.FromEntry(epochTime);
            Assert.Equal(localNow.ToUniversalTime().Day, ((DateTimeOffset)result).UtcDateTime.Day);
            Assert.Equal(localNow.ToUniversalTime().Month, ((DateTimeOffset)result).UtcDateTime.Month);
            Assert.Equal(localNow.ToUniversalTime().Year, ((DateTimeOffset)result).UtcDateTime.Year);
            Assert.Equal(localNow.ToUniversalTime().Hour, ((DateTimeOffset)result).UtcDateTime.Hour);
            Assert.Equal(localNow.ToUniversalTime().Minute, ((DateTimeOffset)result).UtcDateTime.Minute);
            Assert.Equal(localNow.ToUniversalTime().Second, ((DateTimeOffset)result).UtcDateTime.Second);
            // millisecond precision is lost.
            Assert.Equal(0, ((DateTimeOffset)result).UtcDateTime.Millisecond);
            //the output should be in UTC.
            Assert.Equal(((DateTimeOffset)result).DateTime, ((DateTimeOffset)result).UtcDateTime);
        }
    }
}
