using System;
using Quartz.DynamoDB.DataModel;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Contains tests for the DateTimeConverter class.
    /// </summary>
    public class DateTimeConverterTests
    {
        [Fact]
        public void Christmas2015SerializesCorrectly()
        {
            DateTime xmas2015 = new DateTime(2015, 12, 25);
            var sut = new DateTimeConverter();

            var epochTime = sut.ToEntry(xmas2015);
            Assert.Equal(1451001600, epochTime.AsInt());

            var result = sut.FromEntry(epochTime);
            Assert.Equal(xmas2015, (DateTime)result);
        }

        [Fact]
        public void PreEpochDateSerializesCorrectly()
        {
            DateTime ninthOctoberNineteenSixtyNine = new DateTime(1969, 10, 09, 07, 59, 59);
            var sut = new DateTimeConverter();

            var epochTime = sut.ToEntry(ninthOctoberNineteenSixtyNine);
            Assert.Equal(-7228801, epochTime.AsInt());

            var result = sut.FromEntry(epochTime);
            Assert.Equal(ninthOctoberNineteenSixtyNine, (DateTime)result);
        }
    }
}
