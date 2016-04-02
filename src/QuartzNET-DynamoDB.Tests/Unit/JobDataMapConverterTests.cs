using System;
using System.Collections.Generic;
using System.Linq;
using Quartz.DynamoDB.DataModel;
using Xunit;
using Quartz.DynamoDB.DataModel.Storage;

namespace Quartz.DynamoDB.Tests.Unit
{
    public class JobDataMapConverterTests
    {
        [Fact] 
		[Trait("Category", "Unit")]
        public void StringsConvertSuccessfully()
        {
            JobDataMap input = new JobDataMap();
            input.Add("1", "I like to eat hamburgers");
            input.Add("2", "and fries");
            input.Add("test test test", "can you hear me?");

            var sut = new JobDataMapConverter();
            var d = sut.ToEntry(input);
            var output = (JobDataMap)sut.FromEntry(d);

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(input.Keys.ElementAt(i), output.Keys.ElementAt(i));
                Assert.Equal(input.Values.ElementAt(i), output.Values.ElementAt(i));
            }
        }

        [Fact] 
		[Trait("Category", "Unit")]
        public void ComplexObjectConvertsSuccessfully()
        {
            JobDataMap input = new JobDataMap();
            input.Add("test test test", new List<ComplexType>()
            {
                new ComplexType()
                {
                    Time = DateTime.UtcNow,
                    Quantity = 7.7775m,
                    Name = "The league of extraordinary gentlemen"
                },
                new ComplexType()
                {
                     Time = DateTime.MaxValue,
                    Quantity = 9.0m,
                    Name = "Welcome to the beginning of the end"
                }
            });

            var sut = new JobDataMapConverter();
            var d = sut.ToEntry(input);
            var output = (JobDataMap)sut.FromEntry(d);

            Assert.Equal(input.Keys.ElementAt(0), output.Keys.ElementAt(0));
            for (int i = 0; i < 2; i++)
            {
                ComplexType left = ((List<ComplexType>)input.Values.ElementAt(0)).ElementAt(i);
                ComplexType right = ((List<ComplexType>)output.Values.ElementAt(0)).ElementAt(i);

                Assert.Equal(left.Time, right.Time);
                Assert.Equal(left.Name, right.Name);
                Assert.Equal(left.Quantity, right.Quantity);
            }
        }

		[Fact] 
		[Trait("Category", "Unit")]
		public void EmptyDataMapSerialisesSuccessfully()
		{
			JobDataMap input = new JobDataMap();

			var sut = new JobDataMapConverter();
			var d = sut.ToEntry(input);

			Assert.True(d.NULL);
		}
    }

    public class ComplexType
    {
        public DateTime Time { get; set; }

        public decimal Quantity { get; set; }

        public string Name { get; set; }
    }
}
