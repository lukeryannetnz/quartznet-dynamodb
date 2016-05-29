using System;
using Quartz;

namespace QuartzNETDynamoDB.TestApp
{
	public class ConsoleLoggingJob : IJob
	{
		public ConsoleLoggingJob ()
		{
		}


		public void Execute(IJobExecutionContext context)
		{
			Console.WriteLine("ConsoleLoggingJob executing. {0}", DateTime.Now);
		}
	}
}

