using System;
using Quartz;
using System.Threading;

namespace QuartzNETDynamoDB.TestApp
{
	public class ConsoleLoggingJob : IJob
	{
		public ConsoleLoggingJob ()
		{
		}


		public void Execute(IJobExecutionContext context)
		{
			Console.WriteLine("ConsoleLoggingJob executing. {0}, Thread {1}", DateTime.Now, Thread.CurrentThread.ManagedThreadId);
		}
	}
}

