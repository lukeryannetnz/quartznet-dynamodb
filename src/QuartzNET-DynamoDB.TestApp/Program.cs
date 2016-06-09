using System;
using Quartz;
using Quartz.Impl;
using System.Threading;
using Quartz.Spi;

namespace QuartzNETDynamoDB.TestApp
{
	class MainClass
	{
		public static void Main(string[] args)
		{
			PrintGreeting();

			// Grab the Scheduler instance from the Factory 
			IScheduler scheduler = StdSchedulerFactory.GetDefaultScheduler();

			// and start it off
			scheduler.Start();

			ScheduleConsoleLoggingJob(scheduler);

			while (Console.ReadKey().Key != ConsoleKey.Q)
			{
				Thread.Sleep(100);
			}

			// and last shut down the scheduler when you are ready to close your program
			scheduler.Shutdown();
		}

		static void PrintGreeting()
		{
			Console.WriteLine("---------------------------------------");
			Console.WriteLine("Welcome to QuartzNET-DynamoDB test app!");
			Console.WriteLine("Press Q to quit.");
			Console.WriteLine("---------------------------------------");
		}

		private static void ScheduleConsoleLoggingJob(IScheduler scheduler)
		{
			// define the job and tie it to our HelloJob class
			IJobDetail job = JobBuilder.Create<ConsoleLoggingJob>()
				.WithIdentity("ConsoleJob", "ConsoleJobsGroup")
				.Build();

			// Trigger the job to run now, and then repeat every 10 seconds
			ITrigger trigger = TriggerBuilder.Create()
				.WithIdentity("SixtySecondTrigger", "SimpleTriggersGroup")
				.StartNow()
				.WithSimpleSchedule(x => x
					.WithIntervalInSeconds(10)
					.RepeatForever())
				.Build();

			// Tell quartz to schedule the job using our trigger
			scheduler.ScheduleJob(job, trigger);
		}
	}
}
