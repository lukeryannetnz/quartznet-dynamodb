namespace Quartz.DynamoDB.Tests
{
    public class NoOpJob : IJob
    {
        public int TimesExecuted { get; private set; }

        public void Execute(IJobExecutionContext context)
        {
            this.TimesExecuted++;
        }
    }
}