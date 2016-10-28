using Quartz.DynamoDB.DataModel;
using Quartz.Job;
using Xunit;

namespace Quartz.DynamoDB.Tests.Unit
{
    /// <summary>
    /// Tests the DynamoJob serialisation for all attributes of the type.
    /// </summary>
    public class DynamoJobSerialisationTests
    {
        [Fact]
        [Trait("Category", "Unit")]
        public void JobNameSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.Key.Name, result.Job.Key.Name);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobGroupSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.Key.Group, result.Job.Key.Group);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobDescriptionSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.Description, result.Job.Description);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobTypeSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.JobType, result.Job.JobType);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobDataMapSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.JobDataMap, result.Job.JobDataMap);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobDurabilitySerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.Durable, result.Job.Durable);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobPersistAfterExecutionSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.PersistJobDataAfterExecution, result.Job.PersistJobDataAfterExecution);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobConcurrentExecutionSerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.ConcurrentExecutionDisallowed, result.Job.ConcurrentExecutionDisallowed);
        }

        [Fact]
        [Trait("Category", "Unit")]
        public void JobRequestsRecoverySerializesCorrectly()
        {
            var job = CreateDynamoJob();

            var serialized = job.ToDynamo();
            var result = new DynamoJob(serialized);

            Assert.Equal(job.Job.RequestsRecovery, result.Job.RequestsRecovery);
        }

        private static DynamoJob CreateDynamoJob()
        {
            var jobDataMap = new JobDataMap
            {
                {"1", "I like to eat hamburgers"},
                {"2", "and fries"},
                {"test test test", "can you hear me?"}
            };
            var job = new DynamoJob(JobBuilder.Create<NoOpJob>()
                .WithIdentity("testJob")
                .WithDescription("test job")
                .StoreDurably(true)
                .RequestRecovery(false)
                .SetJobData(jobDataMap)
                .Build());
            return job;
        }
    }
}
