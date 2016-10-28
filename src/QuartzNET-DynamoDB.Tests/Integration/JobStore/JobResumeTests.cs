using Quartz.DynamoDB.Tests.Integration;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;

namespace Quartz.DynamoDB.Tests
{
    /// <summary>
    /// Contains tests related to the Resumption of Jobs and Job Groups.
    /// </summary>
    public class JobResumeTests
    {
        IJobStore _sut;

        public JobResumeTests()
        {
            _sut = new JobStore();
            var signaler = new Quartz.DynamoDB.Tests.Integration.RamJobStoreTests.SampleSignaler();
            var loadHelper = new SimpleTypeLoadHelper();

            _sut.Initialize(loadHelper, signaler);
        }

        /// <summary>
        /// Tests that when a paused job with one trigger is resumed, the trigger state is set back to Normal.
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void ResumeJobResumesTrigger()
        {
            var job = TestJobFactory.CreateTestJob();
            var trigger = TestTriggerFactory.CreateTestTrigger(job.Name, job.Group);

            _sut.StoreJobAndTrigger(job, trigger);
            _sut.PauseJob(job.Key);

            var state = _sut.GetTriggerState(trigger.Key);
            Assert.Equal(TriggerState.Paused, state);

            _sut.ResumeJob(job.Key);

            state = _sut.GetTriggerState(trigger.Key);
            Assert.Equal(TriggerState.Normal, state);
        }
    }
}