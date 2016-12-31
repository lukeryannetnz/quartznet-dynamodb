using System.Configuration;
namespace Quartz.DynamoDB.Tests
{
    public class TestJobStoreFactory
    {
        private static JobStore _store;

        public static DynamoDB.JobStore CreateTestJobStore()
        {
            _store = new JobStore();
            _store.InstanceName = ConfigurationManager.AppSettings["InstanceName"];

            return _store;
        }
    }
}