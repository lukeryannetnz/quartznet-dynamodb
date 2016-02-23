using Amazon.DynamoDBv2.DataModel;
using Quartz.Spi;

namespace Quartz.DynamoDB.DataModel
{
    [DynamoDBTable("Trigger")]
    public class DynamoTrigger
    {
        public DynamoTrigger()
        {
        }

        public DynamoTrigger(IOperableTrigger trigger)
        {
            Trigger = trigger;
        }

        [DynamoDBHashKey]
        public string Group
        {
            get { return Trigger.Key.Group; }
            set { }
        }

        public string Name
        {
            get { return Trigger.Key.Name; }
            set { }
        }

        [DynamoDBProperty(typeof (TriggerConverter))]
        public IOperableTrigger Trigger { get; set; }
    }
}