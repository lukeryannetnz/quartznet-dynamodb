using System;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.Impl;

namespace Quartz.DynamoDB.DataModel
{
    /// <summary>
    /// A converter from quartz JobDetail to dynamo Document and back.
    /// </summary>
    public class JobDetailConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            IJobDetail job = value as IJobDetail;
            if (job == null)
            {
                throw new ArgumentException("value must be of type IJobDetail");
            }

            Document doc = new Document();

            doc["Key"] = new JobKeyConverter().ToEntry(job.Key);
            doc["Description"] = job.Description ?? string.Empty;
            doc["JobType"] = new JobTypeConverter().ToEntry(job.JobType);
            //JobDataMap = job.JobDataMap,
            doc["Durable"] = job.Durable;
            doc["PersistJobDataAfterExecution"] = job.PersistJobDataAfterExecution;
            doc["ConcurrentExecutionDisallowed"] = job.ConcurrentExecutionDisallowed;
            doc["RequestsRecovery"] = job.RequestsRecovery;

            return doc;
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            Document doc = entry as Document;
            if (doc == null)
            {
                throw new ArgumentException("entry must be of type Amazon.DynamoDBv2.DocumentModel.Document");
            }

            JobDetailImpl job = new JobDetailImpl();
            job.Key = (JobKey)new JobKeyConverter().FromEntry(doc["Key"]);
            job.Description = doc.TryGetStringValueOtherwiseReturnDefault("Description");
            job.JobType = (Type)new JobTypeConverter().FromEntry(doc["JobType"]);
            //JobDataMap = job.JobDataMap,
            job.Durable = doc["Durable"].AsBoolean();
            job.RequestsRecovery = doc["RequestsRecovery"].AsBoolean();

            return job;
        }
    }
}