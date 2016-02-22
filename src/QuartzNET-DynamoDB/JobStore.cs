using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Matchers;
using Quartz.Spi;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// This class implements a <see cref="IJobStore" /> that
    /// utilizes Amazon DynamoDB as its storage device.
    /// <para>
    /// </para>
    /// <author>Luke Ryan</author>
    /// </summary>
    public class JobStore : IJobStore
    {
        private IAmazonDynamoDB _client;
        private ITypeLoadHelper _loadHelper;

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            _client = AmazonDynamoDbClientFactory.Create();
            new DynamoBootstrapper().BootStrap(_client);

            if (loadHelper == null)
            {
                throw new ArgumentNullException(nameof(loadHelper));
            }

            _loadHelper = loadHelper;
        }

        public void SchedulerStarted()
        {
        }

        public void SchedulerPaused()
        {
            throw new NotImplementedException();
        }

        public void SchedulerResumed()
        {
            throw new NotImplementedException();
        }

        public void Shutdown()
        {
            _client.Dispose();
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public bool IsJobGroupPaused(string groupName)
        {
            throw new NotImplementedException();
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            throw new NotImplementedException();
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            var context = new DynamoDBContext(_client);

            if (!replaceExisting && context.Load<DynamoJobDetail>(newJob.Key.Group, newJob.Key.Name) != null)
            {
                throw new ArgumentException("Job with that key already exists.", nameof(newJob));
            }

            DynamoJobDetail job = DynamoJobDetail.Clone(newJob);

            context.Save(job);
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            throw new NotImplementedException();
        }

        public bool RemoveJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            throw new NotImplementedException();
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            var context = new DynamoDBContext(_client);

            return context.Load<DynamoJobDetail>(jobKey.Group, jobKey.Name);
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            throw new NotImplementedException();
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            throw new NotImplementedException();
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public bool CalendarExists(string calName)
        {
            throw new NotImplementedException();
        }

        public bool CheckExists(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public void ClearAllSchedulingData()
        {
            throw new NotImplementedException();
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            throw new NotImplementedException();
        }

        public bool RemoveCalendar(string calName)
        {
            throw new NotImplementedException();
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfJobs()
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfTriggers()
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfCalendars()
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public IList<string> GetJobGroupNames()
        {
            throw new NotImplementedException();
        }

        public IList<string> GetTriggerGroupNames()
        {
            throw new NotImplementedException();
        }

        public IList<string> GetCalendarNames()
        {
            throw new NotImplementedException();
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void PauseJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            throw new NotImplementedException();
        }

        public void ResumeJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void PauseAll()
        {
            throw new NotImplementedException();
        }

        public void ResumeAll()
        {
            throw new NotImplementedException();
        }

        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            throw new NotImplementedException();
        }

        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            throw new NotImplementedException();
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            throw new NotImplementedException();
        }

        public bool SupportsPersistence { get; }
        public long EstimatedTimeToReleaseAndAcquireTrigger { get; }
        public bool Clustered { get; }
        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }
    }
}
