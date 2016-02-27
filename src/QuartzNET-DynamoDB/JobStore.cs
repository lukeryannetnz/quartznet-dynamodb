using System;
using System.Collections.Generic;
using System.Linq;
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
    public class JobStore : IJobStore, IDisposable
    {
        //todo: think about thread safety.

        private DynamoDBContext _context;
        private ITypeLoadHelper _loadHelper;
        private string _instanceId;
        private string _instanceName;

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            var client = DynamoDbClientFactory.Create();
            _context = new DynamoDBContext(client);
            new DynamoBootstrapper().BootStrap(client);

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
            Dispose();
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
            if (!replaceExisting && _context.Load<DynamoJob>(newJob.Key.Group, newJob.Key.Name) != null)
            {
                throw new ObjectAlreadyExistsException(newJob);
            }

            DynamoJob job = new DynamoJob(newJob);

            _context.Save(job);
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs,
            bool replace)
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
            DynamoJob record = _context.Load<DynamoJob>(jobKey.Group, jobKey.Name);
            return record?.Job;
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            if (!replaceExisting && _context.Load<DynamoJob>(newTrigger.Key.Group, newTrigger.Key.Name) != null)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            if (RetrieveJob(newTrigger.JobKey) == null)
            {
                throw new JobPersistenceException("The job (" + newTrigger.JobKey +
                                                  ") referenced by the trigger does not exist.");
            }

            DynamoTrigger trigger = new DynamoTrigger(newTrigger);

            //if (this.PausedTriggerGroups.FindOneByIdAs<BsonDocument>(newTrigger.Key.Group) != null
            //    || this.PausedJobGroups.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.Group) != null)
            //{
            //    state = "Paused";
            //    if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.ToBsonDocument()) != null)
            //    {
            //        state = "PausedAndBlocked";
            //    }
            //}
            //else if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(newTrigger.JobKey.ToBsonDocument()) != null)
            //{
            //    state = "Blocked";
            //}


            _context.Save(trigger);
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
            var record = _context.Load<DynamoTrigger>(triggerKey.Group, triggerKey.Name);
            return record?.Trigger;
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
            var record = _context.Load<DynamoTrigger>(triggerKey.Group, triggerKey.Name);

            //todo: consider if we need paused and blocked
            return record?.TriggerState ?? TriggerState.None;
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            var record = _context.Load<DynamoTrigger>(triggerKey.Group, triggerKey.Name);

            if (record.TriggerState == TriggerState.Blocked)
            {
                record.State = "PausedAndBlocked";
            }
            else
            {
                record.State = "Paused";
            }

            _context.Save(record, new DynamoDBOperationConfig());
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
            //IOperableTrigger trigger = this.Triggers.FindOneByIdAs<IOperableTrigger>(triggerKey.ToBsonDocument());
            var record = _context.Load<DynamoTrigger>(triggerKey.Group, triggerKey.Name);

            // does the trigger exist?
            if (record == null)
            {
                return;
            }

            // if the trigger is not paused resuming it does not make sense...
            if (record.State != "Paused" &&
                record.State != "PausedAndBlocked")
            {
                return;
            }

            //if (this.BlockedJobs.FindOneByIdAs<BsonDocument>(trigger.JobKey.ToBsonDocument()) != null)
            //{
            //    triggerState["State"] = "Blocked";
            //}
            //else
            //{
            record.State = "Waiting";
            //}

            //this.ApplyMisfire(trigger);

            _context.Save(record, new DynamoDBOperationConfig());
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
            // multiple instances management
            //this.Schedulers.Save(new BsonDocument()
            //    .SetElement(new BsonElement("_id", this.instanceId))
            //    .SetElement(new BsonElement("Expires", (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime))
            //    .SetElement(new BsonElement("State", "Running")));

            var scheduler = new DynamoScheduler
            {
                InstanceId = _instanceId,
                Expires = (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime,
                State = "Running"
            };

            _context.Save(scheduler);

            
            ScanCondition expiredCondition = new ScanCondition("Expires", ScanOperator.LessThan, SystemTime.Now().UtcDateTime.ToUnixEpochTime());
            var expiredSchedulers = _context.Scan<DynamoScheduler>(expiredCondition);

            foreach (var dynamoScheduler in expiredSchedulers)
            {
                _context.Delete(dynamoScheduler);
            }
            
            var activeSchedulers = _context.Scan<DynamoScheduler>();
            //IEnumerable<BsonValue> activeInstances = this.Schedulers.Distinct("_id");

            // Reset the state of any triggers that are associated with non-active schedulers.
            //todo: this will be slow. do the query based on an index.
            foreach (var trigger in _context.Scan<DynamoTrigger>())
            {
                if(!activeSchedulers.Select(s => s.InstanceId).Contains(trigger.SchedulerInstanceId))
                {
                    trigger.SchedulerInstanceId = string.Empty;
                    trigger.State = "Waiting";

                    _context.Save(trigger);
                }
            }

            List<IOperableTrigger> result = new List<IOperableTrigger>();
            Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
            //DateTimeOffset? firstAcquiredTriggerFireTime = null;

            //var candidates = this.Triggers.FindAs<Spi.IOperableTrigger>(
            //    Query.And(
            //        Query.EQ("State", "Waiting"),
            //        Query.LTE("nextFireTimeUtc", (noLaterThan + timeWindow).UtcDateTime)))
            //    .OrderBy(t => t.GetNextFireTimeUtc()).ThenByDescending(t => t.Priority);

            //foreach (IOperableTrigger trigger in candidates)
            //{
            //    if (trigger.GetNextFireTimeUtc() == null)
            //    {
            //        continue;
            //    }

            //    // it's possible that we've selected triggers way outside of the max fire ahead time for batches 
            //    // (up to idleWaitTime + fireAheadTime) so we need to make sure not to include such triggers.  
            //    // So we select from the first next trigger to fire up until the max fire ahead time after that...
            //    // which will perfectly honor the fireAheadTime window because the no firing will occur until
            //    // the first acquired trigger's fire time arrives.
            //    if (firstAcquiredTriggerFireTime != null
            //        && trigger.GetNextFireTimeUtc() > (firstAcquiredTriggerFireTime.Value + timeWindow))
            //    {
            //        break;
            //    }

            //    if (this.ApplyMisfire(trigger))
            //    {
            //        if (trigger.GetNextFireTimeUtc() == null
            //            || trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
            //        {
            //            continue;
            //        }
            //    }

            //    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
            //    // put it back into the timeTriggers set and continue to search for next trigger.
            //    JobKey jobKey = trigger.JobKey;
            //    IJobDetail job = this.Jobs.FindOneByIdAs<IJobDetail>(jobKey.ToBsonDocument());

            //    if (job.ConcurrentExecutionDisallowed)
            //    {
            //        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
            //        {
            //            continue; // go to next trigger in store.
            //        }
            //        else
            //        {
            //            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
            //        }
            //    }

            //    trigger.FireInstanceId = this.GetFiredTriggerRecordId();
            //    var acquired = this.Triggers.FindAndModify(
            //        Query.And(
            //            Query.EQ("_id", trigger.Key.ToBsonDocument()),
            //            Query.EQ("State", "Waiting")),
            //        SortBy.Null,
            //        Update.Set("State", "Acquired")
            //            .Set("SchedulerInstanceId", this.instanceId)
            //            .Set("FireInstanceId", trigger.FireInstanceId));

            //    if (acquired.ModifiedDocument != null)
            //    {
            //        result.Add(trigger);

            //        if (firstAcquiredTriggerFireTime == null)
            //        {
            //            firstAcquiredTriggerFireTime = trigger.GetNextFireTimeUtc();
            //        }
            //    }

            //    if (result.Count == maxCount)
            //    {
            //        break;
            //    }
            //}

            //return result;
            //}

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

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode)
        {
            throw new NotImplementedException();
        }

        public bool SupportsPersistence { get; }
        public long EstimatedTimeToReleaseAndAcquireTrigger { get; }
        public bool Clustered { get; }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> of the Scheduler instance's Id, 
        /// prior to initialize being invoked.
        /// </summary>
        public virtual string InstanceId
        {
            set { this._instanceId = value; }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> of the Scheduler instance's name, 
        /// prior to initialize being invoked.
        /// </summary>
        public virtual string InstanceName
        {
            set { this._instanceName = value; }
        }

        public int ThreadPoolSize { get; set; }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _context.Dispose();
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}