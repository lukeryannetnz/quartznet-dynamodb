using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using Amazon.DynamoDBv2.Model;
using System.Net;
using Quartz.DynamoDB.DataModel.Storage;

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
        private AmazonDynamoDBClient _client;
		private IRepository<DynamoJob, JobKey> _jobRepository;
		private IRepository<DynamoTrigger, TriggerKey> _triggerRepository;

        private string _instanceId;
        //private string _instanceName;

        /// <summary>
        /// Tracks if dispose has been called to detect redundant (multiple) dispose calls.
        /// </summary>
        private bool _disposedValue = false;

        private TimeSpan _misfireThreshold;

        private ISchedulerSignaler _signaler;

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            if (loadHelper == null)
            {
                throw new ArgumentNullException(nameof(loadHelper));
            }
            if (signaler == null)
            {
                throw new ArgumentNullException(nameof(signaler));
            }

            _client = DynamoDbClientFactory.Create();
            _context = new DynamoDBContext(_client);
			_jobRepository = new Repository<DynamoJob, JobKey> (_client);
			_triggerRepository = new Repository<DynamoTrigger, TriggerKey> (_client);
            new DynamoBootstrapper().BootStrap(_client);

            //_loadHelper = loadHelper;
            _signaler = signaler;

            // We should have had an instance id assigned by now, but if we haven't assign one.
            if (string.IsNullOrEmpty(InstanceId))
            {
                InstanceId = Guid.NewGuid().ToString();
            }
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
			if (!replaceExisting && _jobRepository.Load(newJob.Key) != null)
            {
                throw new ObjectAlreadyExistsException(newJob);
            }

            DynamoJob job = new DynamoJob(newJob);

			_jobRepository.Store(job);
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
			var job = _jobRepository.Load (jobKey);

			return job == null ? null : job.Job;
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
			if (!replaceExisting && _triggerRepository.Load(newTrigger.Key) != null)
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

            var response = _client.PutItem(DynamoConfiguration.TriggerTableName, trigger.ToDynamo());

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException(string.Format("Non 200 status code returned from Dynamo: {0}", response));
            }
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
            var request = new GetItemRequest(
              DynamoConfiguration.TriggerTableName,
              new Dictionary<string, AttributeValue>
              {
                    { "Name", new AttributeValue() { S = triggerKey.Name } },
                    { "Group", new AttributeValue() { S = triggerKey.Group } }
              });

            var response = _client.GetItem(request);

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new JobPersistenceException($"Non 200 response code received when querying dynamo {response.ToString()}");
            }

            return response.IsItemSet ? new DynamoTrigger(response.Item).Trigger : null;
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

            //this.ApplyMisfireIfNecessary(trigger);

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

        /// <summary>
        /// A counter for fired trigger records.
        /// TODO: put something here that explains what this does.
        /// </summary>
        private static long _ftrCtr = SystemTime.UtcNow().Ticks;

        

        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            long value = Interlocked.Increment(ref _ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
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
                ExpiresUtc = (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime,
                State = "Running"
            };

            _context.Save(scheduler);

            int epochNow = SystemTime.Now().UtcDateTime.ToUnixEpochTime();

            ScanCondition expiredCondition = new ScanCondition("ExpiresUtcEpoch", ScanOperator.LessThan,
               epochNow);
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
                if (!string.IsNullOrEmpty(trigger.SchedulerInstanceId) && !activeSchedulers.Select(s => s.InstanceId).Contains(trigger.SchedulerInstanceId))
                {
                    trigger.SchedulerInstanceId = string.Empty;
                    trigger.State = "Waiting";

                    _context.Save(trigger);
                }
            }

            List<IOperableTrigger> result = new List<IOperableTrigger>();
            Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            var waitingStateCondition = new ScanCondition("State", ScanOperator.Equal, "Waiting");
            string maxNextFireTime = (noLaterThan + timeWindow).UtcDateTime.ToUnixEpochTime().ToString();
            var dueToFireCondition = new ScanCondition("NextFireTimeUtcEpoch", ScanOperator.LessThanOrEqual, maxNextFireTime);
            var candidates = _context.Scan<DynamoTrigger>(waitingStateCondition, dueToFireCondition);
            //var candidates = this.Triggers.FindAs<Spi.IOperableTrigger>(
            //    Query.And(
            //        Query.EQ("State", "Waiting"),
            //        Query.LTE("nextFireTimeUtc", (noLaterThan + timeWindow).UtcDateTime)))
            //    .OrderBy(t => t.GetNextFireTimeUtc()).ThenByDescending(t => t.Priority);

            foreach (var trigger in candidates)
            {
                if (trigger.Trigger.GetNextFireTimeUtc() == null)
                {
                    continue;
                }

                // it's possible that we've selected triggers way outside of the max fire ahead time for batches 
                // (up to idleWaitTime + fireAheadTime) so we need to make sure not to include such triggers.  
                // So we select from the first next trigger to fire up until the max fire ahead time after that...
                // which will perfectly honor the fireAheadTime window because the no firing will occur until
                // the first acquired trigger's fire time arrives.
                if (firstAcquiredTriggerFireTime != null
                    && trigger.Trigger.GetNextFireTimeUtc() > (firstAcquiredTriggerFireTime.Value + timeWindow))
                {
                    break;
                }

                if (this.ApplyMisfireIfNecessary(trigger))
                {
                    if (trigger.Trigger.GetNextFireTimeUtc() == null
                        || trigger.Trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                    {
                        continue;
                    }
                }

                // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                // put it back into the timeTriggers set and continue to search for next trigger.
                JobKey jobKey = trigger.Trigger.JobKey;
                IJobDetail job = RetrieveJob(jobKey);

                if (job.ConcurrentExecutionDisallowed)
                {
                    if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                    {
                        continue; // go to next trigger in store.
                    }
                    else
                    {
                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }
                }


                // Do this using the lower level Document Model (rather than object model) as Object Model
                // support for conditional updates (optimistic locking) doesn't provide adequate feedback when
                // an update is successful (or not).s

                // Only grab a trigger if the state is still waiting (another scheduler hasn't grabbed it meanwhile)
                // See dynamo docs for more details on conditions http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
                UpdateItemOperationConfig conditionalUpdate = new UpdateItemOperationConfig();
                conditionalUpdate.ConditionalExpression = new Expression
                {
                    ExpressionStatement = "Name = :name and Group = :group and State = :state",
                    ExpressionAttributeValues =
                    {
                        [":name"] = trigger.Trigger.Name,
                        [":group"] = trigger.Trigger.Group,
                        [":state"] = "Waiting"
                    }
                };

                trigger.Trigger.FireInstanceId = this.GetFiredTriggerRecordId();
                trigger.SchedulerInstanceId = InstanceId;
                trigger.State = "Acquired";

                //Table triggerTable = Table.LoadTable(_client, DynamoConfiguration.TriggerTableName);
                //TODO: FIX ME
                //bool acquiredTrigger = triggerTable.TryUpdateItem((Document)(new TriggerConverter().ToEntry(trigger)), conditionalUpdate);

                //var acquired = this.Triggers.FindAndModify(
                //    Query.And(
                //        Query.EQ("_id", trigger.Key.ToBsonDocument()),
                //        Query.EQ("State", "Waiting")),
                //    SortBy.Null,
                //    Update.Set("State", "Acquired")
                //        .Set("SchedulerInstanceId", this.instanceId)
                //        .Set("FireInstanceId", trigger.FireInstanceId));

                if(true)
                //if (acquiredTrigger)
                {
                    result.Add(trigger.Trigger);

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = trigger.Trigger.GetNextFireTimeUtc();
                    }
                }

                if (result.Count == maxCount)
                {
                    break;
                }
            }


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

            return result;
        }

        /// <summary>
        /// Checks if the given triggers NextFireTime is older than now + the misfire threshold.
        /// If it is, applies the misfire and updates the record in the DB.
        /// TODO: come back and test this.
        /// </summary>
        /// <param name="trigger">The trigger.</param>
        /// <returns>True if the trigger misfired, false if it didn't.</returns>
        protected virtual bool ApplyMisfireIfNecessary(DynamoTrigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1*MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.Trigger.GetNextFireTimeUtc();
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.Trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                // If this trigger has no misfire instruction or the next fire time is within our misfire threshold.
                return false;
            }

            ICalendar cal = null;
            if (trigger.Trigger.CalendarName != null)
            {
                cal = this.RetrieveCalendar(trigger.Trigger.CalendarName);
            }

            _signaler.NotifyTriggerListenersMisfired(trigger.Trigger);

            trigger.Trigger.UpdateAfterMisfire(cal);
            this.StoreTrigger(trigger.Trigger, true);

            if (!trigger.Trigger.GetNextFireTimeUtc().HasValue)
            {
                trigger.State = "Complete";
                _context.Save(trigger);

                _signaler.NotifySchedulerListenersFinalized(trigger.Trigger);
            }
            else if (tnft.Equals(trigger.Trigger.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
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

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            get { return _misfireThreshold; }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("Misfirethreashold must be larger than 0");
                }
                _misfireThreshold = value;
            }
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
            get { return _instanceId; }
            set { this._instanceId = value; }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> of the Scheduler instance's name, 
        /// prior to initialize being invoked.
        /// </summary>
        public virtual string InstanceName
        {
            set
            {
                throw new NotImplementedException();
                //this._instanceName = value;
            }
        }

        public int ThreadPoolSize { get; set; }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _context.Dispose();
                }

                _disposedValue = true;
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