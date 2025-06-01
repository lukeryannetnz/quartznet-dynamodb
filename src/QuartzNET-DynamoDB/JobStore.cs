using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Amazon.DynamoDBv2.DataModel;
using Quartz.DynamoDB.DataModel;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using Amazon.DynamoDBv2.Model;
using Quartz.DynamoDB.DataModel.Storage;
using System.Diagnostics;
using Quartz.Impl.Triggers;
using System.Threading.Tasks;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// This class implements a <see cref="IJobStore" /> that
    /// utilizes Amazon DynamoDB as its storage device.
    /// <author>Luke Ryan</author>
    /// </summary>
    public class JobStore : IJobStore, IDisposable
    {
        private readonly DynamoBootstrapper _bootStrapper;
        private static readonly object LockObject = new object();
        private DynamoDBContext _context;
        private IRepository<DynamoJob> _jobRepository;
        private IRepository<DynamoJobGroup> _jobGroupRepository;
        private IRepository<DynamoTrigger> _triggerRepository;
        private IRepository<DynamoScheduler> _schedulerRepository;
        private IRepository<DynamoTriggerGroup> _triggerGroupRepository;
        private IRepository<DynamoCalendar> _calendarRepository;
        private string _instanceId;
        /// <summary>
        /// Tracks if dispose has been called to detect redundant (multiple) dispose calls.
        /// </summary>
        private bool _disposedValue = false;
        private TimeSpan _misfireThreshold;
        private ISchedulerSignaler _signaler;
        /// <summary>
        /// Allows for cleanup operations to be run periodically. Typically these are executed from
        /// within AcquireNextTriggers which is called in a tight loop by the scheduler. Typically
        /// they are expensive in terms of dynamo throughput too.
        /// </summary>
        private readonly Dictionary<string, PeriodicExecutionTracker> _periodicExecutionTrackers;

        public JobStore() : this(new DynamoBootstrapper())
        {
        }

        public JobStore(DynamoBootstrapper bootStrapper)
        {
            _bootStrapper = bootStrapper;
            _periodicExecutionTrackers = new Dictionary<string, PeriodicExecutionTracker>
            {
                {
                    "CreateOrUpdateCurrentSchedulerInstance",
                    new PeriodicExecutionTracker(TimeSpan.FromMinutes(5))
                },
                {
                    "ResetTriggersAssociatedWithNonActiveSchedulers",
                    new PeriodicExecutionTracker(TimeSpan.FromMinutes(10))
                }
            };
        }

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            
        }

        public Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                CreateOrUpdateCurrentSchedulerInstance();
            }
            return Task.CompletedTask;
        }

        public Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            var scheduler = _schedulerRepository.Load(DynamoScheduler.CreateKeyDictionary(InstanceId));
            scheduler.State = "Paused";
            _schedulerRepository.Store(scheduler);
            return Task.CompletedTask;
        }

        public Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            var scheduler = _schedulerRepository.Load(DynamoScheduler.CreateKeyDictionary(InstanceId));
            scheduler.State = "Resumed";
            _schedulerRepository.Store(scheduler);
            return Task.CompletedTask;
        }

        public Task Shutdown(CancellationToken cancellationToken = default)
        {
            Dispose();
            return Task.CompletedTask;
        }

        public Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                StoreJob(newJob, false).Wait(cancellationToken);
                StoreTrigger(newTrigger, false).Wait(cancellationToken);
            }
            return Task.CompletedTask;
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            var group = _jobGroupRepository.Load(new JobKey(string.Empty, groupName).ToGroupDictionary());
            if (group == null)
            {
                return Task.FromResult(false);
            }
            return Task.FromResult(group.State == DynamoJobGroupState.Paused);
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            var group = _triggerGroupRepository.Load(new TriggerKey(string.Empty, groupName).ToGroupDictionary());
            if (group == null)
            {
                return Task.FromResult(false);
            }
            return Task.FromResult(group.State == DynamoTriggerGroupState.Paused);
        }

        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                DynamoJob job = new DynamoJob(newJob);

                if (!replaceExisting && _jobRepository.Load(job.Key) != null)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }

                var jobGroup = this._jobGroupRepository.Load(newJob.Key.ToGroupDictionary());

                if (jobGroup == null)
                {
                    jobGroup = new DynamoJobGroup()
                    {
                        Name = newJob.Key.Group,
                        State = DynamoJobGroupState.Active
                    };

                    _jobGroupRepository.Store(jobGroup);
                }

                _jobRepository.Store(job);
            }
            return Task.CompletedTask;
        }

        Task IJobStore.StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken)
        {
            lock (LockObject)
            {
                // fail fast if there are collisions.
                // ensuring there will be no collisions upfront eliminates the need
                // to cleanup if a collision occurs part way through processing.
                if (!replace)
                {
                    foreach (var job in triggersAndJobs.Keys)
                    {
                        if (CheckExists(job.Key).Result)
                        {
                            throw new ObjectAlreadyExistsException(job);
                        }
                        foreach (var trigger in triggersAndJobs[job])
                        {
                            if (CheckExists(trigger.Key).Result)
                            {
                                throw new ObjectAlreadyExistsException(trigger);
                            }
                        }
                    }
                }

                foreach (var triggersAndJob in triggersAndJobs)
                {
                    StoreJob(triggersAndJob.Key, true).Wait(cancellationToken);
                    foreach (var trigger in triggersAndJob.Value)
                    {
                        StoreTrigger((IOperableTrigger)trigger, true).Wait(cancellationToken);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            bool found;
            lock (LockObject)
            {
                // keep separated to clean up any staled trigger
                IList<IOperableTrigger> triggersForJob = this.GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.RemoveTrigger(trigger.Key);
                }

                found = this.CheckExists(jobKey);
                if (found)
                {
                    _jobRepository.Delete(jobKey.ToDictionary());
                }
            }
            return Task.FromResult(found);
        }

        public Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            bool allFound = true;
            lock (LockObject)
            {
                foreach (JobKey key in jobKeys)
                {
                    allFound = RemoveJob(key).Result && allFound;
                }
            }
            return Task.FromResult(allFound);
        }

        public Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            IJobDetail result;
            lock (LockObject)
            {
                var job = _jobRepository.Load(jobKey.ToDictionary());
                result = job == null ? null : job.Job;
            }
            return Task.FromResult(result);
        }

        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                DynamoTrigger trigger = new DynamoTrigger(newTrigger);

                if (!replaceExisting && _triggerRepository.Load(trigger.Key) != null)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }

                var job = _jobRepository.Load(newTrigger.JobKey.ToDictionary());
                if (job == null || job.Job == null)
                {
                    throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
                }

                var triggerGroup = this._triggerGroupRepository.Load(newTrigger.Key.ToGroupDictionary());

                if (triggerGroup != null && triggerGroup.State == DynamoTriggerGroupState.Paused)
                {
                    trigger.State = DynamoTriggerState.Paused;
                }

                if (triggerGroup == null)
                {
                    triggerGroup = new DynamoTriggerGroup()
                    {
                        Name = newTrigger.Key.Group,
                        State = DynamoTriggerGroupState.Active
                    };

                    _triggerGroupRepository.Store(triggerGroup);
                }

                var jobGroup = this._jobGroupRepository.Load(newTrigger.JobKey.ToGroupDictionary());

                if (jobGroup != null && jobGroup.State == DynamoJobGroupState.Paused)
                {
                    trigger.State = DynamoTriggerState.Paused;
                }

                if (jobGroup == null)
                {
                    jobGroup = new DynamoJobGroup()
                    {
                        Name = newTrigger.JobKey.Group,
                        State = DynamoJobGroupState.Active
                    };

                    _jobGroupRepository.Store(jobGroup);
                }

                if (triggerGroup.State == DynamoTriggerGroupState.Paused
                   || jobGroup.State == DynamoJobGroupState.Paused)
                {
                    if (job.State == DynamoJobState.Blocked)
                    {
                        trigger.State = DynamoTriggerState.PausedAndBlocked;
                    }
                }
                else if (job.State == DynamoJobState.Blocked)
                {
                    trigger.State = DynamoTriggerState.Blocked;
                }

                _triggerRepository.Store(trigger);
            }
            return Task.CompletedTask;
        }

        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(RemoveTrigger(triggerKey, true));
        }

        private bool RemoveTrigger(TriggerKey triggerKey, bool removeOrphanedJob)
        {
            bool found;

            lock (LockObject)
            {
                var trigger = RetrieveTrigger(triggerKey);
                found = trigger != null;

                if (found)
                {
                    _triggerRepository.Delete(triggerKey.ToDictionary());

                    if (removeOrphanedJob)
                    {
                        IJobDetail jobDetail = RetrieveJob(trigger.JobKey);
                        if (jobDetail != null)
                        {
                            IList<IOperableTrigger> trigs = GetTriggersForJob(jobDetail.Key);
                            if ((trigs == null || trigs.Count == 0) && !jobDetail.Durable)
                            {
                                if (RemoveJob(jobDetail.Key))
                                {
                                    _signaler.NotifySchedulerListenersJobDeleted(jobDetail.Key);
                                }
                            }
                        }
                    }
                }
            }

            return found;
        }

        public Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = default)
        {
            bool allFound = true;
            lock (LockObject)
            {
                foreach (TriggerKey key in triggerKeys)
                {
                    allFound = RemoveTrigger(key).Result && allFound;
                }
            }
            return Task.FromResult(allFound);
        }

        public Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            bool result = false;
            lock (LockObject)
            {
                var record = _triggerRepository.Load(triggerKey.ToDictionary());

                if (record != null && record.Trigger != null)
                {
                    if (!record.Trigger.JobKey.Equals(newTrigger.JobKey))
                    {
                        throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                    }

                    // don't want the "orphaned" job removed in this case since the trigger is being replaced 
                    this.RemoveTrigger(triggerKey, false);

                    try
                    {
                        this.StoreTrigger(newTrigger, false);
                    }
                    catch (JobPersistenceException)
                    {
                        this.StoreTrigger(record.Trigger, false); // put previous trigger back...
                        throw;
                    }

                    result = true;
                }
            }
            return Task.FromResult(result);
        }

        public Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            IOperableTrigger result = null;
            lock (LockObject)
            {
                var trigger = _triggerRepository.Load(triggerKey.ToDictionary());
                result = trigger?.Trigger;
            }
            return Task.FromResult(result);
        }

        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            bool exists;
            lock (LockObject)
            {
                var key = new DynamoCalendar(calName).Key;
                exists = _calendarRepository.Load(key) != null;
            }
            return Task.FromResult(exists);
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                return Task.FromResult(_jobRepository.Load(jobKey.ToDictionary()) != null);
            }
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                return Task.FromResult(_triggerRepository.Load(triggerKey.ToDictionary()) != null);
            }
        }

        /// <summary>
        /// Clears (deletes!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
        /// <see cref="ICalendar"/>s.
        /// </summary>
        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                // unschedule jobs (delete triggers)
                _triggerRepository.DeleteTable();
                _triggerGroupRepository.DeleteTable();

                // delete jobs
                _jobRepository.DeleteTable();
                _jobGroupRepository.DeleteTable();

                // delete calendars
                _calendarRepository.DeleteTable();
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Store the given <see cref="ICalendar" />.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="calendar">The <see cref="ICalendar" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="ICalendar" /> existing
        /// in the <see cref="IJobStore" /> with the same name and group
        /// should be over-written.</param>
        /// <param name="updateTriggers">If <see langword="true" />, any <see cref="ITrigger" />s existing
        /// in the <see cref="IJobStore" /> that reference an existing
        /// Calendar with the same name with have their next fire time
        /// re-computed with the new <see cref="ICalendar" />.</param>
        public Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                var dynamoCal = new DynamoCalendar(name, calendar);

                var existingRecord = _calendarRepository.Load(dynamoCal.Key);

                if (existingRecord != null && replaceExisting == false)
                {
                    throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));
                }

                _calendarRepository.Store(dynamoCal);

                if (updateTriggers)
                {
                    var triggers = GetTriggersForCalendar(name);

                    foreach (var trigger in triggers)
                    {
                        trigger.Trigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        _triggerRepository.Store(trigger);
                    }
                }
            }
            return Task.CompletedTask;
        }

        private IEnumerable<DynamoTrigger> GetTriggersForCalendar(string calendarName)
        {
            //todo: this will be slow. do the query based on an index.
            var triggers = _triggerRepository.Scan(null, null, string.Empty).Where(t => t.Trigger.CalendarName == calendarName);
            return triggers;
        }

        public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            var triggers = this.GetTriggersForCalendar(calName);
            if (triggers != null && triggers.Count() > 0)
            {
                throw new JobPersistenceException("Calendar cannot be removed if it is referenced by a Trigger!");
            }

            var calendar = new DynamoCalendar() { Name = calName };
            _calendarRepository.Delete(calendar.Key);

            return Task.FromResult(true);
        }

        public Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            ICalendar result;
            var cal = new DynamoCalendar() { Name = calName };
            var calendar = _calendarRepository.Load(cal.Key);
            result = calendar.Calendar;
            return Task.FromResult(result);
        }

        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            var table = _jobRepository.DescribeTable();
            return Task.FromResult((int)table.Table.ItemCount);
        }

        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            var table = _triggerRepository.DescribeTable();
            return Task.FromResult((int)table.Table.ItemCount);
        }

        public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            var table = _calendarRepository.DescribeTable();
            return Task.FromResult((int)table.Table.ItemCount);
        }

        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            var jobGroupName = matcher.CompareToValue;
            var attributeNames = new Dictionary<string, string> {
                { "#jg", "Group" }
            };
            var attributeValues = new Dictionary<string, AttributeValue> {
                { ":Group", new AttributeValue { S = jobGroupName } }
            };
            var filterExpression = "#jg = :Group";
            var candidates = _jobRepository.Scan(attributeValues, attributeNames, filterExpression);
            return Task.FromResult((IReadOnlyCollection<JobKey>)new HashSet<JobKey>(candidates.Select(t => t.Job.Key)));
        }

        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            var triggerGroupName = matcher.CompareToValue;
            var attributeNames = new Dictionary<string, string> {
                { "#tg", "Group" }
            };
            var attributeValues = new Dictionary<string, AttributeValue> {
                { ":Group", new AttributeValue { S = triggerGroupName } }
            };
            var filterExpression = "#tg = :Group";
            var candidates = _triggerRepository.Scan(attributeValues, attributeNames, filterExpression);
            return Task.FromResult((IReadOnlyCollection<TriggerKey>)new HashSet<TriggerKey>(candidates.Select(t => t.Trigger.Key)));
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            var allJobGroups = this._jobGroupRepository.Scan(null, null, string.Empty);
            return Task.FromResult((IReadOnlyCollection<string>)allJobGroups.Select(jg => jg.Name).ToList());
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            var allTriggerGroups = this._triggerGroupRepository.Scan(null, null, string.Empty);
            return Task.FromResult((IReadOnlyCollection<string>)allTriggerGroups.Select(tg => tg.Name).ToList());
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            var allCalendars = this._calendarRepository.Scan(null, null, string.Empty);
            return Task.FromResult((IReadOnlyCollection<string>)allCalendars.Select(c => c.Name).ToList());
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var candidates = GetDynamoTriggersForJob(jobKey);
            return Task.FromResult((IReadOnlyCollection<IOperableTrigger>)candidates.Select(t => (IOperableTrigger)t.Trigger).ToList());
        }

        private IEnumerable<DynamoTrigger> GetDynamoTriggersForJob(JobKey jobKey)
        {
            var attributeNames = new Dictionary<string, string> {
                { "#jn", "JobName" },
                { "#jg", "JobGroup" }
            };

            var attributeValues = new Dictionary<string, AttributeValue> {
                { ":JobName", new AttributeValue { S = jobKey.Name } },
                { ":JobGroup", new AttributeValue { S = jobKey.Group } }
            };

            var filterExpression = "#jn = :JobName and #jg = :JobGroup";

            var candidates = _triggerRepository.Scan(attributeValues, attributeNames, filterExpression);

            return candidates;
        }

        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            TriggerState result;
            lock (LockObject)
            {
                var record = _triggerRepository.Load(triggerKey.ToDictionary());
                if (record == null)
                {
                    result = TriggerState.None;
                }
                else
                {
                    result = record.State.TriggerState;
                }
            }
            return Task.FromResult(result);
        }

        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            var record = _triggerRepository.Load(triggerKey.ToDictionary());
            if (record.TriggerState == TriggerState.Blocked)
            {
                record.State = DynamoTriggerState.PausedAndBlocked;
            }
            else
            {
                record.State = DynamoTriggerState.Paused;
            }
            _triggerRepository.Store(record);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            IList<string> pausedGroups = new List<string>();
            StringOperator op = matcher.CompareWithOperator;
            if (op == StringOperator.Equality)
            {
                PauseTriggerGroup(matcher.CompareToValue);
                pausedGroups.Add(matcher.CompareToValue);
            }
            else
            {
                IList<string> groups = this.GetTriggerGroupNames().Result.ToList();
                foreach (string group in groups)
                {
                    if (op.Evaluate(group, matcher.CompareToValue))
                    {
                        PauseTriggerGroup(matcher.CompareToValue);
                        pausedGroups.Add(matcher.CompareToValue);
                    }
                }
            }
            foreach (string pausedGroup in pausedGroups)
            {
                var keys = this.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(pausedGroup)).Result;
                foreach (TriggerKey key in keys)
                {
                    this.PauseTrigger(key);
                }
            }
            return Task.FromResult((IReadOnlyCollection<string>)new HashSet<string>(pausedGroups));
        }

        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = this.GetTriggersForJob(jobKey).Result;
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                this.PauseTrigger(trigger.Key);
            }
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            List<string> pausedGroups = new List<String>();
            StringOperator op = matcher.CompareWithOperator;
            if (op == StringOperator.Equality)
            {
                this.PauseJobGroup(matcher.CompareToValue);
                pausedGroups.Add(matcher.CompareToValue);
            }
            else
            {
                IList<string> groups = this.GetJobGroupNames().Result.ToList();
                foreach (string group in groups)
                {
                    if (op.Evaluate(group, matcher.CompareToValue))
                    {
                        this.PauseJobGroup(matcher.CompareToValue);
                        pausedGroups.Add(matcher.CompareToValue);
                    }
                }
            }
            foreach (string groupName in pausedGroups)
            {
                foreach (JobKey jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)).Result)
                {
                    IList<IOperableTrigger> triggers = this.GetTriggersForJob(jobKey).Result.ToList();
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        this.PauseTrigger(trigger.Key);
                    }
                }
            }
            return Task.FromResult((IReadOnlyCollection<string>)pausedGroups);
        }

        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            var record = _triggerRepository.Load(triggerKey.ToDictionary());
            if (record == null)
            {
                return Task.CompletedTask;
            }
            // if the trigger is not paused resuming it does not make sense...
            if (record.State != DynamoTriggerState.Paused &&
                record.State != DynamoTriggerState.PausedAndBlocked)
            {
                return Task.CompletedTask;
            }
            var job = _jobRepository.Load(record.Trigger.JobKey.ToDictionary());
            if (job != null && job.State == DynamoJobState.Blocked)
            {
                record.State = DynamoTriggerState.Blocked;
            }
            else
            {
                record.State = DynamoTriggerState.Waiting;
            }
            this.ApplyMisfireIfNecessaryAsync(record);
            _triggerRepository.Store(record);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            IList<string> resumedGroups = new List<string>();
            var op = matcher.CompareWithOperator;
            if (Equals(op, StringOperator.Equality))
            {
                ResumeTriggerGroup(matcher.CompareToValue);
                resumedGroups.Add(matcher.CompareToValue);
            }
            else
            {
                var groups = GetTriggerGroupNames().Result;
                foreach (var group in groups)
                {
                    if (op.Evaluate(group, matcher.CompareToValue))
                    {
                        ResumeTriggerGroup(matcher.CompareToValue);
                        resumedGroups.Add(matcher.CompareToValue);
                    }
                }
            }
            foreach (var resumedGroup in resumedGroups)
            {
                var keys = GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(resumedGroup)).Result;
                foreach (var key in keys)
                {
                    ResumeTrigger(key);
                }
            }
            return Task.FromResult((IReadOnlyCollection<string>)resumedGroups);
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            var expressionAttributeNames = new Dictionary<string, string> {
                    { "#S", "State" }
                };
            var expressionAttributeValues = new Dictionary<string, AttributeValue> {
                { ":PausedState", new AttributeValue { S = DynamoTriggerGroupState.Paused.ToString() } }
                };
            var filterExpression = "#S = :PausedState";
            var results = _triggerGroupRepository.Scan(expressionAttributeValues, expressionAttributeNames, filterExpression);
            return Task.FromResult((IReadOnlyCollection<string>)new HashSet<string>(results.Select(o => o.Name).ToList()));
        }

        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey).Result.ToList();
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.ResumeTrigger(trigger.Key);
                }
            }
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            var resumedGroups = new List<String>();
            var op = matcher.CompareWithOperator;
            if (Equals(op, StringOperator.Equality))
            {
                ResumeJobGroup(matcher.CompareToValue);
                resumedGroups.Add(matcher.CompareToValue);
            }
            else
            {
                var groups = GetJobGroupNames().Result;
                foreach (var @group in groups.Where(@group => op.Evaluate(@group, matcher.CompareToValue)))
                {
                    ResumeJobGroup(matcher.CompareToValue);
                    resumedGroups.Add(matcher.CompareToValue);
                }
            }
            foreach (var groupName in resumedGroups)
            {
                foreach (var jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)).Result)
                {
                    var triggers = GetTriggersForJob(jobKey).Result;
                    foreach (var trigger in triggers)
                    {
                        ResumeTrigger(trigger.Key);
                    }
                }
            }
            return Task.FromResult((IReadOnlyCollection<string>)new HashSet<string>(resumedGroups));
        }

        public Task PauseAll(CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                var triggerGroupNames = GetTriggerGroupNames().Result;
                foreach (var groupName in triggerGroupNames)
                {
                    PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
            return Task.CompletedTask;
        }

        public Task ResumeAll(CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                var triggerGroupNames = GetTriggerGroupNames().Result;
                foreach (var groupName in triggerGroupNames)
                {
                    ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
            return Task.CompletedTask;
        }

         /// <summary>
        /// A counter for fired trigger records.
        /// A unique value that initialises as the UTC ticks when the application initialises.
        /// This is incremented by the GetFiredTriggerRecordId method.
        /// </summary>
        private static long _ftrCtr = SystemTime.UtcNow().Ticks;

        /// <summary>
        /// Gets a unique fired trigger record id.
        /// </summary>
        /// <returns>The unique fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            long value = Interlocked.Increment(ref _ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }


        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            List<DynamoTrigger> candidates;
            ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            lock (LockObject)
            {
                Debug.WriteLine("Acquiring triggers. No later than: {0}, timewindow: {1}", noLaterThan, timeWindow);
                CreateOrUpdateCurrentSchedulerInstance();
                ResetTriggersAssociatedWithNonActiveSchedulers();
                
                string maxNextFireTime = (noLaterThan + timeWindow).UtcDateTime.ToUnixEpochTime().ToString();
                var candidateExpressionAttributeNames = new Dictionary<string, string> {
                    { "#S", "State" }
                };
                var candidateExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    { ":WaitingState", new AttributeValue { N = DynamoTriggerState.Waiting.InternalValue.ToString() } },
                    { ":MaxNextFireTime", new AttributeValue { N = maxNextFireTime } }
                };
                var candidateFilterExpression = "#S = :WaitingState and NextFireTimeUtcEpoch <= :MaxNextFireTime";
                candidates = _triggerRepository.Scan(candidateExpressionAttributeValues, candidateExpressionAttributeNames, candidateFilterExpression)
                    .OrderBy(t => t.Trigger.GetNextFireTimeUtc()).ThenByDescending(t => t.Trigger.Priority)
                    .ToList();
            }

            foreach (var trigger in candidates)
            {
                Debug.WriteLine("Processing candidate. Name: {0} Next fire time: {1}", trigger.Trigger.Name, trigger.Trigger.GetNextFireTimeUtc());
                if (trigger.Trigger.GetNextFireTimeUtc() == null)
                {
                    Debug.WriteLine("Candidate has no next fire time. Excluding.");
                    continue;
                }

                if (firstAcquiredTriggerFireTime != null
                    && trigger.Trigger.GetNextFireTimeUtc() > (firstAcquiredTriggerFireTime.Value + timeWindow))
                {
                    Debug.WriteLine("Breaking, have hit trigger beyond the time window.");
                    break;
                }

                if (await ApplyMisfireIfNecessaryAsync(trigger))
                {
                    Debug.WriteLine("Applied misfire. Next fire time: {0}", trigger.Trigger.GetNextFireTimeUtc());
                    if (trigger.Trigger.GetNextFireTimeUtc() == null
                        || trigger.Trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                    {
                        Debug.WriteLine("Continuing. No next fire time, or fire time outside of window.");
                        continue;
                    }
                }

                JobKey jobKey = trigger.Trigger.JobKey;
                IJobDetail job = await RetrieveJob(jobKey);
                if (job.ConcurrentExecutionDisallowed)
                {
                    if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                    {
                        Debug.WriteLine("Continuing. Added non-concurrent trigger twice.");
                        continue;
                    }
                    else
                    {
                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }
                }

                bool acquired = false;
                lock (LockObject)
                {
                    var acquireTriggerConditionalExpressionAttributeNames = new Dictionary<string, string> {
                        { "#S", "State" },
                        { "#N", "Name" },
                        { "#G", "Group" }
                    };
                    var acquireTriggerConditionalExpression = "#N = :name and #G = :group and #S = :state";
                    Dictionary<string, AttributeValue> acquireTriggerExpressionAttributeValues = new Dictionary<string, AttributeValue>() {
                        { ":name", new AttributeValue () { S = trigger.Trigger.Name } },
                        { ":group", new AttributeValue () { S = trigger.Trigger.Group } },
                        { ":state", new AttributeValue () { N = DynamoTriggerState.Waiting.InternalValue.ToString() } }
                    };
                    trigger.Trigger.FireInstanceId = this.GetFiredTriggerRecordId();
                    trigger.SchedulerInstanceId = InstanceId;
                    trigger.State = DynamoTriggerState.Acquired;
                    Debug.WriteLine("Acquiring the trigger.");
                    var acquiredTrigger = _triggerRepository.Store(trigger, acquireTriggerExpressionAttributeValues, acquireTriggerConditionalExpressionAttributeNames, acquireTriggerConditionalExpression);
                    acquired = acquiredTrigger.Any();
                }

                if (acquired)
                {
                    Debug.WriteLine("Acquired the trigger.");
                    result.Add(trigger.Trigger);
                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = trigger.Trigger.GetNextFireTimeUtc();
                    }
                }

                if (result.Count == maxCount)
                {
                    Debug.WriteLine("Hit the max count.");
                    break;
                }
            }

            return result;
        }

        /// <summary>
        /// Checks if the given triggers NextFireTime is older than now + the misfire threshold.
        /// If it is, applies the misfire and updates the record in the DB.
        /// TODO: come back and test this.
        /// </summary>
        /// <param name="trigger">The trigger.</param>
        /// <returns>True if the trigger misfired, false if it didn't.</returns>
        protected virtual async Task<bool> ApplyMisfireIfNecessaryAsync(DynamoTrigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
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
                cal = await this.RetrieveCalendar(trigger.Trigger.CalendarName);
            }

            await _signaler.NotifyTriggerListenersMisfired(trigger.Trigger);
            Debug.WriteLine("Misfired. Time now: {0}. Trigger fire time: {1}", misfireTime.Ticks, tnft.Value.Ticks);

            trigger.Trigger.UpdateAfterMisfire(cal);
            await this.StoreTrigger(trigger.Trigger, true);

            if (!trigger.Trigger.GetNextFireTimeUtc().HasValue)
            {
                trigger.State = DynamoTriggerState.Complete;
                await this.StoreTrigger(trigger.Trigger, true);

                await _signaler.NotifySchedulerListenersFinalized(trigger.Trigger);
            }
            else if (tnft.Equals(trigger.Trigger.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = default)
        {
            var t = _triggerRepository.Load(trigger.Key.ToDictionary());
            t.SchedulerInstanceId = string.Empty;
            t.State = DynamoTriggerState.Waiting;
            _triggerRepository.Store(t);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            List<TriggerFiredResult> results = new List<TriggerFiredResult>();
            lock (LockObject)
            {
                foreach (IOperableTrigger trigger in triggers)
                {
                    var storedTrigger = _triggerRepository.Load(trigger.Key.ToDictionary());
                    if (storedTrigger == null)
                    {
                        continue;
                    }
                    if (storedTrigger.State != DynamoTriggerState.Acquired)
                    {
                        continue;
                    }
                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = this.RetrieveCalendar(trigger.CalendarName).Result;
                        if (cal == null)
                        {
                            continue;
                        }
                    }
                    DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();
                    Debug.WriteLine("Triggering Trigger! Previous Fire Time: {0}. Next Fire Time: {1}.  Calendar: {2}.", trigger.GetPreviousFireTimeUtc(), trigger.GetNextFireTimeUtc(), trigger.CalendarName);
                    trigger.Triggered(cal);
                    Debug.WriteLine("Triggered Trigger! Previous Fire Time: {0}. Next Fire Time: {1}.", trigger.GetPreviousFireTimeUtc(), trigger.GetNextFireTimeUtc());
                    storedTrigger.Trigger = (AbstractTrigger)trigger;
                    storedTrigger.State = DynamoTriggerState.Executing;
                    _triggerRepository.Store(storedTrigger);
                    var storedJob = _jobRepository.Load(trigger.JobKey.ToDictionary());
                    TriggerFiredBundle bndle = new TriggerFiredBundle(storedJob.Job,
                                                  trigger,
                                                  cal,
                                                  false,
                                                  SystemTime.UtcNow(),
                                                  trigger.GetPreviousFireTimeUtc(),
                                                  prevFireTime,
                                                  trigger.GetNextFireTimeUtc());
                    IJobDetail job = bndle.JobDetail;
                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var triggersForJob = this.GetDynamoTriggersForJob(job.Key);
                        foreach (var jobTrigger in triggersForJob)
                        {
                            if (jobTrigger.State == DynamoTriggerState.Waiting)
                            {
                                jobTrigger.State = DynamoTriggerState.Blocked;
                            }
                            if (jobTrigger.State == DynamoTriggerState.Paused)
                            {
                                jobTrigger.State = DynamoTriggerState.PausedAndBlocked;
                            }
                            _triggerRepository.Store(jobTrigger);
                        }
                        storedJob.State = DynamoJobState.Blocked;
                        _jobRepository.Store(storedJob);
                    }
                    results.Add(new TriggerFiredResult(bndle));
                }
                return Task.FromResult((IReadOnlyCollection<TriggerFiredResult>)results);
            }
        }

        public Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            this.ReleaseAcquiredTrigger(trigger);
            var storedJob = _jobRepository.Load(jobDetail.Key.ToDictionary());
            if (jobDetail.PersistJobDataAfterExecution)
            {
                storedJob.Job = jobDetail;
                _jobRepository.Store(storedJob);
            }
            if (jobDetail.ConcurrentExecutionDisallowed)
            {
                var triggersForJob = this.GetDynamoTriggersForJob(jobDetail.Key);
                foreach (var jobTrigger in triggersForJob)
                {
                    if (jobTrigger.State == DynamoTriggerState.Blocked)
                    {
                        jobTrigger.State = DynamoTriggerState.Waiting;
                    }
                    if (jobTrigger.State == DynamoTriggerState.PausedAndBlocked)
                    {
                        jobTrigger.State = DynamoTriggerState.Waiting;
                    }
                    _triggerRepository.Store(jobTrigger);
                }
                _signaler.SignalSchedulingChange(null);
            }
            if (storedJob.State == DynamoJobState.Blocked)
            {
                storedJob.State = DynamoJobState.Active;
                _jobRepository.Store(storedJob);
            }
            if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
            {
                Debug.WriteLine("Deleting trigger");
                DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                if (!d.HasValue)
                {
                    d = trigger.GetNextFireTimeUtc();
                    if (!d.HasValue)
                    {
                        this.RemoveTrigger(trigger.Key);
                    }
                    else
                    {
                        Debug.WriteLine("Deleting cancelled - trigger still active");
                    }
                }
                else
                {
                    this.RemoveTrigger(trigger.Key);
                    _signaler.SignalSchedulingChange(null);
                }
            }
            else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
            {
                var record = _triggerRepository.Load(trigger.Key.ToDictionary());
                record.State = DynamoTriggerState.Complete;
                _triggerRepository.Store(record);
                _signaler.SignalSchedulingChange(null);
            }
            else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
            {
                Debug.WriteLine(string.Format(CultureInfo.InvariantCulture, "Trigger {0} set to ERROR state.", trigger.Key));
                var record = _triggerRepository.Load(trigger.Key.ToDictionary());
                record.State = DynamoTriggerState.Error;
                _triggerRepository.Store(record);
                _signaler.SignalSchedulingChange(null);
            }
            else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
            {
                Debug.WriteLine(string.Format(CultureInfo.InvariantCulture, "All triggers of Job {0} set to ERROR state.", trigger.JobKey));
                IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key).Result.ToList();
                SetStateOfTriggers(jobTriggers, DynamoTriggerState.Error);
                _signaler.SignalSchedulingChange(null);
            }
            else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
            {
                IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key).Result.ToList();
                SetStateOfTriggers(jobTriggers, DynamoTriggerState.Complete);
                _signaler.SignalSchedulingChange(null);
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Takes a collection of quartz trigger objects and sets their dynamo state.
        /// </summary>
        /// <param name="triggers">The triggers to update</param>
        /// <param name="state">The state to set.</param>
        private void SetStateOfTriggers(IList<Spi.IOperableTrigger> triggers, DynamoTriggerState state)
        {
            List<DynamoTrigger> dynamoTriggers = new List<DynamoTrigger>();

            for (int i = 0; i < triggers.Count; i++)
            {
                var record = _triggerRepository.Load(triggers[i].Key.ToDictionary());
                record.State = state;
                dynamoTriggers.Add(record);
            }

            _triggerRepository.Store(dynamoTriggers);
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
                if (value.TotalMilliseconds < 0)
                {
                    throw new ArgumentException("Misfirethreshold must be larger than 0");
                }
                _misfireThreshold = value;
            }
        }

        public bool SupportsPersistence { get { return true; } }

        public long EstimatedTimeToReleaseAndAcquireTrigger { get { return 100; } }

        public bool Clustered { get { return true; } }

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
                DynamoConfiguration.InstanceName = value;
            }
        }

        public int ThreadPoolSize { get; set; }

        /// <summary>
        /// Creates a Scheduler record for the current instance id that expires in 10 minutes. 
        /// Or if a record already exists for the id, updates the Expires time to 10 minutes from now. 
        /// 
        /// Always sets the scheduler state to Running.
        /// </summary>
        private void CreateOrUpdateCurrentSchedulerInstance()
        {
            if (!_periodicExecutionTrackers["CreateOrUpdateCurrentSchedulerInstance"].ShouldExecute())
            {
                Debug.WriteLine("Skipping execution of CreateOrUpdateCurrentSchedulerInstance");
                return;
            }

            Debug.WriteLine("Executing CreateOrUpdateCurrentSchedulerInstance");

            var scheduler = new DynamoScheduler
            {
                InstanceId = _instanceId,
                ExpiresUtc = (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime,
                State = "Running"
            };

            _schedulerRepository.Store(scheduler);
        }

        /// <summary>
        /// Reset the state of any triggers that are associated with non-active schedulers.
        /// </summary>
        private void ResetTriggersAssociatedWithNonActiveSchedulers()
        {
            if (!_periodicExecutionTrackers["ResetTriggersAssociatedWithNonActiveSchedulers"].ShouldExecute())
            {
                Debug.WriteLine("Skipping execution of ResetTriggersAssociatedWithNonActiveSchedulers");
                return;
            }

            Debug.WriteLine("Executing ResetTriggersAssociatedWithNonActiveSchedulers");

            var activeSchedulers = _schedulerRepository.Scan(null, null, string.Empty);

            //todo: this will be slow. do the query based on an index.
            foreach (var trigger in _triggerRepository.Scan(null, null, string.Empty))
            {
                if (!string.IsNullOrEmpty(trigger.SchedulerInstanceId) && !activeSchedulers.Select(s => s.InstanceId).Contains(trigger.SchedulerInstanceId))
                {
                    trigger.SchedulerInstanceId = string.Empty;
                    trigger.State = DynamoTriggerState.Waiting;
                    _triggerRepository.Store(trigger);
                }
            }
        }

        public Task ResetTriggerFromErrorState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            lock (LockObject)
            {
                var record = _triggerRepository.Load(triggerKey.ToDictionary());
                if (record != null && record.State == DynamoTriggerState.Error)
                {
                    record.State = DynamoTriggerState.Waiting;
                    _triggerRepository.Store(record);
                    _signaler.SignalSchedulingChange(null);
                }
            }
            return Task.CompletedTask;
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    if (_context != null)
                    {
                        _context.Dispose();
                    }

                    if (_jobRepository != null)
                    {
                        _jobRepository.Dispose();
                    }

                    if (_jobGroupRepository != null)
                    {
                        _jobGroupRepository.Dispose();
                    }

                    if (_triggerRepository != null)
                    {
                        _triggerRepository.Dispose();
                    }

                    if (_triggerGroupRepository != null)
                    {
                        _triggerGroupRepository.Dispose();
                    }

                    if (_calendarRepository != null)
                    {
                        _calendarRepository.Dispose();
                    }

                    if (_schedulerRepository != null)
                    {
                        _schedulerRepository.Dispose();
                    }
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

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken cancellationToken = default)
        {
            if (loadHelper == null)
            {
                throw new ArgumentNullException(nameof(loadHelper));
            }
            if (signaler == null)
            {
                throw new ArgumentNullException(nameof(signaler));
            }

            var client = DynamoDbClientFactory.Create();
            _context = new DynamoDBContext(client, new DynamoDBContextConfig());
            _jobRepository = new Repository<DynamoJob>(client);
            _jobGroupRepository = new Repository<DynamoJobGroup>(client);
            _triggerRepository = new Repository<DynamoTrigger>(client);
            _schedulerRepository = new Repository<DynamoScheduler>(client);
            _triggerGroupRepository = new Repository<DynamoTriggerGroup>(client);
            _calendarRepository = new Repository<DynamoCalendar>(client);

            lock (LockObject)
            {
                _bootStrapper.BootStrap(client);

                //_loadHelper = loadHelper;
                _signaler = signaler;

                // We should have had an instance id assigned by now, but if we haven't assign one.
                if (string.IsNullOrEmpty(InstanceId))
                {
                    InstanceId = Guid.NewGuid().ToString();
                }
            }
            return Task.CompletedTask;
        }

        #endregion

    }
}