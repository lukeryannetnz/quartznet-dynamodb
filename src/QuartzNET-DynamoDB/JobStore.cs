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

namespace Quartz.DynamoDB
{
    /// <summary>
    /// This class implements a <see cref="IJobStore" /> that
    /// utilizes Amazon DynamoDB as its storage device.
    /// <author>Luke Ryan</author>
    /// </summary>
    public class JobStore : IJobStore, IDisposable
    {
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

            var client = DynamoDbClientFactory.Create();
            _context = new DynamoDBContext(client);
            _jobRepository = new Repository<DynamoJob>(client);
            _jobGroupRepository = new Repository<DynamoJobGroup>(client);
            _triggerRepository = new Repository<DynamoTrigger>(client);
            _schedulerRepository = new Repository<DynamoScheduler>(client);
            _triggerGroupRepository = new Repository<DynamoTriggerGroup>(client);
            _calendarRepository = new Repository<DynamoCalendar>(client);

            lock (LockObject)
            {
                new DynamoBootstrapper().BootStrap(client);

                //_loadHelper = loadHelper;
                _signaler = signaler;

                // We should have had an instance id assigned by now, but if we haven't assign one.
                if (string.IsNullOrEmpty(InstanceId))
                {
                    InstanceId = Guid.NewGuid().ToString();
                }
            }
        }

        public void SchedulerStarted()
        {
            lock (LockObject)
            {
                CreateOrUpdateCurrentSchedulerInstance();
            }
        }

        public void SchedulerPaused()
        {
            var scheduler = _schedulerRepository.Load(DynamoScheduler.CreateKeyDictionary(InstanceId));

            scheduler.State = "Paused";

            _schedulerRepository.Store(scheduler);
        }

        public void SchedulerResumed()
        {
            var scheduler = _schedulerRepository.Load(DynamoScheduler.CreateKeyDictionary(InstanceId));

            scheduler.State = "Resumed";

            _schedulerRepository.Store(scheduler);
        }

        public void Shutdown()
        {
            // todo: remove scheduler instance from db?
            Dispose();
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            lock (LockObject)
            {
                StoreJob(newJob, false);
                StoreTrigger(newTrigger, false);
            }
        }

        public bool IsJobGroupPaused(string groupName)
        {
            var group = _jobGroupRepository.Load(new JobKey(string.Empty, groupName).ToGroupDictionary());

            if (group == null)
            {
                return false;
            }

            return group.State == DynamoJobGroupState.Paused;
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            var group = _triggerGroupRepository.Load(new TriggerKey(string.Empty, groupName).ToGroupDictionary());

            if (group == null)
            {
                return false;
            }

            return group.State == DynamoTriggerGroupState.Paused;
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
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
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs,
                                         bool replace)
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
                        if (CheckExists(job.Key))
                        {
                            throw new ObjectAlreadyExistsException(job);
                        }
                        foreach (var trigger in triggersAndJobs[job])
                        {
                            if (CheckExists(trigger.Key))
                            {
                                throw new ObjectAlreadyExistsException(trigger);
                            }
                        }
                    }
                }

                foreach (var triggersAndJob in triggersAndJobs)
                {
                    StoreJob(triggersAndJob.Key, true);
                    foreach (var trigger in triggersAndJob.Value)
                    {
                        StoreTrigger((IOperableTrigger)trigger, true);
                    }
                }
            }
        }

        public bool RemoveJob(JobKey jobKey)
        {
            lock (LockObject)
            {
                // keep separated to clean up any staled trigger
                IList<IOperableTrigger> triggersForJob = this.GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.RemoveTrigger(trigger.Key);
                }

                var found = this.CheckExists(jobKey);
                if (found)
                {
                    _jobRepository.Delete(jobKey.ToDictionary());
                }

                return found;
            }
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            bool allFound = true;

            lock (LockObject)
            {
                foreach (JobKey key in jobKeys)
                {
                    allFound = RemoveJob(key) && allFound;
                }
            }

            return allFound;
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            lock (LockObject)
            {
                var job = _jobRepository.Load(jobKey.ToDictionary());

                return job == null ? null : job.Job;
            }
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
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
                    throw new JobPersistenceException("The job (" + newTrigger.JobKey +
                    ") referenced by the trigger does not exist.");
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
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            bool found;

            lock (LockObject)
            {
                var trigger = this.RetrieveTrigger(triggerKey);
                found = trigger != null;

                if (found)
                {
                    _triggerRepository.Delete(triggerKey.ToDictionary());

                    //todo: support orphaned job removal
                    //					if (removeOrphanedJob)
                    //					{
                    //						IJobDetail jobDetail = this.RetrieveJob(trigger.JobKey);
                    //						IList<IOperableTrigger> trigs = this.GetTriggersForJob(jobDetail.Key);
                    //						if ((trigs == null
                    //							|| trigs.Count == 0)
                    //							&& !jobDetail.Durable)
                    //						{
                    //							if (this.RemoveJob(jobDetail.Key))
                    //							{
                    //								signaler.NotifySchedulerListenersJobDeleted(jobDetail.Key);
                    //							}
                    //						}
                    //					}
                }
            }

            return found;
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            bool allFound = true;

            lock (LockObject)
            {
                foreach (TriggerKey key in triggerKeys)
                {
                    allFound = RemoveTrigger(key) && allFound;
                }
            }

            return allFound;
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            lock (LockObject)
            {
                var record = _triggerRepository.Load(triggerKey.ToDictionary());

                if (record != null && record.Trigger != null)
                {
                    if (!record.Trigger.JobKey.Equals(newTrigger.JobKey))
                    {
                        throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                    }

                    this.RemoveTrigger(triggerKey);

                    try
                    {
                        this.StoreTrigger(newTrigger, false);
                    }
                    catch (JobPersistenceException)
                    {
                        this.StoreTrigger(record.Trigger, false); // put previous trigger back...
                        throw;
                    }

                    return true;
                }
            }

            return false;
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            lock (LockObject)
            {
                var trigger = _triggerRepository.Load(triggerKey.ToDictionary());

                return trigger?.Trigger;
            }
        }

        public bool CalendarExists(string calName)
        {
            lock (LockObject)
            {
                var key = new DynamoCalendar(calName).Key;

                return _calendarRepository.Load(key) != null;
            }
        }

        public bool CheckExists(JobKey jobKey)
        {
            lock (LockObject)
            {
                return _jobRepository.Load(jobKey.ToDictionary()) != null;
            }
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            lock (LockObject)
            {
                return _triggerRepository.Load(triggerKey.ToDictionary()) != null;
            }
        }

        /// <summary>
        /// Clears (deletes!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
        /// <see cref="ICalendar"/>s.
        /// </summary>
        public void ClearAllSchedulingData()
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
        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
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
        }

        private IEnumerable<DynamoTrigger> GetTriggersForCalendar(string calendarName)
        {
            //todo: this will be slow. do the query based on an index.
            var triggers = _triggerRepository.Scan(null, null, string.Empty).Where(t => t.Trigger.CalendarName == calendarName);
            return triggers;
        }

        public bool RemoveCalendar(string calName)
        {
            var triggers = this.GetTriggersForCalendar(calName);
            if (triggers != null && triggers.Count() > 0)
            {
                throw new JobPersistenceException("Calendar cannot be removed if it is referenced by a Trigger!");
            }

            var calendar = new DynamoCalendar() { Name = calName };
            _calendarRepository.Delete(calendar.Key);

            return true;
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            var cal = new DynamoCalendar() { Name = calName };
            var calendar = _calendarRepository.Load(cal.Key);

            return calendar.Calendar;
        }

        public int GetNumberOfJobs()
        {
            var table = _jobRepository.DescribeTable();

            return (int)table.Table.ItemCount;
        }

        public int GetNumberOfTriggers()
        {
            var table = _triggerRepository.DescribeTable();

            return (int)table.Table.ItemCount;
        }

        public int GetNumberOfCalendars()
        {
            var table = _calendarRepository.DescribeTable();

            return (int)table.Table.ItemCount;
        }

        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
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

            return new Collection.HashSet<JobKey>(candidates.Select(t => t.Job.Key));
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
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

            return new Collection.HashSet<TriggerKey>(candidates.Select(t => t.Trigger.Key));
        }

        public IList<string> GetJobGroupNames()
        {
            var allJobGroups = this._jobGroupRepository.Scan(null, null, string.Empty);

            return allJobGroups.Select(jg => jg.Name).ToList();
        }

        public IList<string> GetTriggerGroupNames()
        {
            var allTriggerGroups = this._triggerGroupRepository.Scan(null, null, string.Empty);

            return allTriggerGroups.Select(tg => tg.Name).ToList();
        }

        public IList<string> GetCalendarNames()
        {
            //todo: surely there is a better way to do this than scanning?
            var allCalendars = this._calendarRepository.Scan(null, null, string.Empty);

            return allCalendars.Select(c => c.Name).ToList();
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            var candidates = GetDynamoTriggersForJob(jobKey);

            return candidates.Select(t => (IOperableTrigger)t.Trigger).ToList();
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

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            lock (LockObject)
            {
                var record = _triggerRepository.Load(triggerKey.ToDictionary());

                if (record == null)
                {
                    return TriggerState.None;
                }

                return record.State.TriggerState;
            }
        }

        public void PauseTrigger(TriggerKey triggerKey)
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
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
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
                IList<string> groups = this.GetTriggerGroupNames();

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
                Collection.ISet<TriggerKey> keys = this.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(pausedGroup));

                foreach (TriggerKey key in keys)
                {
                    this.PauseTrigger(key);
                }
            }

            return new Collection.HashSet<string>(pausedGroups);
        }

        private void PauseTriggerGroup(string groupName)
        {
            var triggerGroup = this._triggerGroupRepository.Load(new TriggerKey(string.Empty, groupName).ToGroupDictionary());
            if (triggerGroup == null)
            {
                triggerGroup = new DynamoTriggerGroup()
                {
                    Name = groupName
                };
            }
            triggerGroup.State = DynamoTriggerGroupState.Paused;
            this._triggerGroupRepository.Store(triggerGroup);
        }

        private void ResumeTriggerGroup(string groupName)
        {
            var triggerGroup = _triggerGroupRepository.Load(new TriggerKey(string.Empty, groupName).ToGroupDictionary());
            if (triggerGroup == null)
            {
                triggerGroup = new DynamoTriggerGroup()
                {
                    Name = groupName
                };
            }
            triggerGroup.State = DynamoTriggerGroupState.Active;
            _triggerGroupRepository.Store(triggerGroup);
        }

        public void PauseJob(JobKey jobKey)
        {
            IList<IOperableTrigger> triggersForJob = this.GetTriggersForJob(jobKey);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                this.PauseTrigger(trigger.Key);
            }
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
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
                IList<string> groups = this.GetJobGroupNames();

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
                foreach (JobKey jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
                {
                    IList<IOperableTrigger> triggers = this.GetTriggersForJob(jobKey);
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        this.PauseTrigger(trigger.Key);
                    }
                }
            }

            return pausedGroups;
        }

        private void PauseJobGroup(string groupName)
        {
            var jobGroup = this._jobGroupRepository.Load(new JobKey(string.Empty, groupName).ToGroupDictionary());
            if (jobGroup == null)
            {
                jobGroup = new DynamoJobGroup()
                {
                    Name = groupName
                };
            }
            jobGroup.State = DynamoJobGroupState.Paused;
            this._jobGroupRepository.Store(jobGroup);
        }
        private void ResumeJobGroup(string groupName)
        {
            var jobGroup = _jobGroupRepository.Load(new JobKey(string.Empty, groupName).ToGroupDictionary());
            if (jobGroup == null)
            {
                jobGroup = new DynamoJobGroup()
                {
                    Name = groupName
                };
            }
            jobGroup.State = DynamoJobGroupState.Active;
            _jobGroupRepository.Store(jobGroup);
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            var record = _triggerRepository.Load(triggerKey.ToDictionary());

            if (record == null)
            {
                return;
            }

            // if the trigger is not paused resuming it does not make sense...
            if (record.State != DynamoTriggerState.Paused &&
                record.State != DynamoTriggerState.PausedAndBlocked)
            {
                return;
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

            this.ApplyMisfireIfNecessary(record);

            _triggerRepository.Store(record);
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
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
                var groups = GetTriggerGroupNames();

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
                var keys = GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(resumedGroup));

                foreach (var key in keys)
                {
                    ResumeTrigger(key);
                }
            }

            return resumedGroups;
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            var expressionAttributeNames = new Dictionary<string, string> {
                    { "#S", "State" }
                };

            var expressionAttributeValues = new Dictionary<string, AttributeValue> {
                { ":PausedState", new AttributeValue { S = DynamoTriggerGroupState.Paused.ToString() } }
                };

            var filterExpression = "#S = :PausedState";

            var results = _triggerGroupRepository.Scan(expressionAttributeValues, expressionAttributeNames, filterExpression);

            return new Collection.HashSet<string>(results.Select(o => o.Name).ToList());
        }

        public void ResumeJob(JobKey jobKey)
        {
            lock (LockObject)
            {
                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    this.ResumeTrigger(trigger.Key);
                }
            }
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
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
                var groups = GetJobGroupNames();

                foreach (var @group in groups.Where(@group => op.Evaluate(@group, matcher.CompareToValue)))
                {
                    ResumeJobGroup(matcher.CompareToValue);
                    resumedGroups.Add(matcher.CompareToValue);
                }
            }

            foreach (var groupName in resumedGroups)
            {
                foreach (var jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
                {
                    var triggers = GetTriggersForJob(jobKey);
                    foreach (var trigger in triggers)
                    {
                        ResumeTrigger(trigger.Key);
                    }
                }
            }

            return new Collection.HashSet<string>(resumedGroups);
        }

        public void PauseAll()
        {
            lock (LockObject)
            {
                var triggerGroupNames = GetTriggerGroupNames();

                foreach (var groupName in triggerGroupNames)
                {
                    PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        public void ResumeAll()
        {
            lock (LockObject)
            {
                var triggerGroupNames = GetTriggerGroupNames();

                foreach (var groupName in triggerGroupNames)
                {
                    ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
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

        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            lock (LockObject)
            {
                Debug.WriteLine("Acquiring triggers. No later than: {0}, timewindow: {1}", noLaterThan, timeWindow);

                // multiple instance management. Create a running scheduler for this instance.
                CreateOrUpdateCurrentSchedulerInstance();

                DeleteExpiredSchedulers();
                ResetTriggersAssociatedWithNonActiveSchedulers();

                List<IOperableTrigger> result = new List<IOperableTrigger>();
                Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
                DateTimeOffset? firstAcquiredTriggerFireTime = null;

                string maxNextFireTime = (noLaterThan + timeWindow).UtcDateTime.ToUnixEpochTime().ToString();

                var candidateExpressionAttributeNames = new Dictionary<string, string> {
                    { "#S", "State" }
                };

                var candidateExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    { ":WaitingState", new AttributeValue { N = DynamoTriggerState.Waiting.InternalValue.ToString() } },
                    { ":MaxNextFireTime", new AttributeValue { N = maxNextFireTime } }
                };

                var candidateFilterExpression = "#S = :WaitingState and NextFireTimeUtcEpoch <= :MaxNextFireTime";

                var candidates = _triggerRepository.Scan(candidateExpressionAttributeValues, candidateExpressionAttributeNames, candidateFilterExpression)
                .OrderBy(t => t.Trigger.GetNextFireTimeUtc()).ThenByDescending(t => t.Trigger.Priority);

                foreach (var trigger in candidates)
                {
                    Debug.WriteLine("Processing candidate. Name: {0} Next fire time: {1}", trigger.Trigger.Name, trigger.Trigger.GetNextFireTimeUtc());

                    if (trigger.Trigger.GetNextFireTimeUtc() == null)
                    {
                        Debug.WriteLine("Candidate has no next fire time. Excluding.");
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
                        Debug.WriteLine("Breaking, have hit trigger beyond the time window.");
                        break;
                    }

                    if (this.ApplyMisfireIfNecessary(trigger))
                    {
                        Debug.WriteLine("Applied misfire. Next fire time: {0}", trigger.Trigger.GetNextFireTimeUtc());

                        if (trigger.Trigger.GetNextFireTimeUtc() == null
                            || trigger.Trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                        {
                            Debug.WriteLine("Continuing. No next fire time, or fire time outside of window.");
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
                            Debug.WriteLine("Continuing. Added non-concurrent trigger twice.");

                            continue; // go to next trigger in store.
                        }
                        else
                        {
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }
                    }

                    var acquireTriggerConditionalExpressionAttributeNames = new Dictionary<string, string> {
                        { "#S", "State" },
                        { "#N", "Name" },
                        { "#G", "Group" }
                    };

                    // Only grab a trigger if the state is still waiting (another scheduler hasn't grabbed it meanwhile)
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

                    if (acquiredTrigger.Any())
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
                cal = this.RetrieveCalendar(trigger.Trigger.CalendarName);
            }

            _signaler.NotifyTriggerListenersMisfired(trigger.Trigger);
            Debug.WriteLine("Misfired. Time now: {0}. Trigger fire time: {1}", misfireTime.Ticks, tnft.Value.Ticks);

            trigger.Trigger.UpdateAfterMisfire(cal);
            this.StoreTrigger(trigger.Trigger, true);

            if (!trigger.Trigger.GetNextFireTimeUtc().HasValue)
            {
                trigger.State = DynamoTriggerState.Complete;
                this.StoreTrigger(trigger.Trigger, true);

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
            var t = _triggerRepository.Load(trigger.Key.ToDictionary());

            t.SchedulerInstanceId = string.Empty;
            t.State = DynamoTriggerState.Waiting;

            _triggerRepository.Store(t);
        }

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            lock (LockObject)
            {
                List<TriggerFiredResult> results = new List<TriggerFiredResult>();

                foreach (IOperableTrigger trigger in triggers)
                {
                    var storedTrigger = _triggerRepository.Load(trigger.Key.ToDictionary());
                    // was the trigger deleted since being acquired?
                    if (storedTrigger == null)
                    {
                        continue;
                    }
                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (storedTrigger.State != DynamoTriggerState.Acquired)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = this.RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }

                    DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();

                    Debug.WriteLine("Triggering Trigger! Previous Fire Time: {0}. Next Fire Time: {1}. Repeat Interval: {2}.  Calendar: {3}. Times Triggered: {4}", trigger.GetPreviousFireTimeUtc(), trigger.GetNextFireTimeUtc(), ((SimpleTriggerImpl)trigger).RepeatInterval, trigger.CalendarName, ((SimpleTriggerImpl)trigger).TimesTriggered);

                    trigger.Triggered(cal);
                    Debug.WriteLine("Triggered Trigger! Previous Fire Time: {0}. Next Fire Time: {1}.", trigger.GetPreviousFireTimeUtc(), trigger.GetNextFireTimeUtc());
                    storedTrigger.Trigger = (SimpleTriggerImpl)trigger;
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
                        //concurrent execution not allowed so set triggers to Blocked (or PausedAndBlocked) and Jobs to Blocked.

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
                return results;
            }
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
                                         SchedulerInstruction triggerInstCode)
        {
            this.ReleaseAcquiredTrigger(trigger);

            // It's possible that the job is null if:
            //   1- it was deleted during execution
            //   2- RAMJobStore is being used only for volatile jobs / triggers
            //      from the JDBC job store

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

            // check for trigger deleted during execution...
            if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
            {
                Debug.WriteLine("Deleting trigger");
                DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                if (!d.HasValue)
                {
                    // double check for possible reschedule within job 
                    // execution, which would cancel the need to delete...
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

                IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key);

                //todo: can this be done in one transaction lower down?
                foreach (var trig in jobTriggers)
                {
                    var record = _triggerRepository.Load(trig.Key.ToDictionary());
                    record.State = DynamoTriggerState.Error;
                    _triggerRepository.Store(record);
                }

                _signaler.SignalSchedulingChange(null);
            }
            else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
            {
                IList<Spi.IOperableTrigger> jobTriggers = this.GetTriggersForJob(jobDetail.Key);

                //todo: can this be done in one transaction lower down?
                foreach (var trig in jobTriggers)
                {
                    var record = _triggerRepository.Load(trig.Key.ToDictionary());
                    record.State = DynamoTriggerState.Complete;
                    _triggerRepository.Store(record);
                }

                _signaler.SignalSchedulingChange(null);
            }
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
            var scheduler = new DynamoScheduler
            {
                InstanceId = _instanceId,
                ExpiresUtc = (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime,
                State = "Running"
            };

            _schedulerRepository.Store(scheduler);
        }

        /// <summary>
        /// Deletes any expired scheduler records.
        /// </summary>
        private void DeleteExpiredSchedulers()
        {
            int epochNow = SystemTime.Now().UtcDateTime.ToUnixEpochTime();
            var expressionAttributeValues = new Dictionary<string, AttributeValue> { {
                    ":EpochNow",
                    new AttributeValue {
                        N = epochNow.ToString()
                    }
                }
            };
            var filterExpression = "ExpiresUtcEpoch < :EpochNow";
            var expiredSchedulers = _schedulerRepository.Scan(expressionAttributeValues, null, filterExpression);

            foreach (var dynamoScheduler in expiredSchedulers)
            {
                _schedulerRepository.Delete(dynamoScheduler.Key);
            }
        }

        /// <summary>
        /// Reset the state of any triggers that are associated with non-active schedulers.
        /// </summary>
        private void ResetTriggersAssociatedWithNonActiveSchedulers()
        {
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

        #endregion

    }
}