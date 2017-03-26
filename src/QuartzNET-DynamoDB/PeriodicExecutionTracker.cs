using System;

namespace Quartz.DynamoDB
{
    /// <summary>
    /// Tracks the state of an operation that should only execute periodically.
    /// 
    /// Note: This class is not threadsafe, meaning two executions could happen concurrently
    /// this is by design, we want to minimise the number of times certain operations happen
    /// not ensure they only happen at particular times.
    /// </summary>
    public class PeriodicExecutionTracker
    {
        private readonly TimeSpan _executionFrequency;

        private DateTime _lastExecutionTime = DateTime.MinValue;

        public PeriodicExecutionTracker(TimeSpan executionFrequency)
        {
            _executionFrequency = executionFrequency;
        }

        public bool ShouldExecute()
        {
            return this.ShouldExecute(DateTime.UtcNow);
        }

        public bool ShouldExecute(DateTime utcNow)
        {
            if (_lastExecutionTime == DateTime.MinValue)
            {
                _lastExecutionTime = utcNow;
                return true;
            }

            if (_lastExecutionTime.Add(_executionFrequency).CompareTo(utcNow) <= 0)
            {
                _lastExecutionTime = utcNow;
                return true;
            }

            return false;
        }
    }
}
