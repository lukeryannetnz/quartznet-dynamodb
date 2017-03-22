using System;

namespace Quartz.DynamoDB.DataModel.Storage
{
    /// <summary>
    /// Calculates a backoff duration that is the input cubed
    /// plus a random value between one and the input cubed.
    /// </summary>
    public class ExponentialBackoffWithRandomVariation
    {
        private static readonly Random Random = new Random();

        public static TimeSpan CalculateWaitDuration(int retryAttempt)
        {
            int delay = (int)Math.Pow(retryAttempt, 3);

            delay += Random.Next(1, delay);

            return TimeSpan.FromSeconds(delay);
        }
    }
}
