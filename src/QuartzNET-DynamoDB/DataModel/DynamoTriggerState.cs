namespace Quartz.DynamoDB.DataModel
{
	/// <summary>
	/// Represents the state of the dynamo quartz trigger. Similar to the Quartz.TriggerState enumeration, 
	/// but with additional values e.g. PausedAndBlocked.
	/// </summary>
	public class DynamoTriggerState
	{
		private readonly int internalValue;

		public static readonly DynamoTriggerState None = new DynamoTriggerState(0);

		public static readonly DynamoTriggerState Normal = new DynamoTriggerState(1);

		public static readonly DynamoTriggerState Paused = new DynamoTriggerState(2);

		public static readonly DynamoTriggerState PausedAndBlocked = new DynamoTriggerState(3);

		public static readonly DynamoTriggerState Complete = new DynamoTriggerState(4);

		public static readonly DynamoTriggerState Error = new DynamoTriggerState(5);

		public static readonly DynamoTriggerState Blocked = new DynamoTriggerState(6);

		public static readonly DynamoTriggerState Waiting = new DynamoTriggerState(7);

		public static readonly DynamoTriggerState Acquired = new DynamoTriggerState(8);

		public static readonly DynamoTriggerState Executing = new DynamoTriggerState(9);

		public int InternalValue
		{
			get { return internalValue; }
		}

		public DynamoTriggerState(int value)
		{
			internalValue = value;
		}

		/// <summary>
		/// Returns the State property as the Quartz.TriggerState enumeration required by the JobStore contract.
		/// </summary>
		public TriggerState TriggerState
		{
			get
			{
				switch (InternalValue)
				{
					case 0:
						{
							return TriggerState.None;
						}
					case 1:
						{
							return TriggerState.Normal;
						}
					case 2:
						{
							return TriggerState.Paused;
						}
					case 3: //PausedAndBlocked
						{
							return TriggerState.Paused;
						}
					case 4:
						{
							return TriggerState.Complete;
						}
					case 5:
						{
							return TriggerState.Error;
						}
					case 6:
						{
							return TriggerState.Blocked;
						}
					default:
						{
							return TriggerState.Normal;
						}
				}
			}
		}

		/// <summary>
		/// Determines whether the specified <see cref="object"/> is equal to the current <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
		/// Compares value of the internal integer.
		/// </summary>
		/// <param name="obj">The <see cref="object"/> to compare with the current <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.</param>
		/// <returns><c>true</c> if the specified <see cref="object"/> is equal to the current
		/// <see cref="T:Quartz.DynamoDB.DataModel.DynamoTriggerState"/>; otherwise, <c>false</c>.</returns>
		public override bool Equals(object obj)
		{
			DynamoTriggerState input = obj as DynamoTriggerState;

			if (object.ReferenceEquals(input, null))
			{
				return false;
			}

			return this.InternalValue.Equals(input.InternalValue);
		}

		/// <summary>
		/// Determines whether a specified instance of <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> is
		/// equal to another specified <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
		/// Compares value of the internal integer.
		/// </summary>
		/// <param name="left">The first <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
		/// <param name="right">The second <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
		/// <returns><c>true</c> if <c>left</c> and <c>right</c> are equal; otherwise, <c>false</c>.</returns>
		public static bool operator ==(DynamoTriggerState left, DynamoTriggerState right)
		{
			if (object.ReferenceEquals(left, null) && object.ReferenceEquals(right, null))
			{
				return true;
			}

			if (object.ReferenceEquals(left, null) || object.ReferenceEquals(right, null))
			{
				return false;
			}

			return left.Equals(right);
		}

		/// <summary>
		/// Determines whether a specified instance of <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> is not
		/// equal to another specified <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/>.
		/// Compares value of the internal integer.
		/// </summary>
		/// <param name="left">The first <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
		/// <param name="right">The second <see cref="Quartz.DynamoDB.DataModel.DynamoTriggerState"/> to compare.</param>
		/// <returns><c>true</c> if <c>left</c> and <c>right</c> are not equal; otherwise, <c>false</c>.</returns>
		public static bool operator !=(DynamoTriggerState left, DynamoTriggerState right)
		{
			if (object.ReferenceEquals(left, null) || object.ReferenceEquals(right, null))
			{
				return true;
			}

			return !left.Equals(right);
		}

		public override int GetHashCode()
		{
			return InternalValue.GetHashCode();
		}
	}
}