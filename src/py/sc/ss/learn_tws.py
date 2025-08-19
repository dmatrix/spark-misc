"""
Ultra-Simple Flight State Processor for Learning transformWithState on Databricks

This is the simplest possible example to understand transformWithState:
- Just 3 flight states: boarding -> flying -> landed  
- Minimal validation
- Clear, focused code
- Optimized for Databricks cloud environment
- Uses RocksDB state store with full multi-column family support
- Uses DBFS checkpointing for fault tolerance

Author: Jules S. Damji && Cursor AI
"""

from typing import Iterator, Any, Dict, List
import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class FlightProcessor(StatefulProcessor):
    """
    The simplest flight state processor for learning transformWithState on Databricks.
    
    Powered by Databricks' production-grade infrastructure:
    - RocksDB state store with multi-column family support
    - DBFS checkpointing for fault tolerance  
    - Auto-scaling clusters for performance
    
    Tracks flights through 3 states: boarding -> flying -> landed
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Set up the processor when it starts.
        
        Args:
            handle: Spark's handle for managing state and timers
            
        Returns:
            None
        """
        # Define what we store for each flight
        state_schema = StructType([
            StructField("flight", StringType(), True),      # Flight name like "Delta1247"
            StructField("state", StringType(), True),       # Current state
            StructField("count", IntegerType(), True)       # How many updates
        ])
        
        # Create state storage - RocksDB handles multiple column families!
        self.flight_state = handle.getValueState("flight_state", state_schema)
    
    def handleInputRows(self, key: str, rows: Iterator[pd.DataFrame], timerValues: Any) -> Iterator[pd.DataFrame]:
        """
        Process flight updates for one flight.
        
        Args:
            key: The flight identifier (e.g., "Delta1247")
            rows: Iterator of DataFrames containing flight state updates
            timerValues: Timer information (not used in this example)
            
        Returns:
            Iterator[pd.DataFrame]: Iterator containing result DataFrame with flight state info
        """
        
        # Get all updates for this flight
        all_rows = []
        for batch in rows:
            all_rows.append(batch)
        
        if not all_rows:
            return iter([])
        
        # Combine all updates
        updates = pd.concat(all_rows, ignore_index=True)
        
        # Get current state for this flight
        # Databricks RocksDB handles this efficiently with multi-column family support!
        if self.flight_state.exists():
            state_data = self.flight_state.get()
            
            # The state store returns a DataFrame, extract the actual values
            if isinstance(state_data, pd.DataFrame):
                if len(state_data) > 0:
                    current = state_data.iloc[0]
                    current_state = current['state']
                    current_count = current['count']
                else:
                    current_state = "unknown"
                    current_count = 0
            else:
                # Fallback for other data types
                current_state = "unknown"
                current_count = 0
            
            # Handle None values
            if current_count is None:
                current_count = 0
            if current_state is None:
                current_state = "unknown"
            # Optional: Add logging to see Databricks state management in action
            if current_count % 5 == 0:  # Log every 5th update
                print(f"🗄️  Databricks RocksDB: {key} state retrieved (count: {current_count})")
        else:
            current_state = "unknown"
            current_count = 0
            print(f"🆕 New flight {key} starting in Databricks state store")
        
        # Process each update
        for _, update in updates.iterrows():
            new_state = update['state']
            flight_name = update['flight']
            
            # Simple validation: only allow valid progressions
            if self._is_valid_transition(current_state, new_state):
                current_state = new_state
                current_count += 1
                # Optional: Show state transitions
                if current_count <= 3:  # Show first few transitions
                    print(f"✅ {flight_name}: {current_state} (update #{current_count})")
            else:
                # Still increment count even if transition is invalid (for demo purposes)
                current_count += 1
                # Optional: Show invalid transitions for debugging
                if current_count <= 3:
                    print(f"⚠️  {flight_name}: {current_state} -> {new_state} (invalid transition, staying at {current_state})")
        
        # Save the new state to Databricks RocksDB
        new_state_data = pd.DataFrame({
            'flight': [flight_name],
            'state': [current_state],
            'count': [current_count]
        })
        self.flight_state.update(new_state_data)
        
        # Return the result
        result = pd.DataFrame({
            'flight': [flight_name],
            'current_state': [current_state],
            'update_count': [str(current_count)]
        })
        
        return iter([result])
    
    def _is_valid_transition(self, from_state: str, to_state: str) -> bool:
        """
        Check if state transition is allowed.
        
        Args:
            from_state: Current state of the flight
            to_state: Desired new state for the flight
            
        Returns:
            bool: True if transition is valid, False otherwise
        """
        # Simple rules: boarding -> flying -> landed
        valid_moves = {
            "unknown": ["boarding"],           # New flights start boarding
            "boarding": ["flying"],            # From boarding, can fly
            "flying": ["landed"],              # From flying, can land
            "landed": ["boarding"]             # From landed, can start new journey
        }
        
        allowed = valid_moves.get(from_state, [])
        return to_state in allowed
    
    def handleExpiredTimer(self, key: str, timerValues: Any, expiredTimerInfo: Any) -> Iterator[pd.DataFrame]:
        """
        Handle timers (not used in this simple example).
        
        Args:
            key: The flight identifier for which the timer expired
            timerValues: Timer values
            expiredTimerInfo: Information about the expired timer
            
        Returns:
            Iterator[pd.DataFrame]: Empty iterator since timers are not used
        """
        return iter([])
    
    def close(self) -> None:
        """
        Clean up when done.
        
        Args:
            None
            
        Returns:
            None
        """
        pass
