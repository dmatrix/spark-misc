"""
Ultra-Simple Flight State Processor for Learning transformWithState on Spark 4.0

This is the simplest possible example to understand transformWithState:
- Just 3 flight states: boarding -> flying -> landed  
- Minimal validation
- Clear, focused code
- Optimized for Spark
- Uses RocksDB state store with full multi-column family support
- Uses FS checkpointing for fault tolerance

Author: Jules S. Damji & Cursor AI
"""

from typing import Iterator, Any, Dict, List
import pandas as pd
import json
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class FlightProcessor(StatefulProcessor):
    """
    The simplest flight state processor for learning transformWithState.
    
    Powered by clean learning infrastructure:
    - RocksDB state store with JSON serialization
    - Local checkpointing for fault tolerance  
    - Auto-advancing state transitions for predictable learning
    
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
        # Define simple JSON-based state schema to avoid protobuf issues
        state_schema = StructType([
            StructField("state_json", StringType(), True)   # All state as JSON string
        ])
        
        # Create state storage - RocksDB with JSON serialization!
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
        
        # Get current state for this flight using JSON deserialization
        if self.flight_state.exists():
            state_data = self.flight_state.get()
            
            # Extract JSON string and deserialize
            if isinstance(state_data, pd.DataFrame) and len(state_data) > 0:
                json_str = state_data.iloc[0]['state_json']
                if json_str:
                    try:
                        state_dict = json.loads(json_str)
                        current_state = state_dict.get('state', 'unknown')
                        current_count = state_dict.get('count', 0)
                        flight_name = state_dict.get('flight', key)
                    except (json.JSONDecodeError, KeyError):
                        current_state = "unknown"
                        current_count = 0
                        flight_name = key
                else:
                    current_state = "unknown"
                    current_count = 0
                    flight_name = key
            else:
                current_state = "unknown"
                current_count = 0
                flight_name = key
            
            # Optional: Add logging to see RocksDB state management in action
            if current_count % 5 == 0:  # Log every 5th update
                print(f"🗄️  RocksDB JSON: {key} state retrieved (count: {current_count})")
        else:
            current_state = "unknown"
            current_count = 0
            flight_name = key
            print(f"🆕 New flight {key} starting in RocksDB state store")
        
        # Process each update - ensure state transitions work correctly
        for _, update in updates.iterrows():
            new_state = update['state']
            input_flight_name = update['flight']
            
            # Use the flight name from input (it should match the key)
            flight_name = input_flight_name
            
            # Debug: Show what we're processing
            print(f"🔄 Processing {flight_name}: {current_state} -> {new_state}")
            
            # Always advance to next logical state regardless of input
            old_state = current_state
            current_state = self._get_next_logical_state(current_state)
            current_count += 1
            print(f"🔄 {flight_name}: {old_state} -> {current_state} (auto-advance #{current_count})")
        
        # Save the new state to RocksDB using JSON serialization
        state_dict = {
            'flight': flight_name,
            'state': current_state,
            'count': current_count
        }
        json_str = json.dumps(state_dict)
        
        new_state_data = pd.DataFrame({
            'state_json': [json_str]
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
        # Allow staying in the same state (idempotent updates)
        if from_state == to_state:
            return True
            
        # Simple rules: boarding -> flying -> landed
        valid_moves = {
            "unknown": ["boarding"],           # New flights start boarding
            "boarding": ["flying"],            # From boarding, can fly
            "flying": ["landed"],              # From flying, can land
            "landed": ["boarding"]             # From landed, can start new journey
        }
        
        allowed = valid_moves.get(from_state, [])
        is_valid = to_state in allowed
        
        # Debug transition validation
        print(f"🔍 Transition check: {from_state} -> {to_state} = {'VALID' if is_valid else 'INVALID'}")
        
        return is_valid
    
    def _get_next_logical_state(self, current_state: str) -> str:
        """
        Get the next logical state in the flight progression.
        
        Args:
            current_state: Current state of the flight
            
        Returns:
            str: Next logical state
        """
        # Define the logical progression
        next_states = {
            "unknown": "boarding",
            "boarding": "flying", 
            "flying": "landed",
            "landed": "boarding"  # Cycle back for new journey
        }
        
        return next_states.get(current_state, "boarding")
    
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
