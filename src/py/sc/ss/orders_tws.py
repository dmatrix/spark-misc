"""
Retail Order Tracking System using Spark Structured Streaming with transformWithState.

This module implements the core OrderTrackingProcessor class for monitoring clothing orders
through their complete lifecycle: Order -> Processing -> Shipped -> Delivered.
It handles order cancellations and SLA (Service Level Agreement) monitoring with
automatic breach detection and alerting.

The system uses Spark's transformWithState API to maintain stateful processing of
order events, ensuring data consistency and providing real-time order status updates.

For utility functions like stream creation and query management, see tws_utility.py.

Author: Jules S. Damji & Cursor AI
"""

from typing import Iterator, Any

import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType
)

class OrderTrackingProcessor(StatefulProcessor):
    """
    A stateful processor for tracking retail clothing orders through their complete lifecycle.
    
    This processor manages order state transitions from placement to delivery, monitors
    Service Level Agreements (SLAs), and handles order cancellations. It maintains
    persistent state for each order and emits events for status changes and SLA breaches.
    
    Order Lifecycle States:
        - ORDER: Initial state when order is placed
        - PROCESSING: Order is being prepared/manufactured
        - SHIPPED: Order has been shipped to customer
        - DELIVERED: Order has been successfully delivered
        - CANCELLED: Order has been cancelled (can occur from any non-delivered state)
    
    SLA Monitoring:
        - Processing SLA: 24 hours from order placement to processing start
        - Shipping SLA: 3 days from processing start to shipment
        - Delivery SLA: 7 days total from order placement to delivery
    
    Attributes:
        handle (StatefulProcessorHandle): Handle for managing state and timers
        order_state: Value state storing order information
        processing_sla (int): Processing SLA timeout in milliseconds (24 hours)
        shipping_sla (int): Shipping SLA timeout in milliseconds (3 days)
        delivery_sla (int): Delivery SLA timeout in milliseconds (7 days)
    
    Examples:
        >>> processor = OrderTrackingProcessor()
        >>> # Used within Spark streaming query with transformWithStateInPandas
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize the order tracking processor with state management and SLA timers.
        
        Sets up the order state schema, initializes the stateful processor handle,
        and configures SLA timeout values for different order processing stages.
        
        Args:
            handle (StatefulProcessorHandle): The handle provided by Spark for managing
                state and timers in the stateful processor.
        
        Returns:
            None
            
        Note:
            This method is called once per processor instance during initialization.
            The order state schema includes all necessary fields for tracking order
            lifecycle and timestamps.
        """
        # Define comprehensive schema for order state tracking
        order_state_schema: StructType = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_category", StringType(), True),  # shirts, pants, shoes, etc.
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("current_status", StringType(), True),  # order, processing, shipped, delivered
            StructField("order_time", TimestampType(), True),
            StructField("processing_time", TimestampType(), True),
            StructField("shipped_time", TimestampType(), True),
            StructField("delivered_time", TimestampType(), True)
        ])
        
        # Initialize processor state management
        self.handle: StatefulProcessorHandle = handle
        self.order_state = handle.getValueState("order_state", order_state_schema)
        
        # Configure SLA timers (in milliseconds)
        self.processing_sla: int = 24 * 60 * 60 * 1000  # 24 hours to start processing
        self.shipping_sla: int = 3 * 24 * 60 * 60 * 1000  # 3 days to ship
        self.delivery_sla: int = 7 * 24 * 60 * 60 * 1000  # 7 days total for delivery
    
    def handleInputRows(
        self, 
        key: str, 
        rows: Iterator[pd.DataFrame], 
        timerValues: Any
    ) -> Iterator[pd.DataFrame]:
        """
        Process order events for a specific order_id and manage state transitions.
        
        This method handles all incoming order events for a given order ID, processes
        state transitions, manages SLA timers, and emits appropriate output events.
        It maintains order state consistency and validates state transitions.
        
        Args:
            key (str): The order ID that groups the events (partition key)
            rows (Iterator[pd.DataFrame]): Iterator of DataFrames containing order events
                for this specific order ID
            timerValues (Any): Timer values from Spark for managing processing time
                and SLA monitoring
        
        Returns:
            Iterator[pd.DataFrame]: Iterator yielding DataFrames with processed events,
                status updates, and SLA breach notifications
        
        Yields:
            pd.DataFrame: DataFrame containing columns:
                - order_id (str): The order identifier
                - event_type (str): Type of event processed
                - status (str): Current order status
                - message (str): Human-readable status message
                - sla_breach (bool): Whether an SLA was breached
        
        Raises:
            No explicit exceptions raised, but invalid state transitions are handled
            gracefully by emitting error events.
        
        Note:
            This method processes events in chronological order and maintains
            state consistency. Invalid state transitions result in error events
            rather than exceptions.
        """
        
        # Process all incoming events for this order
        row_list = list(rows)
        if not row_list:
            yield pd.DataFrame(columns=['order_id', 'event_type', 'status', 'message', 'sla_breach'])
            return
        
        # Get the latest event (assuming events come in chronological order)
        combined_df = pd.concat(row_list, ignore_index=True)
        latest_event = combined_df.iloc[-1]  # Get most recent event
        
        event_type = latest_event['event_type']
        timestamp = latest_event['timestamp']
        
        # Get existing order state or create new one
        if self.order_state.exists():
            order_data = self.order_state.get().iloc[0]
            order_id = order_data['order_id']
            customer_id = order_data['customer_id']
            item_name = order_data['item_name']
            item_category = order_data['item_category']
            quantity = order_data['quantity']
            price = order_data['price']
            current_status = order_data['current_status']
            order_time = order_data['order_time']
            processing_time = order_data['processing_time']
            shipped_time = order_data['shipped_time']
            delivered_time = order_data['delivered_time']
        else:
            # New order - initialize from first event
            if event_type != 'ORDER_PLACED':
                # Invalid first event
                yield pd.DataFrame({
                    'order_id': [key],
                    'event_type': ['ERROR'],
                    'status': ['INVALID'],
                    'message': [f'Invalid first event: {event_type}. Expected ORDER_PLACED'],
                    'sla_breach': [False]
                })
                return
            
            # Initialize new order
            order_id = latest_event['order_id']
            customer_id = latest_event['customer_id']
            item_name = latest_event['item_name']
            item_category = latest_event['item_category']
            quantity = latest_event['quantity']
            price = latest_event['price']
            current_status = 'ORDER'
            order_time = timestamp
            processing_time = None
            shipped_time = None
            delivered_time = None
        
        # Process the event and update state
        output_events = []
        sla_breach = False
        
        if event_type == 'ORDER_PLACED' and current_status == 'ORDER':
            current_status = 'ORDER'
            order_time = timestamp
            
            # Set timer for processing SLA
            self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + self.processing_sla)
            
            output_events.append({
                'order_id': order_id,
                'event_type': 'ORDER_CONFIRMED',
                'status': current_status,
                'message': f'Order placed for {quantity}x {item_name} ({item_category})',
                'sla_breach': False
            })
            
        elif event_type == 'PROCESSING_STARTED' and current_status == 'ORDER':
            current_status = 'PROCESSING'
            processing_time = timestamp
            
            # Clear processing SLA timer and set shipping SLA timer
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + self.shipping_sla)
            
            output_events.append({
                'order_id': order_id,
                'event_type': 'PROCESSING_STARTED',
                'status': current_status,
                'message': f'Processing started for {item_name}',
                'sla_breach': False
            })
            
        elif event_type == 'ITEM_SHIPPED' and current_status == 'PROCESSING':
            current_status = 'SHIPPED'
            shipped_time = timestamp
            
            # Clear shipping SLA timer and set delivery SLA timer
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            
            # Calculate remaining time for delivery SLA from order time
            elapsed_time = timerValues.getCurrentProcessingTimeInMs() - pd.to_datetime(order_time).value // 1000000
            remaining_delivery_time = max(self.delivery_sla - elapsed_time, 60000)  # At least 1 minute
            self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + remaining_delivery_time)
            
            output_events.append({
                'order_id': order_id,
                'event_type': 'ITEM_SHIPPED',
                'status': current_status,
                'message': f'Item shipped: {item_name}',
                'sla_breach': False
            })
            
        elif event_type == 'ITEM_DELIVERED' and current_status == 'SHIPPED':
            current_status = 'DELIVERED'
            delivered_time = timestamp
            
            # Clear all timers - order is complete
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            
            # Check if delivered within SLA
            total_time = pd.to_datetime(delivered_time).value // 1000000 - pd.to_datetime(order_time).value // 1000000
            sla_breach = total_time > self.delivery_sla
            
            output_events.append({
                'order_id': order_id,
                'event_type': 'ITEM_DELIVERED',
                'status': current_status,
                'message': f'Item delivered: {item_name}' + (' (SLA BREACH)' if sla_breach else ' (On Time)'),
                'sla_breach': sla_breach
            })
            
            # Order is complete - could clear state or keep for analytics
            # For this example, we'll keep the final state
            
        elif event_type == 'ORDER_CANCELLED':
            # Handle cancellation from any state except delivered
            if current_status != 'DELIVERED':
                # Clear all timers
                for timer in self.handle.listTimers():
                    self.handle.deleteTimer(timer)
                
                current_status = 'CANCELLED'
                
                output_events.append({
                    'order_id': order_id,
                    'event_type': 'ORDER_CANCELLED',
                    'status': current_status,
                    'message': f'Order cancelled: {item_name}',
                    'sla_breach': False
                })
            else:
                output_events.append({
                    'order_id': order_id,
                    'event_type': 'ERROR',
                    'status': current_status,
                    'message': 'Cannot cancel delivered order',
                    'sla_breach': False
                })
        else:
            # Invalid state transition
            output_events.append({
                'order_id': order_id,
                'event_type': 'ERROR',
                'status': current_status,
                'message': f'Invalid transition: {event_type} from {current_status}',
                'sla_breach': False
            })
        
        # Update state
        updated_state = pd.DataFrame({
            'order_id': [order_id],
            'customer_id': [customer_id],
            'item_name': [item_name],
            'item_category': [item_category],
            'quantity': [quantity],
            'price': [price],
            'current_status': [current_status],
            'order_time': [order_time],
            'processing_time': [processing_time],
            'shipped_time': [shipped_time],
            'delivered_time': [delivered_time]
        })
        
        self.order_state.update(updated_state)
        
        # Return output events
        if output_events:
            yield pd.DataFrame(output_events)
        else:
            yield pd.DataFrame(columns=['order_id', 'event_type', 'status', 'message', 'sla_breach'])
    
    def handleExpiredTimer(
        self, 
        key: str, 
        timerValues: Any, 
        expiredTimerInfo: Any
    ) -> Iterator[pd.DataFrame]:
        """
        Handle SLA timer expiration and emit breach notifications.
        
        This method is called when an SLA timer expires, indicating that an order
        has breached its Service Level Agreement. It determines which SLA was
        breached based on the current order status and emits appropriate alerts.
        
        Args:
            key (str): The order ID for which the timer expired
            timerValues (Any): Timer values from Spark streaming engine
            expiredTimerInfo (Any): Information about the expired timer
        
        Returns:
            Iterator[pd.DataFrame]: Iterator yielding DataFrames with SLA breach events
        
        Yields:
            pd.DataFrame: DataFrame containing SLA breach notification with columns:
                - order_id (str): The order identifier
                - event_type (str): Always 'SLA_BREACH' for timer expiration
                - status (str): Current order status when breach occurred
                - message (str): Detailed breach message with order and item info
                - sla_breach (bool): Always True for timer expiration events
        
        Note:
            SLA breach types based on order status:
            - ORDER status: Processing SLA breach (24 hours)
            - PROCESSING status: Shipping SLA breach (3 days)
            - SHIPPED status: Delivery SLA breach (7 days total)
        """
        
        if not self.order_state.exists():
            yield pd.DataFrame(columns=['order_id', 'event_type', 'status', 'message', 'sla_breach'])
            return
        
        order_data = self.order_state.get().iloc[0]
        order_id = order_data['order_id']
        current_status = order_data['current_status']
        item_name = order_data['item_name']
        
        # Determine which SLA was breached based on current status
        if current_status == 'ORDER':
            message = f'SLA BREACH: Order {order_id} not processed within 24 hours - Item: {item_name}'
        elif current_status == 'PROCESSING':
            message = f'SLA BREACH: Order {order_id} not shipped within 3 days - Item: {item_name}'
        elif current_status == 'SHIPPED':
            message = f'SLA BREACH: Order {order_id} not delivered within 7 days - Item: {item_name}'
        else:
            message = f'SLA timer expired for order {order_id} in status {current_status}'
        
        # Emit SLA breach alert
        yield pd.DataFrame({
            'order_id': [order_id],
            'event_type': ['SLA_BREACH'],
            'status': [current_status],
            'message': [message],
            'sla_breach': [True]
        })
    
    def close(self) -> None:
        """
        Cleanup resources when the processor shuts down.
        
        This method is called when the stateful processor is being shut down,
        allowing for proper cleanup of resources, connections, or any other
        cleanup operations that may be required.
        
        Returns:
            None
            
        Note:
            Currently no explicit cleanup is required as Spark handles state
            and timer cleanup automatically. This method is provided for
            future extensibility if cleanup logic is needed.
        """
        pass

