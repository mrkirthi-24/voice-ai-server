import time
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
from pipecat.transports.daily.transport import DailyOutputTransportMessageFrame

# Store reference to task for broadcasting
_task_ref: Optional[Any] = None


def set_transport(task):
    """Set the task reference for broadcasting function calls"""
    global _task_ref
    _task_ref = task
    logger.info("Function tracker task set")


async def broadcast_function_call(
    function_name: str,
    arguments: Dict[str, Any],
    result: Any = None,
    success: bool = True,
    error: Optional[str] = None,
    duration_ms: Optional[float] = None,
):
    """Broadcast function call to frontend via app message"""
    if _task_ref is None:
        logger.warning("Task not set, cannot broadcast function call")
        return

    try:
        message_data = {
            "type": "tool_call",
            "data": {
                "function": function_name,
                "arguments": arguments,
                "result": result,
                "success": success,
                "error": error,
                "duration": duration_ms,
                "timestamp": datetime.now().isoformat(),
            }
        }
        
        # Create the transport message frame
        frame = DailyOutputTransportMessageFrame(message=message_data)
        
        # Queue the frame through the task
        await _task_ref.queue_frames([frame])
        
        logger.info(f"📤 Broadcasted function call: {function_name}")
    except Exception as e:
        logger.error(f"Error broadcasting function call: {e}")


async def track_function_call(
    function_name: str,
    flow_manager: Any,
    original_func,
    *args,
    **kwargs
) -> Any:
    """
    Wrapper to track and broadcast function calls
    
    Args:
        function_name: Name of the function being called
        flow_manager: FlowManager instance for state access
        original_func: The original function to call
        *args, **kwargs: Arguments to pass to the function
    
    Returns:
        Result from the original function
    """
    # Extract arguments for logging
    arguments = {}
    if args:
        arguments['args'] = str(args)[:200]
    if kwargs:
        arguments['kwargs'] = str(kwargs)[:200]
    
    # Add relevant state info
    if hasattr(flow_manager, 'state'):
        state_snapshot = {
            'user_identified': flow_manager.state.get('user_identified', False),
            'phone_number': flow_manager.state.get('phone_number', 'N/A'),
            'name': flow_manager.state.get('name', 'N/A'),
        }
        arguments['state'] = state_snapshot
    
    start_time = time.time()
    result = None
    success = True
    error = None
    
    try:
        logger.info(f"⚡ Function Call: {function_name} - Starting...")
        result = await original_func(flow_manager, *args, **kwargs)
        logger.info(f"Function Call: {function_name} - Completed")
    except Exception as e:
        success = False
        error = str(e)
        logger.error(f"Error Function Call: {function_name} - Failed: {e}")
        raise
    finally:
        duration_ms = round((time.time() - start_time) * 1000, 2)
        
        # Serialize result for transmission
        serialized_result = None
        if result and len(result) > 1:
            # Result is a tuple (data, NodeConfig)
            serialized_result = {
                "success": True,
                "message": f"{function_name} executed successfully"
            }
        
        # Broadcast the function call to frontend
        await broadcast_function_call(
            function_name=function_name,
            arguments=arguments,
            result=serialized_result,
            success=success,
            error=error,
            duration_ms=duration_ms,
        )
    
    return result
