import time
from datetime import datetime
from loguru import logger
from typing import Dict, Any, Optional, Callable
from pipecat.transports.daily.transport import DailyOutputTransportMessageFrame

# Store reference to task for broadcasting
_task_ref: Optional[Any] = None


def set_transport(task):
    """Set the task reference for broadcasting DB operations"""
    global _task_ref
    _task_ref = task
    logger.info("DB tracker task set")


async def broadcast_db_operation(
    operation: str,
    params: Dict[str, Any],
    result: Any = None,
    success: bool = True,
    error: Optional[str] = None,
    duration_ms: Optional[float] = None,
):
    """Broadcast database operation to frontend via app message"""
    if _task_ref is None:
        logger.warning("Task not set, cannot broadcast DB operation")
        return

    try:
        message_data = {
            "type": "db_operation",
            "data": {
                "operation": operation,
                "params": params,
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
        
        logger.info(f"📤 Broadcasted DB operation: {operation}")
    except Exception as e:
        logger.error(f"Error broadcasting DB operation: {e}")


async def track_db_operation(
    operation_name: str,
    operation_func: Callable,
    *args,
    **kwargs
) -> Any:
    """
    Wrapper to track and broadcast database operations
    
    Args:
        operation_name: Name of the DB operation
        operation_func: The async function to call
        *args, **kwargs: Arguments to pass to the function
    
    Returns:
        Result from the operation function
    """
    # Extract relevant params for logging (exclude sensitive data)
    params = {}
    if args:
        params['args'] = str(args)[:200]  # Limit size
    if kwargs:
        # Filter out sensitive keys
        safe_kwargs = {k: v for k, v in kwargs.items() if k not in ['password', 'api_key', 'token']}
        params['kwargs'] = str(safe_kwargs)[:200]
    
    start_time = time.time()
    result = None
    success = True
    error = None
    
    try:
        logger.info(f"🗄️ DB Operation: {operation_name} - Starting...")
        result = await operation_func(*args, **kwargs)
        logger.info(f"✅ DB Operation: {operation_name} - Completed")
    except Exception as e:
        success = False
        error = str(e)
        logger.error(f"❌ DB Operation: {operation_name} - Failed: {e}")
        raise
    finally:
        duration_ms = round((time.time() - start_time) * 1000, 2)
        
        # Broadcast the operation to frontend
        await broadcast_db_operation(
            operation=operation_name,
            params=params,
            result=_serialize_result(result),
            success=success,
            error=error,
            duration_ms=duration_ms,
        )
    
    return result


def _serialize_result(result: Any) -> Any:
    """Serialize result for JSON transmission"""
    if result is None:
        return None
    
    # Limit result size for transmission
    if isinstance(result, (str, int, float, bool)):
        return result
    elif isinstance(result, dict):
        # Limit dict size
        return {k: str(v)[:100] for k, v in list(result.items())[:10]}
    elif isinstance(result, list):
        # Limit list size
        return [str(item)[:100] for item in result[:10]]
    else:
        return str(result)[:200]
