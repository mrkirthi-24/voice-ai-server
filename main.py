import os
from datetime import datetime
import aiohttp
import asyncio
from dotenv import load_dotenv
from loguru import logger
from typing import Dict, List, Any

from pipecat_flows import (
    FlowManager,
    NodeConfig,
)
from pipecat.frames.frames import LLMMessagesAppendFrame, LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.azure.stt import AzureSTTService
from pipecat.services.azure.llm import AzureLLMService
from pipecat.services.tavus.video import TavusVideoService
from pipecat.services.simli.video import SimliVideoService
from pipecat.transports.daily.transport import (
    DailyParams,
    DailyOutputTransportMessageFrame,
)
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.turns.user_stop import TurnAnalyzerUserTurnStopStrategy
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.observers.loggers.user_bot_latency_log_observer import UserBotLatencyLogObserver

from database import (
    book_appointment,
    get_or_create_user_from_database,
    save_conversation_summary_to_database,
    get_user_appointments_from_database,
    update_appointment_in_database,
)

# Import trackers for broadcasting operations to frontend
from db_tracker import set_transport as set_db_tracker_transport
from function_tracker import set_transport as set_function_tracker_transport, track_function_call
from service.custom_flow_manager import CustomFlowManager

load_dotenv(override=True)

# Transport configuration
transport_params = {
    "daily": lambda: DailyParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        video_out_enabled=True,
        video_out_is_live=True,
        video_out_width=1280,
        video_out_height=720,
        vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
    ),
    "webrtc": lambda: TransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        video_out_enabled=True,
        video_out_is_live=True,
        video_out_width=1280,
        video_out_height=720,
        vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
    ),
}


# Summary generation
async def generate_summary_text(
    user_info: Dict[str, Any],
    appointments: List[Dict[str, Any]],
    tool_calls: List[Dict[str, Any]],
    duration_seconds: int,
) -> str:
    """Generate conversation summary text from collected data."""
    try:
        summary_parts = []
        
        # Overview
        summary_parts.append("Call Summary:")
        summary_parts.append(f"Duration: {duration_seconds // 60} minutes {duration_seconds % 60} seconds")
        
        # User information
        if user_info.get("name"):
            summary_parts.append(f"\nUser: {user_info.get('name')} ({user_info.get('phone_number', 'N/A')})")
            summary_parts.append(f"Status: {'New User' if user_info.get('is_new_user') else 'Returning User'}")
        
        # Appointments
        if appointments:
            summary_parts.append(f"\nAppointments Booked: {len(appointments)}")
            for apt in appointments:
                summary_parts.append(f"- {apt.get('date')} at {apt.get('time')}: {apt.get('service', 'General Appointment')}")
        else:
            summary_parts.append("\nNo appointments were booked during this call.")
        
        # Actions performed
        if tool_calls:
            summary_parts.append(f"\nActions Performed: {len(tool_calls)}")
            for call in tool_calls:
                func_name = call.get('function', 'unknown')
                summary_parts.append(f"- {func_name}")
        
        return "\n".join(summary_parts)
    except Exception as e:
        logger.error(f"Error generating summary text: {e}")
        return "Summary generation completed. Please review the conversation details."


async def generate_and_save_summary(action: dict, flow_manager: FlowManager) -> None:
    """Generate conversation summary, save to database, and send to frontend within 10 seconds."""
    try:
        logger.info("Generating conversation summary...")
        start_summary_time = datetime.now()
        
        # Get conversation data from state
        start_time = flow_manager.state.get("conversation_start_time", datetime.now().isoformat())
        end_time = datetime.now().isoformat()
        
        # Calculate duration
        try:
            start_dt = datetime.fromisoformat(start_time)
            end_dt = datetime.fromisoformat(end_time)
            duration_seconds = int((end_dt - start_dt).total_seconds())
        except Exception:
            duration_seconds = 0
        
        # Get user info
        user_identified = flow_manager.state.get("user_identified", False)
        phone_number = flow_manager.state.get("phone_number", "")
        name = flow_manager.state.get("name", "")
        is_new_user = flow_manager.state.get("is_new_user", False)
        
        # Get tool calls
        tool_calls = flow_manager.state.get("tool_calls", [])
        
        # Get appointments (with timeout to ensure we finish within 10 seconds)
        appointments = []
        if phone_number:
            try:
                appointments = await asyncio.wait_for(
                    get_user_appointments_from_database(phone_number),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout getting appointments for summary")
        
        # Filter active appointments
        active_appointments = [
            apt for apt in appointments 
            if apt.get("status", "active") == "active"
        ]
        
        # Generate summary text
        user_info = {
            "name": name,
            "phone_number": phone_number,
            "is_new_user": is_new_user,
        }
        
        summary_text = await generate_summary_text(
            user_info,
            active_appointments,
            tool_calls,
            duration_seconds,
        )
        
        # Build summary object
        summary_data = {
            "duration_seconds": duration_seconds,
            "tool_calls_count": len(tool_calls),
            "appointments_discussed": len(active_appointments),
            "user_identified": user_identified,
            "user_phone": phone_number if user_identified else None,
            "summary_text": summary_text,
            "active_appointments": [
                {
                    "date": apt.get("date"),
                    "time": apt.get("time"),
                    "service": apt.get("service", "General Appointment"),
                    "notes": apt.get("notes", ""),
                }
                for apt in active_appointments
            ],
            "tool_calls": [
                {
                    "function": call.get("function"),
                    "timestamp": call.get("timestamp"),
                }
                for call in tool_calls
            ],
            "generated_at": end_time,
            "start_time": start_time,
            "end_time": end_time,
        }
        
        # Save to database (with timeout)
        if phone_number:
            try:
                await asyncio.wait_for(
                    save_conversation_summary_to_database(phone_number, summary_data),
                    timeout=3.0
                )
                logger.info("Conversation summary saved to database")
            except asyncio.TimeoutError:
                logger.warning("Timeout saving summary to database")
            except Exception as e:
                logger.error(f"Error saving summary to database: {e}")
        
        # Send to frontend via app message
        task = getattr(flow_manager, "_task", None)
        if task:
            try:
                message_frame = DailyOutputTransportMessageFrame(
                    message={
                        "type": "conversation_summary",
                        "data": summary_data,
                    }
                )
                await task.queue_frames([message_frame])
                logger.info("Conversation summary sent to frontend")
            except Exception as e:
                logger.error(f"Error sending summary to frontend: {e}")
        
        # Verify we completed within 10 seconds
        elapsed = (datetime.now() - start_summary_time).total_seconds()
        if elapsed > 10:
            logger.warning(f"Summary generation took {elapsed:.2f} seconds (exceeded 10s limit)")
        else:
            logger.info(f"Summary generated in {elapsed:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error generating conversation summary: {e}")

# User action handlers - Internal implementations
async def handle_identity_verification_internal(flow_manager: FlowManager, phone_number: str, name: str) -> tuple[None, NodeConfig]:
    logger.info(f"User provided phone number: {phone_number} and name: {name} for identity verification.")
    result = await get_or_create_user_from_database(name, phone_number)
    flow_manager.state["name"] = result["name"]
    flow_manager.state["user_identified"] = True
    flow_manager.state["is_new_user"] = result["is_new"]
    flow_manager.state["phone_number"] = result["phone_number"]
    flow_manager.state["date"] = datetime.now().strftime("%Y-%m-%d")
    flow_manager.state["available_slots"] = ['11:00', '15:00', '16:00', '17:00']    
    return None, create_user_verified_node(flow_manager)

async def handle_identity_verification(flow_manager: FlowManager, phone_number: str, name: str) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_identity_verification",
        flow_manager,
        handle_identity_verification_internal,
        phone_number=phone_number,
        name=name
    )

async def handle_booking_appointment_internal(flow_manager: FlowManager, slot: str) -> tuple[None, NodeConfig]:
    logger.info(f"User selected slot: {slot} to book the appointment.")
    result = await book_appointment(flow_manager.state["phone_number"], flow_manager.state["date"], slot)
    
    if not result.get("success", False):
        flow_manager.state["available_slots"] = [s for s in flow_manager.state.get("available_slots", []) if s != slot]
        return None, create_rescheduled_appointment_node(flow_manager)
    else:
        flow_manager.state["slot_booked"] = slot
        flow_manager.state["appointment_id"] = result.get("appointment_id")
        flow_manager.state["appointment_booked"] = True
        return None, create_confirmation_node(flow_manager)

async def handle_booking_appointment(flow_manager: FlowManager, slot: str) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_booking_appointment",
        flow_manager,
        handle_booking_appointment_internal,
        slot=slot
    )

async def handle_modify_appointment_internal(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info("User wants to modify the appointment.")
    return None, create_update_appointment_node(flow_manager)

async def handle_modify_appointment(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_modify_appointment",
        flow_manager,
        handle_modify_appointment_internal
    )

async def handle_update_appointment_internal(flow_manager: FlowManager, slot: str) -> tuple[None, NodeConfig]:
    logger.info(f"User updated the appointment to slot: {slot}.")
    
    appointment_id = flow_manager.state.get("appointment_id")
    date = flow_manager.state.get("date")
    
    if not appointment_id:
        logger.error("No appointment_id found in state for update")
        flow_manager.state["available_slots"] = [s for s in flow_manager.state.get("available_slots", []) if s != slot]
        return None, create_rescheduled_appointment_node(flow_manager)
    
    if not date:
        logger.error("No date found in state for update")
        flow_manager.state["available_slots"] = [s for s in flow_manager.state.get("available_slots", []) if s != slot]
        return None, create_rescheduled_appointment_node(flow_manager)
    
    result = await update_appointment_in_database(appointment_id, date, slot)
    
    if not result.get("success", False):
        flow_manager.state["available_slots"] = [s for s in flow_manager.state.get("available_slots", []) if s != slot]
        return None, create_rescheduled_appointment_node(flow_manager)
    else:
        flow_manager.state["slot_booked"] = slot
        flow_manager.state["appointment_id"] = appointment_id
        flow_manager.state["appointment_booked"] = True
        return None, create_confirmation_node(flow_manager)

async def handle_update_appointment(flow_manager: FlowManager, slot: str) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_update_appointment",
        flow_manager,
        handle_update_appointment_internal,
        slot=slot
    )

async def handle_end_conversation_internal(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info("User wants to end the conversation.")
    return None, create_end_node()

async def handle_end_conversation(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_end_conversation",
        flow_manager,
        handle_end_conversation_internal
    )

async def handle_not_interested_internal(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info("User is not interested in booking an appointment.")
    return None, create_end_node()

async def handle_not_interested(flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    return await track_function_call(
        "handle_not_interested",
        flow_manager,
        handle_not_interested_internal
    )

# Node creation functions
def create_initial_node() -> NodeConfig:
    """Create the initial node for appointment booking conversation."""
    return NodeConfig(
        name="initial",
        role_messages=[
            {
                "role": "system",
                "content": """You are an appointment booking assistant for Zion Salon. You must ALWAYS use 
                the available functions to progress the conversation. This is a phone conversation and your
                responses will be converted to audio. Keep the conversation friendly, casual, and polite. 
                Avoid outputting special characters and emojis.""",
            }
        ],
        task_messages=[
            {
                "role": "system",
                "content": """Introduce yourself and greet the user. 
                Ask the user for name and phone number for identity verification. 
                If the user provides a phone number that is not 10 digits, ask again for the phone number.
                Repeat the name and phone number to the user before proceeding with the conversation.
                """
            }
        ],
        functions=[handle_identity_verification, handle_not_interested],
    )

def create_user_verified_node(flow_manager: FlowManager) -> NodeConfig:
    """Create the node for user verified."""
    return NodeConfig(
        name="user_verified",
        task_messages=[
            {
                "role": "system",
                "content": f"""Welcome {flow_manager.state.get('name', 'there')}. Tell the user that {flow_manager.state.get('available_slots', [])} are available 
                for {flow_manager.state.get('date', 'today')}. Ask the user to select a slot to book the appointment. Confirm the slot before proceeding with the conversation.""",
            }
        ],
        functions=[handle_booking_appointment, handle_not_interested],
    )

def create_rescheduled_appointment_node(flow_manager: FlowManager) -> NodeConfig:
    """Create the node for rescheduled appointment."""
    return NodeConfig(
        name="rescheduled_appointment",
        task_messages=[
            {
                "role": "system",
                "content": f"""Tell the user the slot is already booked. Ask the user to choose a slot from the available slots {flow_manager.state.get("available_slots", [])}. 
                If the user does not select a slot, use the function handle_not_interested to end the conversation. Confirm the slot before proceeding with the conversation.""",
            }
        ],
        functions=[handle_booking_appointment, handle_not_interested],
    )

def create_confirmation_node(flow_manager: FlowManager) -> NodeConfig:
    """Create the node for confirmation."""
    date = flow_manager.state.get("date", "the selected date")
    slot = flow_manager.state.get("slot_booked", "the selected time")
    
    return NodeConfig(
        name="confirmation",
        task_messages=[
            {
                "role": "system",
                "content": f"""Appointment booked successfully for {date} at {slot}.
                Ask the user if they have any other questions or want to modify the appointment. 
                If the user has no other questions, use the function handle_end_conversation to end the conversation.
                If the user wants to modify the appointment, use the function handle_modify_appointment to modify the appointment.""",
            }
        ],
        functions=[handle_modify_appointment, handle_end_conversation],
    )

def create_update_appointment_node(flow_manager: FlowManager) -> NodeConfig:
    """Create the node for update appointment."""
    available_slots = flow_manager.state.get("available_slots", [])
    slots_text = ", ".join(available_slots) if available_slots else "available slots"
    date = flow_manager.state.get("date", "today")
    
    return NodeConfig(
        name="update_appointment",
        task_messages=[
            {
                "role": "system",
                "content": f"""Ask the user to choose a slot from {slots_text} for {date}. 
                If the user selects a slot, use the function handle_update_appointment to modify the appointment. 
                If the user does not select a slot, use the function handle_end_conversation to end the conversation.""",
            }
        ],
        functions=[handle_update_appointment, handle_end_conversation],
    )

def create_end_node() -> NodeConfig:
    """Create the node for end of conversation."""
    return NodeConfig(
        name="end",
        task_messages=[
            {
                "role": "system",
                "content": "Thank the user for their time and end the conversation politely and concisely.",
            }
        ],
        post_actions=[{"type": "end_conversation"}]
    )

async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    logger.info("Starting bot")
    
    async with aiohttp.ClientSession() as session:
        stt = AzureSTTService(
            api_key=os.getenv("AZURE_SPEECH_API_KEY"),
            region=os.getenv("AZURE_SPEECH_REGION"),
        )

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            voice_id=os.getenv("CARTESIA_VOICE_ID", "a167e0f3-df7e-4d52-a9c3-f949145efdab"),
        )

        llm = AzureLLMService(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            model=os.getenv("AZURE_OPENAI_MODEL", "gpt-4"),
            endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )

        # tavus = TavusVideoService(
        #     api_key=os.getenv("TAVUS_API_KEY"),
        #     replica_id=os.getenv("TAVUS_REPLICA_ID"),
        #     session=session,
        # )

        simli_ai = SimliVideoService(
            api_key=os.getenv("SIMLI_API_KEY"),
            face_id="cace3ef7-a4c4-425d-a8cf-a5358eb0c427",
        )

        context = LLMContext([])
        context_aggregator_pair = LLMContextAggregatorPair(
            context,
            user_params=LLMUserAggregatorParams(
                user_turn_strategies=UserTurnStrategies(
                    stop=[
                        TurnAnalyzerUserTurnStopStrategy(turn_analyzer=LocalSmartTurnAnalyzerV3())
                    ]
                ),
            ),
        )
        user_aggregator, assistant_aggregator = context_aggregator_pair

        pipeline = Pipeline(
            [
                transport.input(),
                stt,
                user_aggregator,
                llm,
                tts,
                simli_ai,
                transport.output(),
                assistant_aggregator,
            ]
        )

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                audio_in_sample_rate=16000,
                audio_out_sample_rate=24000,
                enable_metrics=True,
                enable_usage_metrics=True,
            ),
            idle_timeout_secs=runner_args.pipeline_idle_timeout_secs,
            observers=[UserBotLatencyLogObserver()]
        )

        set_db_tracker_transport(task)
        set_function_tracker_transport(task)
        
        flow_manager = CustomFlowManager(
            task=task,
            llm=llm,
            transport=transport,
            context_aggregator=context_aggregator_pair,
        )

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            logger.info("Client connected")
            
            # Clean up any previous session state
            flow_manager.state.clear()
            flow_manager._pending_transition = None
            
            flow_manager.state["conversation_start_time"] = datetime.now().isoformat()
            flow_manager.state["tool_calls"] = []

            flow_manager._initialized = False
            await flow_manager.initialize(create_initial_node())
            await task.queue_frames([LLMRunFrame()])


        @user_aggregator.event_handler("on_user_turn_stop_timeout")
        async def on_user_turn_stop_timeout(aggregator):
            message = {
                "role": "system",
                "content": "Continue.",
            }
            await user_aggregator.queue_frame(LLMMessagesAppendFrame([message], run_llm=True))


        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            logger.info("Client disconnected")
            
            try:
                conversation_started = flow_manager.state.get("conversation_start_time") is not None
                user_identified = flow_manager.state.get("user_identified", False)
                
                if conversation_started and user_identified:
                    logger.info("Generating conversation summary before cleanup")
                    summary_action = {
                        "type": "conversation_end",
                        "timestamp": datetime.now().isoformat(),
                    }
                    asyncio.create_task(generate_and_save_summary(summary_action, flow_manager))
                else:
                    logger.info("No significant conversation to summarize")
            except Exception as e:
                logger.error(f"Error preparing summary on disconnect: {e}")
            
            async def delayed_cleanup():
                await asyncio.sleep(0.5)
                try:
                    flow_manager.state.clear()
                    flow_manager._pending_transition = None
                    flow_manager._initialized = False
                    logger.info("FlowManager state cleaned up")
                except Exception as e:
                    logger.error(f"Error cleaning up FlowManager state: {e}")
            asyncio.create_task(delayed_cleanup())
            await task.cancel()

        runner = PipelineRunner(handle_sigint=runner_args.handle_sigint)
        await runner.run(task)


async def bot(runner_args: RunnerArguments):
    """Main bot entry point compatible with Pipecat Cloud."""
    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport, runner_args)


if __name__ == "__main__":
    from pipecat.runner.run import main
    main()
