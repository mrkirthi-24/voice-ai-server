import os
import asyncio
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from loguru import logger
import requests

from main import run_bot
from pipecat.runner.types import RunnerArguments

load_dotenv(override=True)

app = FastAPI(title="Voice Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_sessions = {}


class StartCallRequest(BaseModel):
    room_url: Optional[str] = None


class StartCallResponse(BaseModel):
    room_url: str
    session_id: str
    status: str


class StopCallRequest(BaseModel):
    session_id: str


@app.post("/start-call", response_model=StartCallResponse)
async def start_call(request: StartCallRequest):
    """Start a new call session"""
    try:
        logger.info(f"Starting call session with room: {request.room_url}")
        
        room_url = request.room_url
        
        if not room_url:
            room_url = await create_daily_room()
            logger.info(f"Created new Daily room: {room_url}")
        
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        task = asyncio.create_task(start_bot_session(room_url, session_id))
        
        # Store session info
        active_sessions[session_id] = {
            "room_url": room_url,
            "task": task,
            "started_at": datetime.now().isoformat(),
            "status": "active"
        }
        
        logger.info(f"✅ Bot session {session_id} started in room {room_url}")
        
        return StartCallResponse(
            room_url=room_url,
            session_id=session_id,
            status="started"
        )
        
    except Exception as e:
        logger.error(f"❌ Error starting call: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stop-call")
async def stop_call(request: StopCallRequest):
    """Stop an active voice agent session"""
    try:
        session_id = request.session_id
        
        if session_id not in active_sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        
        session = active_sessions[session_id]
        
        if session["task"]:
            session["task"].cancel()
        
        del active_sessions[session_id]
        
        logger.info(f"✅ Bot session {session_id} stopped")
        
        return {"status": "stopped", "session_id": session_id}
        
    except Exception as e:
        logger.error(f"❌ Error stopping call: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def create_daily_room():
    """Create a new Daily.co room"""
    daily_api_key = os.getenv("DAILY_API_KEY")
    
    if not daily_api_key:
        room_url = os.getenv("DAILY_ROOM_URL")
        if room_url:
            return room_url
        raise HTTPException(
            status_code=500,
            detail="No Daily API key or room URL configured"
        )
    
    response = requests.post(
        "https://api.daily.co/v1/rooms",
        headers={
            "Authorization": f"Bearer {daily_api_key}",
            "Content-Type": "application/json"
        },
        json={
            "properties": {
                "enable_chat": False,
                "enable_screenshare": False,
                "enable_recording": False,
                "start_video_off": True,
                "start_audio_off": False
            }
        }
    )
    
    if response.status_code != 200:
        logger.error(f"Failed to create Daily room: {response.text}")
        return os.getenv("DAILY_ROOM_URL")
    
    room_data = response.json()
    return room_data["url"]


async def start_bot_session(room_url: str, session_id: str):
    """Start the bot in a specific Daily room"""
    try:
        logger.info(f"🤖 Starting bot for session {session_id}")
        from pipecat.transports.daily.transport import DailyTransport, DailyParams
        from pipecat.audio.vad.silero import SileroVADAnalyzer
        from pipecat.audio.vad.vad_analyzer import VADParams
        
        transport = DailyTransport(
            room_url=room_url,
            token=None,
            bot_name="Voice Agent",
            params=DailyParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                video_out_enabled=True,
                video_out_is_live=True,
                video_out_width=1280,
                video_out_height=720,
                vad_analyzer=SileroVADAnalyzer(
                    params=VADParams(stop_secs=0.2)
                ),
            )
        )
        
        runner_args = RunnerArguments()
        await run_bot(transport, runner_args)
        
    except asyncio.CancelledError:
        logger.info(f"🛑 Bot session {session_id} cancelled")
    except Exception as e:
        logger.error(f"❌ Error in bot session {session_id}: {e}")
        if session_id in active_sessions:
            active_sessions[session_id]["status"] = "error"


if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting Voice Agent API Server")    
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
