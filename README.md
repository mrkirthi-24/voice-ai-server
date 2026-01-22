## Tech Stack

- **Framework**: Pipecat Agents
- **STT**: Azure Speech Services
- **TTS**: Cartesia
- **LLM**: Azure OpenAI (GPT-4)
- **Avatar**: Tavus
- **Database**: Supabase

## Setup

### 1. Install Dependencies

```bash
cd service
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Required API keys:
- Azure Speech Services (for STT)
- Cartesia (for TTS)
- Azure OpenAI (for LLM)
- Tavus (for video avatar)
- Supabase (for database)
- Daily.co (for WebRTC transport)
